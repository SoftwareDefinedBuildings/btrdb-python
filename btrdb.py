import capnp
import collections
import socket
import struct
import threading

class BTrDBConnection(object):
    def __init__(self, btrdb_host, btrdb_port, schema_filepath):
        self.bs = capnp.load(schema_filepath)
        
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((btrdb_host, btrdb_port))
        
        self.seqno = 0
        self.seqnolock = threading.Lock()
        self.seqmap = {}
        self.have = ''
        self.expecting = 0
        self.hdrexpecting = 0
        self.numsegs = 0
        self.partmsg = {}
        
        self.alive = True
        
        rcvthread = threading.Thread(target = self._readall, args = ())
        rcvthread.daemon = True
        rcvthread.start()
        
    def _readall(self):
        while self.alive:
            resp = self.s.recv(1)
            self._data_received(resp)
            
    def _data_received(self, data):
        """ For some reason, using the built-in read function of the capnp
        module holds the GIL and blocks all threads. Therefore, this somewhat
        kludgy workaround is required. """
        # Taken from https://github.com/SoftwareDefinedBuildings/QDF/blob/master/qdf/quasar.py (written by Michael Andersen)
        self.have += data
        if ((len(self.have) >= self.expecting + self.hdrexpecting + 4) or
            (len(self.have) > 0 and self.expecting == 0)):
            if self.expecting == 0:
                if self.hdrexpecting == 0 and len(self.have) >= 4:
                    #Move to the first stage of decoding: work out header length
                    self.numsegs, = struct.unpack("<I", self.have[:4])
                    self.numsegs += 1
                    self.hdrexpecting = (self.numsegs) * 4
                    if self.hdrexpecting % 8 == 0:
                        self.hdrexpecting += 4
                    self.hdrptr = 4
                if self.hdrexpecting != 0 and len(self.have) >= self.hdrexpecting + self.hdrptr:
                    for i in xrange(self.numsegs):
                        segsize, = struct.unpack("<I", self.have[self.hdrptr:self.hdrptr+4])
                        self.expecting += segsize*8
                        self.hdrptr += 4

            if len(self.have) >= self.expecting + self.hdrexpecting + 4:
                ptr = self.expecting + self.hdrexpecting + 4
                self._process_segment(self.have[:ptr])
                self.have = self.have[ptr:]
                self.expecting = 0
                self.numsegs = 0
                self.hdrexpecting = 0
                
    def _process_segment(self, data):
        resp = self.bs.Response.from_bytes(data, traversal_limit_in_words = 100000000, nesting_limit = 1000)
        et = resp.echoTag
        if et in self.partmsg:
            sofar = self.partmsg[et]
            new = False
        else:
            sofar = []
            new = True
        sofar.append(resp)
        if resp.final:
            if not new:
                del self.partmsg[et]
            with self.seqnolock:
                if et not in self.seqmap:
                    return
                receiver = self.seqmap[et]
            receiver.rcvcond.acquire()
            receiver.rcvd.append(sofar)
            if len(receiver.rcvd) == 1:
                receiver.rcvcond.notify()
            receiver.rcvcond.release()
        elif new:
            self.partmsg[et] = sofar
            
    def make_context(self):
        return BTrDBConnection.BTrDBContext(self)
            
    class BTrDBContext(object):
        def __init__(self, connection):
            self.rcvd = collections.deque() # A Queue is enough, but the Queue class is synchronized
            self.rcvcond = threading.Condition()
            self.connection = connection
            with connection.seqnolock:
                self.seqno = connection.seqno
                connection.seqno = (connection.seqno + 1) & 0xFFFFFFFFFFFFFFFF
                connection.seqmap[self.seqno] = self
        	        
        def _read(self):
            self.rcvcond.acquire()
            while len(self.rcvd) == 0:
                self.rcvcond.wait()
            msg = self.rcvd.popleft()
            self.rcvcond.release()
            return msg
            
        def queryStandardValues(self, uuid, start_time, end_time, version = 0):
            msg = self.connection.bs.Request.new_message()
            msg.echoTag = self.seqno
            msg.queryStandardValues = self.connection.bs.CmdQueryStandardValues.new_message()
            msg.queryStandardValues.uuid = uuid.bytes
            msg.queryStandardValues.startTime = start_time
            msg.queryStandardValues.endTime = end_time
            msg.queryStandardValues.version = version
            msg.write(self.connection.s)
            
            resplist = self._read()
            tvpairs = []
            version = None
            for resp in resplist:
                if resp.which() == "records":
                    version = version or resp.records.version
                    for record in resp.records.values:
                        tvpairs.append((record.time, record.value))
            return tvpairs, version
            
        def queryStatisticalValues(self, uuid, start_time, end_time, point_width, version = 0):
            msg = self.connection.bs.Request.new_message()
            msg.echoTag = self.seqno
            msg.queryStatisticalValues = self.connection.bs.CmdQueryStatisticalValues.new_message()
            msg.queryStatisticalValues.uuid = uuid.bytes
            msg.queryStatisticalValues.startTime = start_time
            msg.queryStatisticalValues.endTime = end_time
            msg.queryStatisticalValues.pointWidth = point_width
            msg.queryStatisticalValues.version = version
            msg.write(self.connection.s)
            
            resplist = self._read()
            stattuples = []
            version = None
            for resp in resplist:
                if resp.which() == "statisticalRecords":
                    version = version or resp.statisticalRecords.version
                    for record in resp.statisticalRecords.values:
                        stattuples.append({"time": record.time, "count": record.count, "min": record.min, "mean": record.mean, "max": record.max})
            return stattuples, version
            
        def queryWindowValues(self, uuid, start_time, end_time, width, depth = 0, version = 0):
            msg = self.connection.bs.Request.new_message()
            msg.echoTag = self.seqno
            msg.queryWindowValues = self.connection.bs.CmdQueryWindowValues.new_message()
            msg.queryWindowValues.uuid = uuid.bytes
            msg.queryWindowValues.startTime = start_time
            msg.queryWindowValues.endTime = end_time
            msg.queryWindowValues.width = width
            msg.queryWindowValues.depth = depth
            msg.queryWindowValues.version = version
            msg.write(self.connection.s)
            
            resplist = self._read()
            stattuples = []
            version = None
            for resp in resplist:
                if resp.which() == "statisticalRecords":
                    version = version or resp.statisticalRecords.version
                    for record in resp.statisticalRecords.values:
                        stattuples.append({"time": record.time, "count": record.count, "min": record.min, "mean": record.mean, "max": record.max})
            return stattuples, version
            
        def queryVersion(self, uuids, version = 0):
            msg = self.connection.bs.Request.new_message()
            msg.echoTag = self.seqno
            msg.queryVersion = self.connection.bs.CmdQueryVersion.new_message()
            queryVersion = msg.queryVersion.init('uuids', len(uuids))
            for i, tvt in enumerate(uuids):
                queryVersion[i] = uuids[i].bytes
            msg.write(self.connection.s)
            
            resplist = self._read()
            versions = []
            for resp in resplist:
                if resp.which() == "versionList":
                    for version in resp.versionList.versions:
                        versions.append(version)
            return versions
            
        def queryNearestValue(self, uuid, time, backward, version = 0):
            msg = self.connection.bs.Request.new_message()
            msg.echoTag = self.seqno
            msg.queryNearestValue = self.connection.bs.CmdQueryNearestValue.new_message()
            msg.queryNearestValue.uuid = uuid.bytes
            msg.queryNearestValue.time = time
            msg.queryNearestValue.backward = backward
            msg.queryNearestValue.version = version
            msg.write(self.connection.s)
            
            resplist = self._read()
            tvpairs = []
            version = None
            for resp in resplist:
                if resp.which() == "records":
                    version = version or resp.records.version
                    for record in resp.records.values:
                        tvpairs.append((record.time, record.value))
            return tvpairs, version
            
        def queryChangedRanges(self, uuid, from_generation, to_generation, resolution):
            msg = self.connection.bs.Request.new_message()
            msg.echoTag = self.seqno
            msg.queryChangedRanges = self.connection.bs.CmdQueryChangedRanges.new_message()
            msg.queryChangedRanges.uuid = uuid.bytes
            msg.queryChangedRanges.fromGeneration = from_generation
            msg.queryChangedRanges.toGeneration = to_generation
            msg.queryChangedRanges.resolution = resolution
            msg.write(self.connection.s)
            
            resplist = self._read()
            ranges = []
            version = None
            for resp in resplist:
                if resp.which() == "changedRngList":
                    version = version or resp.changedRngList.version
                    for record in resp.changedRngList.values:
                        ranges.append((record.startTime, record.endTime))
            return ranges, version
            
        def insertValues(self, uuid, sequence_of_time_value_tuples, sync = False):
            msg = self.connection.bs.Request.new_message()
            msg.echoTag = self.seqno
            msg.insertValues = self.connection.bs.CmdInsertValues.new_message()
            msg.insertValues.uuid = uuid.bytes
            insertValues = msg.insertValues.init('values', len(sequence_of_time_value_tuples))
            for i, tvt in enumerate(sequence_of_time_value_tuples):
                insertValues[i].time = tvt[0]
                insertValues[i].value = tvt[1]
            msg.insertValues.sync = sync
            msg.write(self.connection.s)
            
            resplist = self._read()
            return str(resplist[0].statusCode)
            
        def deleteValues(self, uuid, start_time, end_time):
            msg = self.connection.bs.Request.new_message()
            msg.echoTag = self.seqno
            msg.deleteValues = self.connection.bs.CmdDeleteValues.new_message()
            msg.deleteValues.uuid = uuid.bytes
            msg.deleteValues.startTime = start_time
            msg.deleteValues.endTime = end_time
            msg.write(self.connection.s)
            
            resplist = self._read()
            return str(resplist[0].statusCode)
            
        def destroy(self):
            with self.connection.seqnolock:
                del self.connection.seqmap[self.seqno]
                
    def close(self):
        self.alive = False
        self.s.shutdown(socket.SHUT_RDWR)
        self.s.close()

