"""
The 'btrdb' module provides Python bindings to interact with BTrDB. It is
intended to be used with Python's threading library.
"""

import capnp
import collections
import socket
import struct
import threading

class BTrDBConnection(object):
    """
    BTrDB Connection encapsulates one connection to a BTrDB. One connection is
    capable of concurrently executing multiple queries.
    
    However, if you plan to make many serial requests per thread, it is much
    more efficient to get a 'BTrDBContext' object with newContext(), use that
    context to make the serial requests on that thread, and then destroy() the
    context after making the requests. The methods of the BTrDBConnection
    object are simply wrappers that create a context, make the query, and then
    destroy the context.
    """
    def __init__(self, btrdb_host, btrdb_port, schema_filepath):
        """
        Construct a new 'BTrDBConnection' object.
        
        :param btrdb_host: The hostname at which the BTrDB is located.
        :param btrdb_port: The port of the Cap'n Proto interface of the BTrDB.
        :param schema_filepath: The filepath of the Cap'n Proto schema file for BTrDB. The file can be found at https://github.com/SoftwareDefinedBuildings/btrdb/blob/master/cpinterface/interface.capnp.
        """
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
            
    def newContext(self):
        """
        Creates a new BTrDB Context from this connection.
        
        :return: A 'BTrDBContext' object.
        """
        return BTrDBContext(self)
                
    def _context_wrapper(self, method, args, kwargs):
        c = self.newContext()
        try:
            return getattr(c, method)(*args, **kwargs)
        finally:
            c.destroy()
                
    def queryStandardValues(self, *args, **kwargs):
        """
        Performs a Standard Values Query.
        See BTrDBContext.queryStandardValues for details.
        """
        return self._context_wrapper('queryStandardValues', args, kwargs)
            
    def queryStatisticalValues(self, *args, **kwargs):
        """
        Performs a Statistical Values Query.
        See BTrDBContext.queryStatisticalValues for details.
        """
        return self._context_wrapper('queryStatisticalValues', args, kwargs)
        
    def queryWindowValues(self, *args, **kwargs):
        """
        Performs a Window Values Query.
        See BTrDBContext.queryWindowValues for details.
        """
        return self._context_wrapper('queryWindowValues', args, kwargs)
        
    def queryVersion(self, *args, **kwargs):
        """
        Queries the BTrDB for the most recent version numbers for a list of streams.
        See BTrDBContext.queryVersion for details.
        """
        return self._context_wrapper('queryVersion', args, kwargs)
        
    def queryNearestValue(self, *args, **kwargs):
        """
        Performs a Nearest Value Query.
        See BTrDBContext.queryNearestValue for details.
        """
        return self._context_wrapper('queryNearestValue', args, kwargs)
        
    def queryChangedRanges(self, *args, **kwargs):
        """
        Performs a Changed Ranges Query.
        See BTrDBContext.queryChangedRanges for details.
        """
        return self._context_wrapper('queryChangedRanges', args, kwargs)
        
    def insertValues(self, *args, **kwargs):
        """
        Inserts points into the BTrDB.
        See BTrDBContext.insertValues for details.
        """
        return self._context_wrapper('insertValues', args, kwargs)
        
    def deleteValues(self, *args, **kwargs):
        """
        Deletes points from the BTrDB.
        See BTrDBContext.deleteValues for details.
        """
        return self._context_wrapper('deleteValues', args, kwargs)
                
    def close(self):
        """
        Relinquishes the resources allocated for this BTrDB Connection.
        """
        self.alive = False
        self.s.shutdown(socket.SHUT_RDWR)
        self.s.close()

class BTrDBContext(object):
    """
    A 'BTrDBContext' can be used to query the BTrDB of the underlying
    'BTrDBConnection'. You can create an unlimited number of BTrDB Contexts
    from a BTrDB Connection, but each one can only have one outstanding
    request to BTrDB at a time; when a request is made the current thread
    blocks until the response is fully received. Issuing multiple concurrent
    requests on the same BTrDB Context results in undefined behavior.
    However, creating multiple BTrDB Contexts from a single BTrDB Connection,
    and concurrently using those separate contexts, is perfectly legal.
    """
    def __init__(self, connection):
        """
        You should NEVER invoke the BTrDBContext constructor directly.
        Instead, use BTrDBConnection.newContext.
        """
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
        """
        Performs a Standard Values Query.
        
        :param uuid: The UUID of the stream to query, as a UUID object.
        :param start_time: The start of the time range to query, in nanoseconds since the epoch.
        :param end_time: The end of the time range to query, in nanoseconds since the epoch.
        :param version: The version of the stream to query. Defaults to the most recent version.
        :return: A list of (time, value) tuples representing each returned point, and the version number identifying which version of the stream satisfied the query.
        """
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
        """
        Performs a Statistical Values Query.
        
        :param uuid: The UUID of the stream to query, as a UUID object.
        :param start_time: The start of the time range to query, in nanoseconds since the epoch.
        :param end_time: The end of the time range to query, in nanoseconds since the epoch.
        :param point_width: The base-two logarithm of the width of each point in nanoseconds.
        :param version: The version of the stream to query. Defaults to the most recent version.
        :return: A list of dictionaries representing the statistics for each point, and the version number identifying which version of the stream satisfied the query.
        """
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
        """
        Performs a Window Values Query.
        
        :param uuid: The UUID of the stream to query, as a uuid.UUID object.
        :param start_time: The start of the time range to query, in nanoseconds since the epoch.
        :param end_time: The end of the time range to query, in nanoseconds since the epoch.
        :param point_width: The width of each point in nanoseconds.
        :param depth: The base-two logarithm of the precision, in nanoseconds, at which to divide the time range into points. A depth of 0 (the default) represents exact division. A higher value of this parameter makes the size of the points less precise, but makes the corresponding statistics faster to compute.
        :param version: The version of the stream to query. Defaults to the most recent version.
        :return: A list of dictionaries representing the statistics for each point, and the version number identifying which version of the stream satisfied the query.
        """
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
        
    def queryVersion(self, uuids):
        """
        Queries the BTrDB for the most recent version numbers for a list of streams.
        
        :param uuids: A list of UUIDs representing the streams being queried.
        :return: A list of version numbers corresponding to the queried streams.
        """
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
        """
        Performs a Nearest Value Query.
        
        :param uuid: A UUID representing the stream being queried, as a uuid.UUID object.
        :param time: The time at which a nearby point is queried.
        :param backward: A boolean indicating whether to search for a point. If True, searches for the nearest point at or later than the provided 'time' parameter. If False, searches for the nearest point before the provided 'time' parameter.
        :param version: The version of the stream to query. Defaults to the most recent version.
        """
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
        """
        Performs a Changed Ranges Query.
        
        :param uuid: A UUID representing the stream being queried, as a uuid.UUID object.
        :param from_generation: The starting version number.
        :param to_generation: The ending version number (not included).
        :param resolution: The base-two logarithm of the precision, in nanoseconds, at which to find the changed ranges.
        """
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
        """
        Inserts points into the BTrDB.
        
        :param uuid: A UUID representing the stream into which points are to be inserted.
        :param sequence_of_time_value_tuples: A list of tuples, each of which represents a (time, value) point to insert.
        :param sync: A boolean indicating whether or not to commit insertion before returning. If True, commits the transaction before returning. If False (the default), may return before the transaction is committed as a performance optimization.
        :return: The status code, as a string, indicating whether the insertion was successful.
        """
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
        """
        Deletes points from the BTrDB.
        
        :param uuid: A UUID representing the stream from which points are to be deleted.
        :param start_time: The start of the time range from which to delete points, in nanoseconds since the epoch.
        :param end_time: The end of the time range from which to delete points, in nanoseconds since the epoch.
        :return: The status code, as a string, indicating whether the deletion was successful.
        """
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
        """
        Relinquishes the resources allocated for this BTrDB Context.
        """
        with self.connection.seqnolock:
            del self.connection.seqmap[self.seqno]
