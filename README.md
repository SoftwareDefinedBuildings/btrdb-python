BTrDB Bindings for Python
=========================
These BTrDB Bindings for Python are meant to be used with Python's multithreading library. Documentation can be generated using pydoc.

Here is an example:
```
>>> import uuid
>>> import btrdb
>>>
>>> # This is the UUID of the stream we are going to interact with
... u = uuid.UUID('6390e9df-dfcb-4084-8080-8c719ce751ed')
>>>
>>> # Set up the BTrDB Connection and Context
... connection = btrdb.BTrDBConnection('localhost', 4410)
>>> context = connection.newContext()
>>>
>>> # Insert some data
... # We have to use the "sync" flag because we want the insert to be committed immediately, so that we can query it right away
... statuscode = context.insertValues(u, ((1, 10), (3, 14), (5, 19), (9, 13)), sync = True)
>>> print statuscode
ok
>>> # Query some data
... result = context.queryStandardValues(u, 0, 7)
>>> print result
([(1, 10.0), (3, 14.0), (5, 19.0)], 2L)
>>>
>>> # Close the context and connection
... context.destroy()
>>> connection.close()
```
