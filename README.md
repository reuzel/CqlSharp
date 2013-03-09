CqlSharp
========

CqlSharp is a high performant, asynchronous Cassandra CQL binary protocol driver offering an ADO.NET like interface.

Features
--------

* CqlSharp is 100% written in C#, and requires .NET 4.5
* Implements the (new) CQL Binary Protocol and therefore requires Cassandra 1.2 and up
* Query tracing is supported
* The API is ADO.NET like. If you are familiar with SqlConnection, SqlCommand, and SqlReader, you should be able to use CqlSharp as well
* The API is predominately async via TPL. Synchronous alternatives are available as well
* Supports mapping of objects to query parameters, or select query results to objects. Mapping is tunable by decorating your classes via Table and Column attributes.
* Configuration is done through connection strings, multiple clusters are supported.
* Most behavioral aspects of the CqlSharp are configurable: max number of connections, new connection threshold, discovery scope, max connection idle time, etc. etc.
* Relative Node Discovery: given the 'seed' nodes in your connection string, CqlSharp may find other nodes for you: all nodes in your cluster, nodes in the same datacenter, or the nodes in the same rack
* Load balanced connection management: you can give your queries a load 'factor' and the client will take that into account when picking connections to send queries over.
* Queries will be retried when connections or nodes fail.
* Node monitoring: Node failure is detected. Recovery checks occur using a exponential back-off algorithm.

Some non-functionals
--------------------

* All I/O is done asynchronously using the wonderful TPL and .NET 4.5 async/await support. As a result, the client should be quite efficient in terms of threading.
* Buffering, MemoryPools and Task result caching reduce GC activity, especially important when doing high volumes of queries in a short time frame
* Uses multiple connections in parallel to execute queries faster. Queries are also multiplexed on single connections, leading to efficient connection usage.

Wish list
---------

* Even better performance, e.g. by creating using more sophisticated memory pools that support multiple buffer sizes.
* Linq2Cql
* Alternative retry models (e.g. retry with reduced consistency)

Why another Cassandra Client?
-----------------------------

This project started off as an improvement of the quite well written Cassandra-Sharp. I wanted to replace the predominantly synchronous reading and writing with an async version, such that a more efficient client was to be obtained. After some time, I discovered that this was no little change. In fact, the introduction of asynchronous I/O would lead to so much changes that I decided to start a new client from scratch. Well, from scratch... some parts I "borrowed" from Cassandra-Sharp (especially the value serialization bits). Where I did that, you'll see a remark in the XML comments...
