CqlSharp
========

CqlSharp is a high performance, asynchronous Cassandra CQL binary protocol driver offering an ADO.NET like interface.

Installation
------------

The latest release of CqlSharp can be found as [package on NuGet](http://nuget.org/packages/CqlSharp/). Please install from there.

Features
--------

* CqlSharp is 100% written in C#, and requires .NET 4.5
* Implements the (new) CQL Binary Protocol and therefore requires Cassandra 1.2 and up
* Query tracing is supported
* The API is ADO.NET like. If you are familiar with SqlConnection, SqlCommand, and SqlReader, you should be able to use CqlSharp as well
* The API is predominately async via TPL. Synchronous alternatives are available as well
* Supports mapping of objects to query parameters, or select query results to objects. Mapping is tunable by decorating your classes via Table and Column attributes.
* Configuration is done through connection strings, multiple clusters are supported.
* Rows returned from select queries are read via a pull-model, allowing for large result sets to be processed efficiently.
* CqlSharp allows for partition/token aware routing of queries. This allows queries to be directly send to the Cassandra nodes that are 'owner' of that data.
* Most behavioral aspects of the CqlSharp are configurable: max number of connections, new connection threshold, discovery scope, max connection idle time, etc. etc.
* Relative Node Discovery: given the 'seed' nodes in your connection string, CqlSharp may find other nodes for you: all nodes in your cluster, nodes in the same data center, or the nodes in the same rack
* Load balanced connection management: you can give your queries a load 'factor' and the client will take that into account when picking connections to send queries over.
* Queries will be retried when connections or nodes fail.
* Node monitoring: Node failure is detected. Recovery checks occur using a exponential back-off algorithm

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

