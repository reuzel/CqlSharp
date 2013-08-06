CqlSharp
========

CqlSharp is a high performance, asynchronous [Cassandra](http://cassandra.apache.org/) CQL binary protocol driver offering an ADO.NET like interface.

Please see the [Wiki](https://github.com/reuzel/CqlSharp/wiki) for more extensive [documentation](https://github.com/reuzel/CqlSharp/wiki).

Installation
------------

The latest release of CqlSharp can be found as a [package on NuGet](http://nuget.org/packages/CqlSharp/). Please install from there.

Features
--------

* CqlSharp is 100% written in C#, and requires .NET 4.5. It has no dependencies on other packages or libraries.
* CqlSharp is an implementation of the (new) CQL Binary Protocol and therefore requires Cassandra 1.2 and up
* Query tracing is supported.
* The API is ADO.NET like. If you are familiar with SqlConnection, SqlCommand, and SqlReader, you should be able to use CqlSharp with no difficulty.
* The API is predominately [asynchronous](http://msdn.microsoft.com/en-us/library/vstudio/hh191443.aspx), heavily relying on the System.Threading.Tasks namespace. Synchronous alternatives are available for convenience.
* Supports mapping of objects to query parameters, or query results to objects. Mapping is tunable by decorating your classes via Table and Column attributes.
* Configuration is done through connection strings. The simultaneous use of multiple Cassandra clusters is supported.
* Rows returned from select queries are read via a pull-model, allowing for large result sets to be processed efficiently. Query results may be buffered in memory as well.
* CqlSharp allows for partition/token aware routing of queries. This allows queries to be directly sent to the Cassandra nodes that are the 'owner' of that data.
* CqlSharp supports Snappy compression of queries and responses
* Most behavioral aspects of the CqlSharp are configurable: max number of connections, new connection threshold, discovery scope, max connection idle time, etc. etc.
* Relative Node Discovery: given the 'seed' nodes in your connection string, CqlSharp may find other nodes for you: all nodes in your cluster, nodes in the same data center, or the nodes in the same rack
* Load balanced connection management: you can give your queries a load 'factor' and the client will take that into account when picking connections to send queries over.
* Queries will be automatically retried when connections or nodes fail.
* Node monitoring: Node failure is automatically detected. Recovery checks occur using an exponential back-off algorithm

Some non-functionals
--------------------

* All I/O is done asynchronously using the wonderful TPL and .NET 4.5 async/await support. As a result, the client should be quite efficient in terms of threading. This makes it especially suitable in server environments where all threads are necessary to handle incoming requests.
* Buffering, MemoryPools and Task result caching reduce GC activity, especially important when doing high volumes of queries in a short time frame
* CqlSharp uses multiple connections in parallel to execute queries faster. Queries are also multiplexed on single connections, leading to efficient connection usage.
* Using compiled expressions, mapping rows to objects and vice-versa is very fast

Wish list
---------

* ~~Even better performance, e.g. by creating using more sophisticated memory pools that support multiple buffer sizes.~~
* Linq2Cql
* Alternative retry models (e.g. retry with reduced consistency)

