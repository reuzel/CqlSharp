## Version 0.30.0 - Binary Protocol V2 support
* Allowing default database to be set via connectionstring, regardless of connectionstrategy in use
* Batch support via CqlBatchTransaction. Assign the command to the CqlBatchTransaction and executes will be buffered. When CqlBatchTransaction
is committed it will create and submit a batch statement.
* Paging support. CqlCommand can be given a page size: when iterating over a query result, the data will be fetched in "chunks"
 of the given page size
* Use of bound parameters with non-prepared queries
* Support for Serial_Local for CompareAndSet (CAS) statements
* Performance: when using Cql protocol v2, cached result metadata will be used, reducing network overhead
* Performance: Prepare() better utilizes caching, less Task creation overhead
* Fix: ChangeDatabase function uses correct use syntax
* Fix: ConnectionStratagies are better aware of existing connections on startup.

## Version 0.20.1 - Oops
* Critical bug fix: connection idle bug due to comparison of datetimes in different timezones, Issue #15

## Version 0.20.0 - ADO.NET implementation
* CqlSharp now implements the ADO.NET interfaces (this is a breaking change!), using the System.Data.Common classes
* Introduced QueryResult property on CqlCommand to expose Cassandra result information for non-Queries
* Parameters for prepared queries will be automatically generated when no paramaters are set beforehand
* ClusterConfig replaced by CqlConnectionStringBuilder (and moved to root namespace)
* Query cancellation now supported
* Database can be changed from default to something else per CqlConnection when Exclusive strategy is used
* Some preparation to make ConnectionStrategy extendable like logging
* Getting and logging more info on Cassandra cluster like name, release, cqlversion, etc.

## Version 0.14.1 - Bug fixes (not released)
* Making short values unsigned shorts as depicted in Cql binary protocol spec.
* Added few missing ConfigureAwaits
* Cleanup of code
* Faster Random Strategy Logic

## Version 0.14.0 - Need for Speed
* ObjectMapping performance improvement: Using compiled expressions to set and get object properties and fields
* Snappy compression support
* Removed ParallelConnections option, corresponding behavior now depends on connection selection strategy
* Improved memory management (more byte[] reuse, different array sizes, faster pool)
* Faster Balanced Strategy logic

## Version 0.13.0 - Logging and Contexts
* ConfigureAwait on all awaits to avoid issues with synchronization contexts
* Logging! Extendable via MEF by simply implementing and co-deploying ILoggerFactory and ILogger implementations
* Small bug fixes

## Version 0.12.0 - Null values
* Support/fix for null values
* Support for Nullable types
* Addition of Max Query Retry configuration parameter
* Performance improvements (less async, less spinlock where lock is wanted)
* Fixing some nasty race conditions

## Version 0.11.0 - Cluster changes
* Several bug fixes
* CqlSharp now listens to cluster events like Node Up/Down/Add/Remove. Nodes are added and removed while the application is running.

## Version 0.10.0 - Partition Aware
* Added PartitionKey, PartitionAware Connection Strategy that selects a connection based on the PartitionKey set on a CqlCommand.
* Removed QueryOptions class, options can now be set directly on CqlCommand objects.
* Bug fix on short serialization
* Removed a few race-conditions
* Added varint support

## Version 0.9.0 - Let's go!
* Initial public release