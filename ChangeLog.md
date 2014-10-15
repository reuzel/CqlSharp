## Version 0.40.1 - Fix CqlConnection.Open retry bug
* Solves issue with connection to 2.0 and 1.x clusters when MaxQueryRetries=0
* Retries CqlConnection.Open up to MaxQueryRetries + 1, when opening connections to the cluster fails

## Version 0.40.0 - Cassandra 2.1, binary protocol v3 and new type system
* Implements binary protocol v3: more queries per connection, timestamp property, UDT and Tuple types, larger collections.
* Rewrite of type system, CqlType is no longer an enum but a class with subclasses for every Cassandra type.
* Previous CqlType enum is now called CqlTypeCode.
* Primitive Cql types can be accessed through CqlType.* static properties. Others can be constructed from .NET class, type string, or TypeCode + parameter via the CqlType.CreateType overloads.
* CqlParameter no longer accepts different CqlTypeCodes, but requires a single CqlType
* Added high performance and extremely flexible type conversion that prevents boxing, and even converts collection types (e.g. hashset{int} to list{long})
* Removed explicit references to collection key and value types. They are not used separate from the corresponding CqlTypes anymore
* CqlDataReader.GetDataTypeName(int i) now returns full string representation of the Cassandra type (e.g. map<varchar, int>)
* CqlDataReader.GetFieldCqlType added that returns the CqlType used for the given field
* Removed a lot of boxing/unboxing of primitive types during serialization/deserialization
* Implemented DbDataReader.GetFieldValue{T}(int i)
* Supports User Defined Types. Annotate your class with CqlUserType and CqlColumn to have it correctly mapped to a Cassandra User Defined Type
* Guesses protocol version from release_version shortening connection setup times for older clusters

## Version 0.39.0 - Performance of sync operations
* Many changes to have synchronous API really execute synchronously, it is no longer a sync-over-async wrapper
* Improved exception generation in case of query cancellation or timeout
* Logging cancelled queries
* Replacing old test app with a new performance test app
* added NLog as seperate logging package
* Fix: Adding LocalOne consistency level

## Version 0.38.0 - Fixes and Improved Aliveness Checking of Cassandra Nodes
* Fixed bug that hampered exponential backoff
* Succesfull connection to a node must be made before it is marked as up again, keeping it out-of-scope of connection strategies until proven up.
* Fixing issue where multiple node UP notifications are received in a short time
* Clearing prepared query ids, forcing queries to be reprepared when node is marked down
* Making sure that at least a single query attempt is done when MaxRetryCount setting is set to 0
* MEF fix: now using correct directories for loading extensions

## Version 0.37.0 - TraceLogger and Fixes on cluster reconfiguration
* Fix crash when nodes are added to a running system (and have no tokens gossiped). CqlSharp now reloads configurations every minute until all tokens are found.
* Fix missing logger binding when using exclusive connection strategy
* Simplified logger classes and added TraceLogger (thanks to justmara)

## Version 0.36.0 - Fixes and Development support
* Fixing situation where queries using a pagesize are ended with a data frame with no contents
* Fix of protocol negotiation flow for older Cassandra versions
* Exposing supported CQL version from CqlConnection
* Allowing the primary CQL* classes to be subclassed (e.g. allowing them to be mocked)
* Introducing a non-generic interface to ObjectAccessor{T}

## Version 0.35.0 - Make ExecuteScalar according to spec.
* Have ExecuteScalar return null in case no row was found, and DBNull.Value if the database column value is null

## Version 0.34.0 - Socket options
* Adding socket options: connect timeout, Linger state, KeepAlive, SendBufferSize, and ReceiveBufferSize
* Fix: Proper CqlException returned when no cluster nodes can be reached

## Version 0.33.0 - Improved transaction handling
* Improved state management of transaction
* Adding Reset to CqlTransaction, allowing reuse of transaction objects
* Adding CqlError class, to expose Cql error details (including trace ids) as LastQueryResult on CqlCommand

## Version 0.32.2 - Default Guid insertion fix
* Fixing issue where nill guids were inserted as null values
* Adding TimeGuid.Default to get a nill guid with time version flags set.

## Version 0.32.1 - Transaction Race fix
* Fixing race condition issues when using prepared queries in a transaction

## Version 0.32.0 - Decimal support and Fixes
* Support for decimal types
* TimeGuid generation rewritten in order to guarantee uniqueness when many TimeGuids are generated within a short timeframe.

## Version 0.31.0 - Linq-2-Cql
* Adjustments to support Linq2Cql in CqlSharp.Linq package
* Making ObjectAccessor public accessible
* Introducing CqlKey and CqlIndex attributes

## Version 0.30.2 - Deadlock removal
* Fix: Adding two missing ConfigureAwait statements avoiding deadlock in MVC projects

## Version 0.30.1 - Node address fallback
* Fallback to listen-address during discovery when rpc-address is 0.0.0.0. Issue #20
* Performance: Removing some boxing of structs during deserialization

## Version 0.30.0 - Binary Protocol V2 support
* Allowing default database to be set via connectionstring, regardless of connectionstrategy in use
* Sasl Authentication support. PasswordAuthenticator is supported out-of-the-box. Extendable via MEF by providing IAuthenticator and IAuthenticatorFactory implementations.
* Batch support via CqlBatchTransaction. Assign the command to the CqlBatchTransaction and executes will be buffered. When CqlBatchTransaction
is committed it will create and submit a batch statement.
* Paging support. CqlCommand can be given a page size: when iterating over a query result, the data will be fetched in "chunks"
 of the given page size
* Use of bound parameters with non-prepared queries
* Support for Serial_Local for CompareAndSet (CAS) statements
* Added CqlConnection.Shutdown methods to close all connections to Cassandra
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