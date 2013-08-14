## Version 0.15.0 - Bug fixes
* Breaking changes: making CqlSharp implement ADO.NET interfaces

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