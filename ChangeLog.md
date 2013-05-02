## Version 0.11.0
* Several bug fixes
* CqlSharp now listens to cluster events like Node Up/Down/Add/Remove. Nodes are added and removed while the application is running.

## Version 0.10.0
* Added PartitionKey, PartitionAware Connection Strategy that selects a connection based on the PartitionKey set on a CqlCommand.
* Removed QueryOptions class, options can now be set directly on CqlCommand objects.
* Bug fix on short serialization
* Removed a few race-conditions
* Added varint support

## Version 0.9.0
* Initial public release