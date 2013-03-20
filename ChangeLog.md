## Version 0.10.0
* Added PartitionKey, PartitionAware Connection Strategy that selects a connection based on the PartitionKey set on a CqlCommand.
* Removed QueryOptions class, options can now be set directly on CqlCommand objects.
* Bug fix on short serialization
* Removed a few race-conditions
* Added varint support

## Version 0.9.0
* Initial public release