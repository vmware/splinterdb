# SplinterDB Known Limitations 

SplinterDB is expected to evolve its API and add significant features, and it is not recommended for production use until version 1.0.
Thus, SplinterDB is provided *as-is* given the following limitations and missing features:

* Data recovery is not yet implemented (see [issue](https://github.com/vmware/splinterdb/issues/236) for roadmap).
* Public API is not yet stable. Users should expect breaking changes in future versions.
* SplinterDB on-disk format is not versioned (data may not survive upgrades.)
* Key and data size need to be some fraction of page size. 
* The application must specify the minimum and maximum of the key range.
* SplinterDB on-disk size is fixed at compile time.
* SplinterDB does not expose an API to force the latest write to be durable (e.g., fsync/commit.)
* SplinterDB disk size cannot be changed once specified.
* SplinterDB does not have a public API for async features.
* SplinterDB does not retain initialization parameters metadata (which cannot be discovered from the database.)
* Internal metrics and stats are not exposed to applications.
* Range delete is not yet implemented.
* Empty database (e.g. db->clear()) is not yet implemented.
* Transactions not supported (no atomic multi-put.)
