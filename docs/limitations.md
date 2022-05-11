# SplinterDB Known Limitations 

SplinterDB is expected to evolve its API and add significant features, and it is not recommended for production use until version 1.0.

Thus, SplinterDB is provided *as-is* given the following limitations and missing features:

* Data recovery is not yet implemented (see [issue](https://github.com/vmware/splinterdb/issues/236) for roadmap).
* Public API is not yet stable. Users should expect breaking changes in future versions.
* SplinterDB on-disk format is not versioned (Data may not survive software upgrades.)
* Single 4KiB page size, with fixed extent size of 32 pages/extent.
* Key and value size need to be less than the page size. Key size must be
  between 8 to 105 bytes. Support for smaller key-sizes is experimental.
* The application must specify the minimum and maximum of the key range.
* SplinterDB on-disk size is fixed at compile time.
* SplinterDB does not expose an API to force the latest write to be durable (e.g., fsync/commit.)
* SplinterDB disk size cannot be changed once configured.
* SplinterDB does not have a public API for the experimental async features.
* SplinterDB does not retain configuration parameters and metadata. (These cannot
  be discovered from the database, and have to be provided for re-starting SplinterDB.)
* Internal metrics and stats are not exposed to applications.
* Range delete is not yet implemented.
* Empty database (e.g. db->clear()) is not yet implemented.
* Transactions not supported (no atomic multi-put.)
