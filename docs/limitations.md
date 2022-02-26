# SplinterDB Known Limitations 

* Key and data size need to be some fraction of page size. 
* The application must specify the minimum and maximum of the key range.
* SplinterDB size is fixed at compile time.
* SplinterDB does not expose an API to force the latest write to be durable.
* SplinterDB disk size cannot be changed once specified.
* SplinterDB does not have a public API for async features.
* SplinterDB does not retain initialization parameters metadata (which cannot be discovered from the database).
* SplinterDB on-disk format is not versioned.
* Internal metrics and stats are not exposed to applications.

# Missing Features
* Checkpointing. 
* Recovery mechanism.
* Range delete.
* Empty database.
* Transactions (SplinterDB has atomic operations.)
