// Tests of the splinterdb-rs library
//
// If you add a new function to the public API of this library, add a test here
// (or extend an existing test) to demonstrate how to use it.

#[cfg(test)]
mod tests {
   use crate::{SdbMessage, SdbMessageType, SdbRustDataFuncs};
   use std::io::Result;

   // Test of performing two insertions and lookup
   #[test]
   fn ins_test() -> Result<()> {
      use splinterdb_sys::slice;
      use tempfile::tempdir;
      println!("BEGINNING TEST!");

      let mut sdb = crate::SplinterDB::new::<crate::DefaultSdb>();

      let data_dir = tempdir()?; // is removed on drop
      let data_file = data_dir.path().join("db.splinterdb");

      sdb.db_create(
         &data_file,
         &crate::DBConfig {
            cache_size_bytes: 1024 * 1024,
            disk_size_bytes: 30 * 1024 * 1024,
            max_key_size: 23,
            max_value_size: 100,
         },
      )?;

      println!("SUCCESSFULLY CREATED DB!");

      let key = b"some-key-0".to_vec();
      let value = b"some-value-0".to_vec();

      // verify that we can correctly create a splinter-slice from these keys and values
      let ks: slice = crate::create_splinter_slice(&key);
      let vs: slice = crate::create_splinter_slice(&value);
      assert_eq!(ks.length, key.len() as u64);
      assert_eq!(vs.length, value.len() as u64);
      unsafe {
         for i in 0..key.len() {
            assert_eq!(*((ks.data as *const u8).offset(i as isize)), key[i]);
         }
         for i in 0..value.len() {
            assert_eq!(*((vs.data as *const u8).offset(i as isize)), value[i]);
         }
      }

      sdb.insert(&key, &value)?;
      sdb.insert(&(b"some-key-4".to_vec()), &(b"some-value-4".to_vec()))?;
      println!("SUCCESSFULLY INSERTED TO DB!");

      let res = sdb.lookup(&key)?;
      match res {
         crate::LookupResult::NotFound => panic!("inserted key not found"),
         crate::LookupResult::FoundTruncated(_) => panic!("inserted key found but truncated"),
         crate::LookupResult::Found(v) => assert_eq!(v, value),
      }

      println!("SUCCESSFULLY PERFORMED LOOKUP!");

      println!("Dropping SplinterDB!");
      drop(sdb);
      println!("Drop done! Exiting");
      Ok(())
   }

   // Insert and delete, then lookup
   #[test]
   fn ins_and_del_test() -> Result<()> {
      use tempfile::tempdir;
      println!("BEGINNING TEST!");

      let mut sdb = crate::SplinterDB::new::<crate::rust_cfg::DefaultSdb>();

      let data_dir = tempdir()?; // is removed on drop
      let data_file = data_dir.path().join("db.splinterdb");

      sdb.db_create(
         &data_file,
         &crate::DBConfig {
            cache_size_bytes: 1024 * 1024,
            disk_size_bytes: 30 * 1024 * 1024,
            max_key_size: 23,
            max_value_size: 100,
         },
      )?;

      println!("SUCCESSFULLY CREATED DB!");

      let key = b"some-key-0".to_vec();
      let value = b"some-value-0".to_vec();
      sdb.insert(&key, &value)?;
      sdb.insert(&(b"some-key-1".to_vec()), &(b"some-value-1".to_vec()))?;
      sdb.insert(&(b"some-key-2".to_vec()), &(b"some-value-2".to_vec()))?;
      println!("SUCCESSFULLY PERFORMED INSERTIONS!");

      sdb.delete(&(b"some-key-1".to_vec()))?;
      sdb.delete(&(b"some-key-2".to_vec()))?;

      // lookup key that should not be present
      let res = sdb.lookup(&(b"some-key-1".to_vec()))?;
      match res {
         crate::LookupResult::NotFound => println!("Good!"),
         crate::LookupResult::FoundTruncated(_) => panic!("Should not have found this key!"),
         crate::LookupResult::Found(_) => panic!("Should not have found this key!"),
      }

      // lookup key that should still be present
      let res = sdb.lookup(&key)?;
      match res {
         crate::LookupResult::NotFound => panic!("inserted key not found"),
         crate::LookupResult::FoundTruncated(_) => panic!("inserted key found but truncated"),
         crate::LookupResult::Found(v) => assert_eq!(v, value),
      }

      println!("SUCCESSFULLY PERFORMED LOOKUPS!");

      println!("Dropping SplinterDB!");
      drop(sdb);
      println!("Drop done! Exiting");
      Ok(())
   }

   // Many inserts and lookup test
   #[test]
   fn many_ins_lookup() -> Result<()> {
      use tempfile::tempdir;
      println!("BEGINNING TEST!");

      let mut sdb = crate::SplinterDB::new::<crate::rust_cfg::DefaultSdb>();

      let data_dir = tempdir()?; // is removed on drop
      let data_file = data_dir.path().join("db.splinterdb");

      sdb.db_create(
         &data_file,
         &crate::DBConfig {
            cache_size_bytes: 1024 * 1024,
            disk_size_bytes: 30 * 1024 * 1024,
            max_key_size: 23,
            max_value_size: 100,
         },
      )?;

      println!("SUCCESSFULLY CREATED DB!");

      for i in 0..=100 {
         let key = ("some-key-".to_owned() + &i.to_string()).into_bytes().to_vec();
         let value = ("some-value-".to_owned() + &i.to_string()).into_bytes().to_vec();
         sdb.insert(&key, &value)?;
      }
      println!("SUCCESSFULLY PERFORMED INSERTIONS!");

      // lookup key that should not be present
      let res = sdb.lookup(&(b"some-key-101".to_vec()))?;
      match res {
         crate::LookupResult::NotFound => println!("Good!"),
         crate::LookupResult::FoundTruncated(_) => panic!("Should not have found this key!"),
         crate::LookupResult::Found(_) => panic!("Should not have found this key!"),
      }

      // lookup key that should still be present
      let res = sdb.lookup(&(b"some-key-56".to_vec()))?;
      match res {
         crate::LookupResult::NotFound => panic!("inserted key not found"),
         crate::LookupResult::FoundTruncated(_) => panic!("inserted key found but truncated"),
         crate::LookupResult::Found(v) => assert_eq!(v, b"some-value-56".to_vec()),
      }

      println!("SUCCESSFULLY PERFORMED LOOKUPS!");

      println!("Dropping SplinterDB!");
      drop(sdb);
      println!("Drop done! Exiting");
      Ok(())
   }

   #[test]
   fn overwrite_test() -> Result<()> {
      use tempfile::tempdir;
      println!("BEGINNING TEST!");

      let mut sdb = crate::SplinterDB::new::<crate::rust_cfg::DefaultSdb>();

      let data_dir = tempdir()?; // is removed on drop
      let data_file = data_dir.path().join("db.splinterdb");

      sdb.db_create(
         &data_file,
         &crate::DBConfig {
            cache_size_bytes: 1024 * 1024,
            disk_size_bytes: 30 * 1024 * 1024,
            max_key_size: 23,
            max_value_size: 100,
         },
      )?;
      println!("SUCCESSFULLY CREATED DB!");

      let key = b"some-key-0".to_vec();
      let value = b"some-value-0".to_vec();
      let nval = b"some-value-1".to_vec();
      sdb.insert(&key, &value)?;
      sdb.insert(&key, &nval)?;

      // lookup key
      let res = sdb.lookup(&key)?;
      match res {
         crate::LookupResult::NotFound => panic!("inserted key not found"),
         crate::LookupResult::FoundTruncated(_) => panic!("inserted key found but truncated"),
         crate::LookupResult::Found(v) => {
            assert_eq!(v, nval);
         },
      }
      println!("SUCCESSFULLY PERFORMED LOOKUP!");

      println!("Dropping SplinterDB!");
      drop(sdb);
      println!("Drop done! Exiting");
      Ok(())
   }

   #[test]
   fn range_lookup_test() -> Result<()> {
      use tempfile::tempdir;
      println!("BEGINNING TEST!");

      let mut sdb = crate::SplinterDB::new::<crate::rust_cfg::DefaultSdb>();

      let data_dir = tempdir()?; // is removed on drop
      let data_file = data_dir.path().join("db.splinterdb");

      sdb.db_create(
         &data_file,
         &crate::DBConfig {
            cache_size_bytes: 1024 * 1024,
            disk_size_bytes: 30 * 1024 * 1024,
            max_key_size: 23,
            max_value_size: 100,
         },
      )?;
      println!("SUCCESSFULLY CREATED DB!");

      sdb.insert(&(b"some-key-0".to_vec()), &(b"some-value-0".to_vec()))?;
      sdb.insert(&(b"some-key-3".to_vec()), &(b"some-value-3".to_vec()))?;
      sdb.insert(&(b"some-key-5".to_vec()), &(b"some-value-5".to_vec()))?;
      sdb.insert(&(b"some-key-6".to_vec()), &(b"some-value-6".to_vec()))?;

      let mut found: Vec<(Vec<u8>, Vec<u8>)> = Vec::new(); // to collect results
      let mut iter = sdb.range(None)?;
      loop {
         match iter.next() {
            Ok(Some(r)) => found.push((r.key.to_vec(), r.value.to_vec())),
            Ok(None) => break,
            Err(e) => return Err(e),
         }
      }

      println!("Found {} results", found.len());

      assert_eq!(found[0], (b"some-key-0".to_vec(), b"some-value-0".to_vec()));
      assert_eq!(found[1], (b"some-key-3".to_vec(), b"some-value-3".to_vec()));
      assert_eq!(found[2], (b"some-key-5".to_vec(), b"some-value-5".to_vec()));
      assert_eq!(found[3], (b"some-key-6".to_vec(), b"some-value-6".to_vec()));

      drop(iter);

      println!("Dropping SplinterDB!");
      drop(sdb);
      println!("Drop done! Exiting");
      Ok(())
   }

   // Simple implementation of some merge behavior for testing
   // When an update is performed, simply make the value the larger of the two
   struct SimpleMerge {}
   impl SdbRustDataFuncs for SimpleMerge {
      // leave all functions but merges as default

      fn merge(_key: &[u8], old_msg: SdbMessage, new_msg: SdbMessage) -> Result<SdbMessage>
      {
         let old_val = old_msg.data;
         let new_val = new_msg.data;

         let upd_val = if old_val >= new_val {
            old_val
         } else {
            new_val
         };

         // if old insert and new update -> insert
         // otherwise -> update
         match old_msg.msg_type {
            SdbMessageType::INSERT => Ok(SdbMessage {
               msg_type: SdbMessageType::INSERT,
               data: upd_val,
            }),
            SdbMessageType::UPDATE => Ok(SdbMessage {
               msg_type: SdbMessageType::UPDATE,
               data: upd_val,
            }),
            _ => panic!("Expected INSERT or UPDATE"),
         }
      }
      fn merge_final(_key: &[u8], oldest_msg: SdbMessage) -> Result<SdbMessage>
      {
         // Simply label this message as an insertion
         Ok(SdbMessage {
            msg_type: SdbMessageType::INSERT,
            data: oldest_msg.data,
         })
      }
   }

   #[test]
   fn simple_merge_test() -> Result<()> {
      use tempfile::tempdir;
      println!("BEGINNING TEST!");

      let mut sdb = crate::SplinterDB::new::<SimpleMerge>();

      let data_dir = tempdir()?; // is removed on drop
      let data_file = data_dir.path().join("db.splinterdb");

      sdb.db_create(
         &data_file,
         &crate::DBConfig {
            cache_size_bytes: 1024 * 1024,
            disk_size_bytes: 30 * 1024 * 1024,
            max_key_size: 23,
            max_value_size: 100,
         },
      )?;
      println!("SUCCESSFULLY CREATED DB!");

      sdb.insert(&(b"some-key-0".to_vec()), &(b"some-value-0".to_vec()))?;
      sdb.insert(&(b"some-key-3".to_vec()), &(b"some-value-3".to_vec()))?;
      sdb.insert(&(b"some-key-5".to_vec()), &(b"some-value-5".to_vec()))?;
      sdb.insert(&(b"some-key-6".to_vec()), &(b"some-value-6".to_vec()))?;

      println!("SUCCESSFULLY PERFORMED INSERTIONS!");

      sdb.update(&(b"some-key-0".to_vec()), &(b"some-value-3".to_vec()))?;
      sdb.update(&(b"some-key-3".to_vec()), &(b"some-value-2".to_vec()))?;
      sdb.update(&(b"some-key-5".to_vec()), &(b"some-value-5".to_vec()))?;
      sdb.update(&(b"some-key-6".to_vec()), &(b"some-value-9999999999999999999".to_vec()))?;

      // also issue update to key that does not exist
      sdb.update(&(b"some-key-2".to_vec()), &(b"some-value-2".to_vec()))?;

      println!("SUCCESSFULLY PERFORMED MERGES!");

      let mut found: Vec<(Vec<u8>, Vec<u8>)> = Vec::new(); // to collect results
      let mut iter = sdb.range(None)?;
      loop {
         match iter.next() {
            Ok(Some(r)) => found.push((r.key.to_vec(), r.value.to_vec())),
            Ok(None) => break,
            Err(e) => return Err(e),
         }
      }

      println!("Found {} results", found.len());

      assert_eq!(found[0], (b"some-key-0".to_vec(), b"some-value-3".to_vec()));
      assert_eq!(found[1], (b"some-key-2".to_vec(), b"some-value-2".to_vec()));
      assert_eq!(found[2], (b"some-key-3".to_vec(), b"some-value-3".to_vec()));
      assert_eq!(found[3], (b"some-key-5".to_vec(), b"some-value-5".to_vec()));
      assert_eq!(found[4], (b"some-key-6".to_vec(), b"some-value-9999999999999999999".to_vec()));

      drop(iter);

      println!("Dropping SplinterDB!");
      drop(sdb);
      println!("Drop done! Exiting");
      Ok(())
   }

   // Test of performing two insertions and lookup in a column family
   #[test]
   fn cf_ins_test() -> Result<()> {
      use tempfile::tempdir;
      println!("BEGINNING TEST!");

      let mut sdb = crate::SplinterDBWithColumnFamilies::new();

      let data_dir = tempdir()?; // is removed on drop
      let data_file = data_dir.path().join("db.splinterdb");

      sdb.db_create(
         &data_file,
         &crate::DBConfig {
            cache_size_bytes: 1024 * 1024,
            disk_size_bytes: 30 * 1024 * 1024,
            max_key_size: 23,
            max_value_size: 100,
         },
      )?;

      println!("SUCCESSFULLY CREATED DB!");

      let cf: crate::SplinterColumnFamily = sdb.column_family_create::<crate::DefaultSdb>(23);

      println!("SUCCESSFULLY CREATED CF!");

      let key = b"some-key-0".to_vec();
      let value = b"some-value-0".to_vec();

      println!("Inserting first key/value!");
      cf.insert(&key, &value)?;
      println!("Inserting second key/value!");
      cf.insert(&(b"some-key-4".to_vec()), &(b"some-value-4".to_vec()))?;
      println!("SUCCESSFULLY INSERTED TO DB!");

      let res = cf.lookup(&key)?;
      match res {
         crate::LookupResult::NotFound => panic!("inserted key not found"),
         crate::LookupResult::FoundTruncated(_) => panic!("inserted key found but truncated"),
         crate::LookupResult::Found(v) => assert_eq!(v, value),
      }

      println!("SUCCESSFULLY PERFORMED LOOKUP!");

      println!("Dropping Column Family!");
      drop(cf);
      println!("Dropping SplinterDB!");
      drop(sdb);
      println!("Drop done! Exiting");
      Ok(())
   }

   // test combining multiple column families in one instance
   #[test]
   fn cf_many_cfs_test() -> Result<()> {
      use tempfile::tempdir;
      println!("BEGINNING TEST!");

      let mut sdb = crate::SplinterDBWithColumnFamilies::new();

      let data_dir = tempdir()?; // is removed on drop
      let data_file = data_dir.path().join("db.splinterdb");

      sdb.db_create(
         &data_file,
         &crate::DBConfig {
            cache_size_bytes: 1024 * 1024,
            disk_size_bytes: 30 * 1024 * 1024,
            max_key_size: 23,
            max_value_size: 100,
         },
      )?;

      println!("SUCCESSFULLY CREATED DB!");

      let cf0: crate::SplinterColumnFamily = sdb.column_family_create::<crate::DefaultSdb>(23);
      let cf1: crate::SplinterColumnFamily = sdb.column_family_create::<crate::DefaultSdb>(23);
      let cf2: crate::SplinterColumnFamily = sdb.column_family_create::<crate::DefaultSdb>(23);
      let cf3: crate::SplinterColumnFamily = sdb.column_family_create::<crate::DefaultSdb>(23);

      cf0.insert(&(b"some-key-0".to_vec()), &(b"some-val-cf0".to_vec()))?;
      cf1.insert(&(b"some-key-0".to_vec()), &(b"some-val-cf1".to_vec()))?;
      cf2.insert(&(b"some-key-0".to_vec()), &(b"some-val-cf2".to_vec()))?;
      cf3.insert(&(b"some-key-0".to_vec()), &(b"some-val-cf3".to_vec()))?;

      cf0.insert(&(b"some-key-1".to_vec()), &(b"some-val-cf0".to_vec()))?;
      cf1.insert(&(b"some-key-1".to_vec()), &(b"some-val-cf1".to_vec()))?;
      cf2.insert(&(b"some-key-1".to_vec()), &(b"some-val-cf2".to_vec()))?;
      cf3.insert(&(b"some-key-1".to_vec()), &(b"some-val-cf3".to_vec()))?;

      println!("SUCCESSFULLY PERFORMED INSERTIONS!");

      // lookup key 0 in all column families
      let res = cf0.lookup(&(b"some-key-0".to_vec()))?;
      match res {
         crate::LookupResult::NotFound => panic!("inserted key not found"),
         crate::LookupResult::FoundTruncated(_) => panic!("inserted key found but truncated"),
         crate::LookupResult::Found(v) => assert_eq!(v, b"some-val-cf0".to_vec()),
      }
      let res = cf1.lookup(&(b"some-key-0".to_vec()))?;
      match res {
         crate::LookupResult::NotFound => panic!("inserted key not found"),
         crate::LookupResult::FoundTruncated(_) => panic!("inserted key found but truncated"),
         crate::LookupResult::Found(v) => assert_eq!(v, b"some-val-cf1".to_vec()),
      }
      let res = cf2.lookup(&(b"some-key-0".to_vec()))?;
      match res {
         crate::LookupResult::NotFound => panic!("inserted key not found"),
         crate::LookupResult::FoundTruncated(_) => panic!("inserted key found but truncated"),
         crate::LookupResult::Found(v) => assert_eq!(v, b"some-val-cf2".to_vec()),
      }
      let res = cf3.lookup(&(b"some-key-0".to_vec()))?;
      match res {
         crate::LookupResult::NotFound => panic!("inserted key not found"),
         crate::LookupResult::FoundTruncated(_) => panic!("inserted key found but truncated"),
         crate::LookupResult::Found(v) => assert_eq!(v, b"some-val-cf3".to_vec()),
      }

      println!("SUCCESSFULLY PERFORMED LOOKUPS!");

      println!("Dropping Column Families!");
      drop(cf0);
      drop(cf1);
      drop(cf2);
      drop(cf3);
      println!("Dropping SplinterDB!");
      drop(sdb);
      println!("Drop done! Exiting");
      Ok(())
   }

   #[test]
   fn cf_range_test() -> Result<()> {
      use tempfile::tempdir;
      println!("BEGINNING TEST!");

      let mut sdb = crate::SplinterDBWithColumnFamilies::new();

      let data_dir = tempdir()?; // is removed on drop
      let data_file = data_dir.path().join("db.splinterdb");

      sdb.db_create(
         &data_file,
         &crate::DBConfig {
            cache_size_bytes: 1024 * 1024,
            disk_size_bytes: 30 * 1024 * 1024,
            max_key_size: 23,
            max_value_size: 100,
         },
      )?;

      println!("SUCCESSFULLY CREATED DB!");

      let cf0: crate::SplinterColumnFamily = sdb.column_family_create::<crate::DefaultSdb>(23);
      let cf1: crate::SplinterColumnFamily = sdb.column_family_create::<crate::DefaultSdb>(23);

      let keys: [Vec<u8>; 4] = [b"some-key-0".to_vec(), b"some-key-1".to_vec(), b"some-key-2".to_vec(), b"some-key-3".to_vec()];
      let vals: [Vec<u8>; 2] = [b"some-val-cf0".to_vec(), b"some-val-cf1".to_vec()];
      let cfs: [&crate::SplinterColumnFamily; 2] = [&cf0, &cf1];

      for i in 0..2 {
         for k in 0..4 {
            cfs[i].insert(&keys[k], &vals[i])?;
         }
      }
      println!("SUCCESSFULLY PERFORMED INSERTIONS!");
      
      for i in 0..2 {
         let mut iter = cfs[i].range(None)?;
         let mut found: Vec<(Vec<u8>, Vec<u8>)> = Vec::new(); // to collect results
         loop {
            match iter.next() {
               Ok(Some(r)) => found.push((r.key.to_vec(), r.value.to_vec())),
               Ok(None) => break,
               Err(e) => return Err(e),
            }
         }
         assert_eq!(found[0], (keys[0].clone(), vals[i].clone()));
         assert_eq!(found[1], (keys[1].clone(), vals[i].clone()));
         assert_eq!(found[2], (keys[2].clone(), vals[i].clone()));
         assert_eq!(found[3], (keys[3].clone(), vals[i].clone()));
      }      

      println!("SUCCESSFULLY PERFORMED RANGE LOOKUPS!");



      println!("Dropping Column Families!");
      drop(cf0);
      drop(cf1);
      println!("Dropping SplinterDB!");
      drop(sdb);
      println!("Drop done! Exiting");
      Ok(())
   }

   struct ReversedSortConfig {}
   impl SdbRustDataFuncs for ReversedSortConfig {
      fn key_comp(key1: &[u8], key2: &[u8]) -> std::cmp::Ordering {
         key2.cmp(&key1)
      }
   }

   #[test]
   fn cf_diff_configs_test() -> Result<()> {
      use tempfile::tempdir;
      println!("BEGINNING TEST!");

      let mut sdb = crate::SplinterDBWithColumnFamilies::new();

      let data_dir = tempdir()?; // is removed on drop
      let data_file = data_dir.path().join("db.splinterdb");

      sdb.db_create(
         &data_file,
         &crate::DBConfig {
            cache_size_bytes: 1024 * 1024,
            disk_size_bytes: 30 * 1024 * 1024,
            max_key_size: 23,
            max_value_size: 100,
         },
      )?;

      println!("SUCCESSFULLY CREATED DB!");

      let cf0: crate::SplinterColumnFamily = sdb.column_family_create::<crate::DefaultSdb>(23);
      let cf1: crate::SplinterColumnFamily = sdb.column_family_create::<ReversedSortConfig>(23);

      let keys: [Vec<u8>; 4] = [b"some-key-0".to_vec(), b"some-key-1".to_vec(), b"some-key-2".to_vec(), b"some-key-3".to_vec()];
      let vals: [Vec<u8>; 2] = [b"some-val-cf0".to_vec(), b"some-val-cf1".to_vec()];
      let cfs: [&crate::SplinterColumnFamily; 2] = [&cf0, &cf1];

      for i in 0..2 {
         for k in 0..4 {
            cfs[i].insert(&keys[k], &vals[i])?;
         }
      }
      println!("SUCCESSFULLY PERFORMED INSERTIONS!");
      {
         let mut iter0 = cfs[0].range(None)?;
         let mut found0: Vec<(Vec<u8>, Vec<u8>)> = Vec::new(); // to collect results
         loop {
            match iter0.next() {
               Ok(Some(r)) => found0.push((r.key.to_vec(), r.value.to_vec())),
               Ok(None) => break,
               Err(e) => return Err(e),
            }
         }
         assert_eq!(found0[0], (keys[0].clone(), vals[0].clone()));
         assert_eq!(found0[1], (keys[1].clone(), vals[0].clone()));
         assert_eq!(found0[2], (keys[2].clone(), vals[0].clone()));
         assert_eq!(found0[3], (keys[3].clone(), vals[0].clone()));   

         let mut iter1 = cfs[1].range(None)?;
         let mut found1: Vec<(Vec<u8>, Vec<u8>)> = Vec::new(); // to collect results
         loop {
            match iter1.next() {
               Ok(Some(r)) => found1.push((r.key.to_vec(), r.value.to_vec())),
               Ok(None) => break,
               Err(e) => return Err(e),
            }
         }
         assert_eq!(found1[3], (keys[0].clone(), vals[1].clone()));
         assert_eq!(found1[2], (keys[1].clone(), vals[1].clone()));
         assert_eq!(found1[1], (keys[2].clone(), vals[1].clone()));
         assert_eq!(found1[0], (keys[3].clone(), vals[1].clone()));
      }
      println!("SUCCESSFULLY PERFORMED RANGE LOOKUPS!");

      println!("Dropping Column Families!");
      drop(cf0);
      drop(cf1);
      println!("Dropping SplinterDB!");
      drop(sdb);
      println!("Drop done! Exiting");
      Ok(())
   }
}


