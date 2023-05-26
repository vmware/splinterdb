// Tests of the splinterdb-rs library
//
// If you add a new function to the public API of this library, add a test here
// (or extend an existing test) to demonstrate how to use it.

#[cfg(test)]
mod tests {

    // Test of performing two insertions and lookup
    #[test]
    fn ins_test() -> std::io::Result<()> {
        use splinterdb_sys::slice;
        use tempfile::tempdir;
        println!("BEGINNING TEST!");

        let mut sdb = crate::SplinterDB::create_uninit_obj();

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
    fn ins_and_del_test() -> std::io::Result<()> {
        use tempfile::tempdir;
        println!("BEGINNING TEST!");

        let mut sdb = crate::SplinterDB::create_uninit_obj();

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

    #[test]
    fn overwrite_test() -> std::io::Result<()> {
        use tempfile::tempdir;
        println!("BEGINNING TEST!");

        let mut sdb = crate::SplinterDB::create_uninit_obj();

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
    fn range_lookup_test() -> std::io::Result<()> {
        use tempfile::tempdir;
        println!("BEGINNING TEST!");

        let mut sdb = crate::SplinterDB::create_uninit_obj();

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
}
