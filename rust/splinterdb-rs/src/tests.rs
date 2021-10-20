#[cfg(test)]
mod tests {
    #[test]
    fn simple_journey() -> std::io::Result<()> {
        use tempfile::tempdir;

        let data_dir = tempdir()?; // is removed on drop
        let data_file = data_dir.path().join("db.splinterdb");

        let db = crate::db_create(
            &data_file,
            &crate::DBConfig {
                cache_size_bytes: 1024 * 1024,
                disk_size_bytes: 30 * 1024 * 1024,
                max_key_size: 23,
                max_value_size: 100,
            },
        )?;

        let key = b"some-key-0".to_vec();
        let value = b"some-value-0".to_vec();
        db.insert(&key, &value)?;

        let res = db.lookup(&key)?;
        match res {
            crate::LookupResult::NotFound => panic!("inserted key not found"),
            crate::LookupResult::FoundTruncated(_) => panic!("inserted key found but truncated"),
            crate::LookupResult::Found(v) => assert_eq!(v, value),
        }

        db.insert(&(b"some-key-4".to_vec()), &(b"some-value-4".to_vec()))?;
        db.insert(&(b"some-key-5".to_vec()), &(b"some-value-5".to_vec()))?;
        db.delete(&(b"some-key-4".to_vec()))?;
        db.insert(&(b"some-key-6".to_vec()), &(b"some-value-6".to_vec()))?;
        db.insert(&(b"some-key-3".to_vec()), &(b"some-value-3".to_vec()))?;

        let res = db.lookup(&(b"some-key-5".to_vec()))?;
        assert_eq!(res, crate::LookupResult::Found(b"some-value-5".to_vec()));

        let res = db.lookup(&(b"some-key-4".to_vec()))?;
        assert_eq!(res, crate::LookupResult::NotFound);

        let mut found: Vec<(Vec<u8>, Vec<u8>)> = Vec::new(); // to collect results
        let mut iter = db.range(None)?;
        loop {
            match iter.next() {
                Ok(Some(r)) => found.push((r.key.to_vec(), r.value.to_vec())),
                Ok(None) => break,
                Err(e) => return Err(e),
            }
        }

        assert_eq!(found[0], (b"some-key-0".to_vec(), b"some-value-0".to_vec()));
        assert_eq!(found[1], (b"some-key-3".to_vec(), b"some-value-3".to_vec()));
        assert_eq!(found[2], (b"some-key-5".to_vec(), b"some-value-5".to_vec()));
        assert_eq!(found[3], (b"some-key-6".to_vec(), b"some-value-6".to_vec()));

        drop(iter);
        drop(db);
        Ok(())
    }
}
