#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(test)]
mod tests {

	fn path_as_cstring<P: AsRef<std::path::Path>>(path: P) -> std::ffi::CString {
        let as_os_str = path.as_ref().as_os_str();
        let as_str = as_os_str.to_str().unwrap();
        std::ffi::CString::new(as_str).unwrap()
    }

    // Really basic "smoke" test of the generated code, just to see that the
    // C library actually links.
    #[test]
    fn invoke_things() {
        use tempfile::tempdir;

        let data_dir = tempdir().unwrap(); // is removed on drop
        let data_file = data_dir.path().join("db.splinterdb");
        let path = path_as_cstring(data_file); // don't drop until init is done

        println!("path = {:?}", path);

        let mut data_config: super::data_config = unsafe { std::mem::zeroed() };
        let mut cfg: super::splinterdb_config = unsafe { std::mem::zeroed() };
        cfg.filename = path.as_ptr();
        cfg.cache_size = 200 * 1024 * 1024;
        cfg.disk_size = 400 * 1024 * 1024;
        cfg.data_cfg = &mut data_config;

        println!("CONFIG CREATED!");

        let mut splinterdb: *mut super::splinterdb = std::ptr::null_mut();

        println!("SPLINTER POINTER CREATED!");

        unsafe {
            super::default_data_config_init(
                32,
                cfg.data_cfg,
            )
        };

        println!("CONFIG INIT!");

        let rc = unsafe {
            super::splinterdb_create(&cfg, &mut splinterdb)
        };
        assert_eq!(rc, 0);

        println!("SPLINTER CREATED!");

        unsafe { super::splinterdb_close(&mut splinterdb) };

        println!("SPLINTER CLOSED!");
    }
}