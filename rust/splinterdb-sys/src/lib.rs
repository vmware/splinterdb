// Low level library wrapping the SplinterDB C API

// Allow some lints from the generated code
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(unused_imports)]
#![allow(unused)]

// The real content of this library comes from the generated code
include!("generated.rs");

// Tests of the generated code can go here.
// But before adding a test here, consider whether it might belong elsewhere.
// Perhaps it should be written in C, in the main library?
// Or maybe it should live in the splinterdb-rs library?
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
        unsafe { super::platform_set_log_streams(super::stdout, super::stderr); }
        use tempfile::tempdir;

        let data_dir = tempdir().unwrap(); // is removed on drop
        let data_file = data_dir.path().join("db.splinterdb");
        let path = path_as_cstring(data_file); // don't drop until init is done

        let mut data_cfg: super::data_config = unsafe { std::mem::zeroed() };
        const max_key_size : u64 = 102;
        unsafe {
            super::default_data_config_init(max_key_size, &mut data_cfg);
        };

        let mut cfg: super::splinterdb_config = unsafe { std::mem::zeroed() };
        cfg.filename = path.as_ptr();
        cfg.cache_size = 200 * 1024 * 1024;
        cfg.disk_size = 400 * 1024 * 1024;
        cfg.data_cfg = &mut data_cfg;

        let mut splinterdb: *mut super::splinterdb = std::ptr::null_mut();
        let rc = unsafe { super::splinterdb_create(&cfg, &mut splinterdb) };
        assert_eq!(rc, 0);
        unsafe { super::splinterdb_close(&mut splinterdb) };
    }
}
