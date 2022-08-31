use std::io::{Error, Result};
use std::path::Path;

#[derive(Debug, Copy, Clone)]
pub struct DBConfig {
    pub cache_size_bytes: usize,
    pub disk_size_bytes: usize,
    pub max_key_size: u8,
}

pub const MAX_KEY_SIZE: u32 = 102;

#[derive(Debug)]
pub struct SplinterDB {
    inner: *mut splinterdb_sys::splinterdb,

    // retain the data config, since it must live at least as long as the splinterdb
    // since splinterdb retains a reference here, the data_config cannot move
    _data_cfg: Box<splinterdb_sys::data_config>,
}

unsafe impl Sync for SplinterDB {}
unsafe impl Send for SplinterDB {}

impl Drop for SplinterDB {
    fn drop(&mut self) {
        unsafe { splinterdb_sys::splinterdb_close(&mut self.inner) };
    }
}

impl SplinterDB {
    fn create_or_open<P: AsRef<Path>>(
        path: &P,
        cfg: &DBConfig,
        open_existing: bool,
    ) -> Result<SplinterDB> {
        let mut data_cfg: Box<splinterdb_sys::data_config> =
            Box::new(unsafe { std::mem::zeroed() });
        unsafe {
            splinterdb_sys::default_data_config_init(cfg.max_key_size as u64, data_cfg.as_mut());
        }

        let mut sdb_cfg: splinterdb_sys::splinterdb_config = unsafe { std::mem::zeroed() };
        sdb_cfg.data_cfg = data_cfg.as_mut();

        let path = path_as_cstring(path); // don't drop until init is done
        sdb_cfg.filename = path.as_ptr();
        sdb_cfg.cache_size = cfg.cache_size_bytes as u64;
        sdb_cfg.disk_size = cfg.disk_size_bytes as u64;

        let mut splinterdb: *mut splinterdb_sys::splinterdb = std::ptr::null_mut();

        let rc = if open_existing {
            unsafe { splinterdb_sys::splinterdb_open(&sdb_cfg, &mut splinterdb) }
        } else {
            unsafe { splinterdb_sys::splinterdb_create(&sdb_cfg, &mut splinterdb) }
        };
        as_result(rc)?;

        Ok(SplinterDB {
            inner: splinterdb,
            _data_cfg: data_cfg,
        })
    }

    pub fn create<P: AsRef<Path>>(path: &P, cfg: &DBConfig) -> Result<SplinterDB> {
        Self::create_or_open(path, cfg, false)
    }

    pub fn open<P: AsRef<Path>>(path: &P, cfg: &DBConfig) -> Result<SplinterDB> {
        Self::create_or_open(path, cfg, true)
    }

    pub fn register_thread(&self) {
        unsafe { splinterdb_sys::splinterdb_register_thread(self.inner) };
    }

    pub fn deregister_thread(&self) {
        unsafe { splinterdb_sys::splinterdb_deregister_thread(self.inner) };
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let rc = unsafe {
            splinterdb_sys::splinterdb_insert(
                self.inner,
                as_splinter_slice(key),
                as_splinter_slice(value),
            )
        };
        as_result(rc)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let rc = unsafe { splinterdb_sys::splinterdb_delete(self.inner, as_splinter_slice(key)) };
        as_result(rc)
    }

    pub fn lookup(&self, key: &[u8], out_result: &mut LookupResult) -> Result<()> {
        let rc = unsafe {
            splinterdb_sys::splinterdb_lookup(
                self.inner,
                as_splinter_slice(key),
                &mut out_result.inner,
            )
        };
        as_result(rc)?;
        Ok(())
    }

    pub fn range(&self, start_key: Option<&[u8]>) -> Result<RangeIterator> {
        let mut iter: *mut splinterdb_sys::splinterdb_iterator = std::ptr::null_mut();

        let rc = unsafe {
            let start_slice = match start_key {
                Some(s) => as_splinter_slice(s),
                None => splinterdb_sys::slice {
                    data: ::std::ptr::null(),
                    length: 0,
                },
            };
            splinterdb_sys::splinterdb_iterator_init(self.inner, &mut iter, start_slice)
        };
        as_result(rc)?;
        Ok(RangeIterator::new(iter))
    }


    pub fn set_verbose_logging() {
        unsafe {
            splinterdb_sys::platform_set_log_streams(splinterdb_sys::stdout, splinterdb_sys::stderr);
        }
    }
}

pub struct LookupResult<'a> {
    inner: splinterdb_sys::splinterdb_lookup_result,
    _splinterdb: &'a SplinterDB,
}

impl<'a> LookupResult<'a> {
    pub fn new(splinterdb: &'a SplinterDB) -> LookupResult<'a> {
        let mut ret = LookupResult {
            inner: unsafe { std::mem::zeroed() },
            _splinterdb: splinterdb,
        };
        unsafe {
            splinterdb_sys::splinterdb_lookup_result_init(
                splinterdb.inner,
                &mut ret.inner,
                0,
                std::ptr::null_mut(),
            );
        }
        ret
    }

    pub fn get(&self) -> Option<&[u8]> {
        let found = unsafe { splinterdb_sys::splinterdb_lookup_found(&self.inner) };
        if found == 0 {
            return None;
        }
        unsafe {
            let mut val_slice: splinterdb_sys::slice = std::mem::zeroed();
            let rc = splinterdb_sys::splinterdb_lookup_result_value(&self.inner, &mut val_slice);
            assert!(rc == 0); // lookup_result_value only errors if result is not found
            let value: &[u8] = ::std::slice::from_raw_parts(
                ::std::mem::transmute(val_slice.data),
                val_slice.length as usize,
            );
            Some(value)
        }
    }
}

impl<'a> Drop for LookupResult<'a> {
    fn drop(&mut self) {
        unsafe { splinterdb_sys::splinterdb_lookup_result_deinit(&mut self.inner) }
    }
}

#[derive(Debug)]
pub struct IteratorResult<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
}

#[derive(Debug)]
pub struct RangeIterator<'a> {
    inner: *mut splinterdb_sys::splinterdb_iterator,
    _marker: ::std::marker::PhantomData<splinterdb_sys::splinterdb_iterator>,
    _parent_marker: ::std::marker::PhantomData<&'a splinterdb_sys::splinterdb>,
    state: Option<IteratorResult<'a>>,
}

impl<'a> Drop for RangeIterator<'a> {
    fn drop(&mut self) {
        unsafe { splinterdb_sys::splinterdb_iterator_deinit(self.inner) }
    }
}

impl<'a> RangeIterator<'a> {
    pub fn new(iter: *mut splinterdb_sys::splinterdb_iterator) -> RangeIterator<'a> {
        RangeIterator {
            inner: iter,
            _marker: ::std::marker::PhantomData,
            _parent_marker: ::std::marker::PhantomData,
            state: None,
        }
    }

    // stashes current state of the iterator from the C API
    fn _stash_current(&mut self) {
        let r = unsafe {
            let mut key_slice: splinterdb_sys::slice = std::mem::zeroed();
            let mut val_slice: splinterdb_sys::slice = std::mem::zeroed();

            splinterdb_sys::splinterdb_iterator_get_current(
                self.inner,
                &mut key_slice,
                &mut val_slice,
            );

            let key: &[u8] = ::std::slice::from_raw_parts(
                ::std::mem::transmute(key_slice.data),
                key_slice.length as usize,
            );
            let value: &[u8] = ::std::slice::from_raw_parts(
                ::std::mem::transmute(val_slice.data),
                val_slice.length as usize,
            );
            IteratorResult { key, value }
        };
        self.state = Some(r);
    }

    fn inner_advance(&mut self) {
        unsafe { splinterdb_sys::splinterdb_iterator_next(self.inner) };
    }

    // almost an iterator, but we need to be able to return errors
    // and retain ownership of the result
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<&IteratorResult>> {
        // Rust iterator expects to start just before the first element
        // but Splinter iterators start at the first element
        // so we only call inner_advance if its our first iteration
        if self.state.is_some() {
            self.inner_advance();
        }

        let valid = unsafe { splinterdb_sys::splinterdb_iterator_valid(self.inner) };
        if valid == 0 {
            let rc = unsafe { splinterdb_sys::splinterdb_iterator_status(self.inner) };
            as_result(rc)?;
            return Ok(None);
        }

        self._stash_current();

        match self.state {
            None => Ok(None),
            Some(ref r) => Ok(Some(r)),
        }
    }
}

unsafe fn as_splinter_slice(s: &[u8]) -> splinterdb_sys::slice {
    splinterdb_sys::slice {
        length: s.len() as u64,
        data: ::std::mem::transmute(s.as_ptr()),
    }
}

fn as_result(rc: ::std::os::raw::c_int) -> Result<()> {
    if rc != 0 {
        Err(Error::from_raw_os_error(rc))
    } else {
        Ok(())
    }
}

fn path_as_cstring<P: AsRef<Path>>(path: P) -> std::ffi::CString {
    let as_os_str = path.as_ref().as_os_str();
    let as_str = as_os_str.to_str().unwrap();
    std::ffi::CString::new(as_str).unwrap()
}

mod tests;
