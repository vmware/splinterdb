use std::io::{Error, Result};
use std::path::Path;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
pub struct DBConfig {
    pub cache_size_bytes: usize,
    pub disk_size_bytes: usize,
    pub max_key_size: u8,
    pub max_value_size: u8,
}

#[derive(Debug)]
pub struct SplinterDB {
    _inner: *mut splinterdb_sys::splinterdb,
}

unsafe impl Sync for SplinterDB {}
unsafe impl Send for SplinterDB {}

impl Drop for SplinterDB {
    fn drop(&mut self) {
        unsafe { splinterdb_sys::splinterdb_close(self._inner) };
    }
}

#[derive(Debug, PartialEq)]
pub enum LookupResult {
    Found(Vec<u8>),
    FoundTruncated(Vec<u8>),
    NotFound,
}

fn as_result(rc: ::std::os::raw::c_int) -> Result<()> {
    if rc != 0 {
        Err(Error::from_raw_os_error(rc))
    } else {
        Ok(())
    }
}

#[derive(Debug)]
pub struct IteratorResult<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
}

#[derive(Debug)]
pub struct RangeIterator<'a> {
    _inner: *mut splinterdb_sys::splinterdb_iterator,
    _marker: ::std::marker::PhantomData<splinterdb_sys::splinterdb_iterator>,
    _parent_marker: ::std::marker::PhantomData<&'a splinterdb_sys::splinterdb>,
    state: Option<IteratorResult<'a>>,
}

impl<'a> Drop for RangeIterator<'a> {
    fn drop(&mut self) {
        unsafe { splinterdb_sys::splinterdb_iterator_deinit(self._inner) }
    }
}

impl<'a> RangeIterator<'a> {
    pub fn new(iter: *mut splinterdb_sys::splinterdb_iterator) -> RangeIterator<'a> {
        RangeIterator {
            _inner: iter,
            _marker: ::std::marker::PhantomData,
            _parent_marker: ::std::marker::PhantomData,
            state: None,
        }
    }

    // stashes current state of the iterator from the C API
    fn _stash_current(&mut self) {
        let mut key_out = ::std::ptr::null();
        let mut val_out = ::std::ptr::null();
        let mut key_bytes_out: u64 = 0;
        let mut val_bytes_out: u64 = 0;

        let (key, value): (&[u8], &[u8]) = unsafe {
            splinterdb_sys::splinterdb_iterator_get_current(
                self._inner,
                &mut key_bytes_out,
                &mut key_out,
                &mut val_bytes_out,
                &mut val_out,
            );
            (
                ::std::slice::from_raw_parts(
                    ::std::mem::transmute(key_out),
                    key_bytes_out as usize,
                ),
                ::std::slice::from_raw_parts(
                    ::std::mem::transmute(val_out),
                    val_bytes_out as usize,
                ),
            )
        };
        let r = IteratorResult { key, value };
        self.state = Some(r);
    }

    fn _inner_advance(&mut self) {
        unsafe { splinterdb_sys::splinterdb_iterator_next(self._inner) };
    }

    // almost an iterator, but we need to be able to return errors
    // and retain ownership of the result
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<&IteratorResult>> {
        // Rust iterator expects to start just before the first element
        // but Splinter iterators start at the first element
        // so we only call _inner_advance if its our first iteration
        if self.state.is_some() {
            self._inner_advance();
        }

        let valid = unsafe { splinterdb_sys::splinterdb_iterator_valid(self._inner) };
        if valid == 0 {
            let rc = unsafe { splinterdb_sys::splinterdb_iterator_status(self._inner) };
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

impl SplinterDB {
    pub fn register_thread(&self) {
        unsafe { splinterdb_sys::splinterdb_register_thread(self._inner) };
    }

    pub fn deregister_thread(&self) {
        unsafe { splinterdb_sys::splinterdb_deregister_thread(self._inner) };
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let rc = unsafe {
            splinterdb_sys::splinterdb_insert(
                self._inner,
                key.len() as u64,
                ::std::mem::transmute(key.as_ptr()),
                value.len() as u64,
                ::std::mem::transmute(value.as_ptr()),
            )
        };
        as_result(rc)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let rc = unsafe {
            splinterdb_sys::splinterdb_delete(
                self._inner,
                key.len() as u64,
                ::std::mem::transmute(key.as_ptr()),
            )
        };
        as_result(rc)
    }

    pub fn lookup(&self, key: &[u8]) -> Result<LookupResult> {
        unsafe {
            let mut lr: splinterdb_sys::splinterdb_lookup_result = std::mem::zeroed();
            splinterdb_sys::splinterdb_lookup_result_init(
                self._inner,
                &mut lr,
                0,
                std::ptr::null_mut(),
            );

            let rc = splinterdb_sys::splinterdb_lookup(
                self._inner,
                key.len() as u64,
                ::std::mem::transmute(key.as_ptr()),
                &mut lr,
            );
            as_result(rc)?;

            let found = splinterdb_sys::splinterdb_lookup_found(&lr);
            if found == 0 {
                return Ok(LookupResult::NotFound);
            }

            let mut value_ptr: *const std::os::raw::c_char = std::mem::zeroed();
            let mut value_size: u64 = 0;
            let rc = splinterdb_sys::splinterdb_lookup_result_value(
                self._inner,
                &lr,
                &mut value_size,
                &mut value_ptr,
            );
            as_result(rc)?;

            let mut value: Vec<u8> = vec![0; value_size as usize];
            std::ptr::copy(
                value_ptr,
                std::mem::transmute(value.as_mut_ptr()),
                value_size as usize,
            );
            Ok(LookupResult::Found(value))
        }
    }

    pub fn range(&self, start_key: Option<&[u8]>) -> Result<RangeIterator> {
        let mut iter: *mut splinterdb_sys::splinterdb_iterator = std::ptr::null_mut();

        let rc = unsafe {
            let (start_ptr, start_ptr_len) = match start_key {
                Some(s) => (::std::mem::transmute(s.as_ptr()), s.len()),
                None => (::std::ptr::null(), 0),
            };
            splinterdb_sys::splinterdb_iterator_init(
                self._inner,
                &mut iter,
                start_ptr_len as u64,
                start_ptr,
            )
        };
        as_result(rc)?;
        Ok(RangeIterator::new(iter))
    }
}

fn path_as_cstring<P: AsRef<Path>>(path: P) -> std::ffi::CString {
    let as_os_str = path.as_ref().as_os_str();
    let as_str = as_os_str.to_str().unwrap();
    std::ffi::CString::new(as_str).unwrap()
}

fn db_create_or_open<P: AsRef<Path>>(
    path: &P,
    cfg: &DBConfig,
    open_existing: bool,
) -> Result<SplinterDB> {
    let path = path_as_cstring(path); // don't drop until init is done
    let mut sdb_cfg: splinterdb_sys::splinterdb_config = unsafe { std::mem::zeroed() };
    sdb_cfg.filename = path.as_ptr();
    sdb_cfg.cache_size = cfg.cache_size_bytes as u64;
    sdb_cfg.disk_size = cfg.disk_size_bytes as u64;

    let mut splinterdb: *mut splinterdb_sys::splinterdb = std::ptr::null_mut();

    unsafe {
        splinterdb_sys::default_data_config_init(
            cfg.max_key_size as u64,
            cfg.max_value_size as u64,
            &mut sdb_cfg.data_cfg,
        );
    };

    let rc = if open_existing {
        unsafe { splinterdb_sys::splinterdb_open(&sdb_cfg, &mut splinterdb) }
    } else {
        unsafe { splinterdb_sys::splinterdb_create(&sdb_cfg, &mut splinterdb) }
    };
    as_result(rc)?;

    Ok(SplinterDB { _inner: splinterdb })
}

pub fn db_create<P: AsRef<Path>>(path: &P, cfg: &DBConfig) -> Result<SplinterDB> {
    db_create_or_open(path, cfg, false)
}

pub fn db_open<P: AsRef<Path>>(path: &P, cfg: &DBConfig) -> Result<SplinterDB> {
    db_create_or_open(path, cfg, true)
}

mod tests;
