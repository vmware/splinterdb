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
pub struct KvsbDB {
    _inner: *mut splinterdb_sys::kvstore_basic,
}

unsafe impl Sync for KvsbDB {}
unsafe impl Send for KvsbDB {}

impl Drop for KvsbDB {
    fn drop(&mut self) {
        unsafe { splinterdb_sys::kvstore_basic_close(self._inner) };
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
    _inner: *mut splinterdb_sys::kvstore_basic_iterator,
    _marker: ::std::marker::PhantomData<splinterdb_sys::kvstore_basic_iterator>,
    _parent_marker: ::std::marker::PhantomData<&'a splinterdb_sys::kvstore_basic>,
    state: Option<IteratorResult<'a>>,
}

impl<'a> Drop for RangeIterator<'a> {
    fn drop(&mut self) {
        unsafe { splinterdb_sys::kvstore_basic_iter_deinit(&mut self._inner) }
    }
}

impl<'a> RangeIterator<'a> {
    pub fn new(iter: *mut splinterdb_sys::kvstore_basic_iterator) -> RangeIterator<'a> {
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
        let mut key_bytes_out: usize = 0;
        let mut val_bytes_out: usize = 0;

        let (key, value): (&[u8], &[u8]) = unsafe {
            splinterdb_sys::kvstore_basic_iter_get_current(
                self._inner,
                &mut key_out,
                &mut key_bytes_out,
                &mut val_out,
                &mut val_bytes_out,
            );
            (
                ::std::slice::from_raw_parts(::std::mem::transmute(key_out), key_bytes_out),
                ::std::slice::from_raw_parts(::std::mem::transmute(val_out), val_bytes_out),
            )
        };
        let r = IteratorResult { key, value };
        self.state = Some(r);
    }

    fn _inner_advance(&mut self) {
        unsafe { splinterdb_sys::kvstore_basic_iter_next(self._inner) };
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

        let valid = unsafe { splinterdb_sys::kvstore_basic_iter_valid(self._inner) };
        if !valid {
            let rc = unsafe { splinterdb_sys::kvstore_basic_iter_status(self._inner) };
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

impl KvsbDB {
    pub fn register_thread(&self) {
        unsafe { splinterdb_sys::kvstore_basic_register_thread(self._inner) };
    }

    pub fn deregister_thread(&self) {
        unsafe { splinterdb_sys::kvstore_basic_deregister_thread(self._inner) };
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let rc = unsafe {
            splinterdb_sys::kvstore_basic_insert(
                self._inner,
                ::std::mem::transmute(key.as_ptr()),
                key.len(),
                ::std::mem::transmute(value.as_ptr()),
                value.len(),
            )
        };
        as_result(rc)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let rc = unsafe {
            splinterdb_sys::kvstore_basic_delete(
                self._inner,
                ::std::mem::transmute(key.as_ptr()),
                key.len(),
            )
        };
        as_result(rc)
    }

    pub fn lookup(&self, key: &[u8]) -> Result<LookupResult> {
        let mut val_out: Vec<u8> = vec![0; splinterdb_sys::MAX_MESSAGE_SIZE as usize];
        let mut val_bytes_out: usize = 0;
        let mut val_truncated_out: bool = false;
        let mut val_found_out: bool = false;
        let rc = unsafe {
            splinterdb_sys::kvstore_basic_lookup(
                self._inner,
                ::std::mem::transmute(key.as_ptr()),
                key.len(),
                ::std::mem::transmute(val_out.as_ptr()),
                val_out.len(),
                &mut val_bytes_out,
                &mut val_truncated_out,
                &mut val_found_out,
            )
        };
        as_result(rc)?;
        if !val_found_out {
            return Ok(LookupResult::NotFound);
        }
        if val_truncated_out {
            return Ok(LookupResult::FoundTruncated(val_out));
        }
        val_out.truncate(val_bytes_out);
        Ok(LookupResult::Found(val_out))
    }

    pub fn range(&self, start_key: Option<&[u8]>) -> Result<RangeIterator> {
        let mut iter: *mut splinterdb_sys::kvstore_basic_iterator = std::ptr::null_mut();

        let rc = unsafe {
            let (start_ptr, start_ptr_len) = match start_key {
                Some(s) => (::std::mem::transmute(s.as_ptr()), s.len()),
                None => (::std::ptr::null(), 0),
            };
            splinterdb_sys::kvstore_basic_iter_init(
                self._inner,
                &mut iter,
                start_ptr,
                start_ptr_len,
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
) -> Result<KvsbDB> {
    let path = path_as_cstring(path); // don't drop until init is done
    let cfg = splinterdb_sys::kvstore_basic_cfg {
        filename: path.as_ptr(),
        cache_size: cfg.cache_size_bytes,
        disk_size: cfg.disk_size_bytes,
        max_key_size: cfg.max_key_size as usize,
        max_value_size: cfg.max_value_size as usize,
        key_comparator: None,
        key_comparator_context: std::ptr::null_mut(),
        heap_handle: std::ptr::null_mut(),
        heap_id: std::ptr::null_mut(),
    };
    let cfg_ptr = &cfg as *const splinterdb_sys::kvstore_basic_cfg;
    let mut kvsb: *mut splinterdb_sys::kvstore_basic = std::ptr::null_mut();

    let rc = if open_existing {
        unsafe { splinterdb_sys::kvstore_basic_open(cfg_ptr, &mut kvsb) }
    } else {
        unsafe { splinterdb_sys::kvstore_basic_create(cfg_ptr, &mut kvsb) }
    };
    as_result(rc)?;

    Ok(KvsbDB { _inner: kvsb })
}

pub fn db_create<P: AsRef<Path>>(path: &P, cfg: &DBConfig) -> Result<KvsbDB> {
    db_create_or_open(path, cfg, false)
}

pub fn db_open<P: AsRef<Path>>(path: &P, cfg: &DBConfig) -> Result<KvsbDB> {
    db_create_or_open(path, cfg, true)
}

mod tests;
