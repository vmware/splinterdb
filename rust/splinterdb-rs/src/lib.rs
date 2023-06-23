use std::io::{Result};
use std::path::Path;

pub mod util;
pub use util::{DBConfig, LookupResult, IteratorResult};
use util::*;

pub mod rust_cfg;
pub use rust_cfg::{CompareResult, SdbMessageType, SdbMessage, SdbRustDataFuncs, DefaultSdb};
use rust_cfg::new_sdb_data_config;

pub mod splinter_cf;
pub use splinter_cf::{SplinterDBWithColumnFamilies, SplinterColumnFamily};

#[derive(Debug)]
pub struct SplinterDB {
   _inner: *mut splinterdb_sys::splinterdb,
   data_cfg: splinterdb_sys::data_config,
}

unsafe impl Sync for SplinterDB {}
unsafe impl Send for SplinterDB {}

impl Drop for SplinterDB {
   fn drop(&mut self) {
      unsafe { splinterdb_sys::splinterdb_close(&mut self._inner) };
   }
}

#[derive(Debug)]
pub struct RangeIterator<'a> {
   _inner: *mut splinterdb_sys::splinterdb_iterator,
   _marker: ::std::marker::PhantomData<splinterdb_sys::splinterdb_iterator>,
   _parent_marker: ::std::marker::PhantomData<&'a splinterdb_sys::splinterdb>,
   state: Option<IteratorResult<'a>>,
}

impl<'a> Drop for RangeIterator<'a>
{
   fn drop(&mut self)
   {
      unsafe { splinterdb_sys::splinterdb_iterator_deinit(self._inner) }
   }
}

impl<'a> RangeIterator<'a>
{
   pub fn new(iter: *mut splinterdb_sys::splinterdb_iterator) -> RangeIterator<'a> 
   {
      RangeIterator {
         _inner: iter,
         _marker: ::std::marker::PhantomData,
         _parent_marker: ::std::marker::PhantomData,
         state: None,
      }
   }

   // stashes current state of the iterator from the C API
   fn _stash_current(&mut self)
   {
      let mut key_out: splinterdb_sys::slice = splinterdb_sys::slice {
         length: 0,
         data: ::std::ptr::null(),
      };
      let mut val_out: splinterdb_sys::slice = splinterdb_sys::slice {
         length: 0,
         data: ::std::ptr::null(),
      };

      let (key, value): (&[u8], &[u8]) = unsafe {
         // get key and value
         splinterdb_sys::splinterdb_iterator_get_current(
            self._inner,
            &mut key_out,
            &mut val_out,
         );
         // parse key and value into rust slices
         (
            ::std::slice::from_raw_parts(
               ::std::mem::transmute(key_out.data),
               key_out.length as usize,
            ),
            ::std::slice::from_raw_parts(
               ::std::mem::transmute(val_out.data),
               val_out.length as usize,
            ),
         )
      };
      let r = IteratorResult { key, value };
      self.state = Some(r);
   }

   fn _inner_advance(&mut self)
   {
      unsafe { splinterdb_sys::splinterdb_iterator_next(self._inner) };
   }

   // almost an iterator, but we need to be able to return errors
   // and retain ownership of the result
   #[allow(clippy::should_implement_trait)]
   pub fn next(&mut self) -> Result<Option<&IteratorResult>>
   {
      // Rust iterator expects to start just before the first element
      // but Splinter iterators start at the first element
      // so we only call _inner_advance if its our first iteration
      if self.state.is_some() {
         self._inner_advance();
      }

      let valid = unsafe { splinterdb_sys::splinterdb_iterator_valid(self._inner) } as i32;
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

impl SplinterDB
{
   // Create a new SplinterDB object. This is uninitialized.
   pub fn new<T: rust_cfg::SdbRustDataFuncs>() -> SplinterDB
   {
      SplinterDB {
         _inner: std::ptr::null_mut(),
         data_cfg: new_sdb_data_config::<T>(0),
      }
   }

   fn db_create_or_open<P: AsRef<Path>>(
      &mut self,
      path: &P,
      cfg: &DBConfig,
      open_existing: bool,
   ) -> Result<()> {
      let path = path_as_cstring(path); // don't drop until init is done

      // set up the splinterdb config
      let mut sdb_cfg: splinterdb_sys::splinterdb_config = unsafe { std::mem::zeroed() };
      sdb_cfg.filename = path.as_ptr();
      sdb_cfg.cache_size = cfg.cache_size_bytes as u64;
      sdb_cfg.disk_size = cfg.disk_size_bytes as u64;
      sdb_cfg.data_cfg = &mut self.data_cfg;

      // set key bytes
      self.data_cfg.max_key_size = cfg.max_key_size as u64;

      // Open or create the database
      let rc = if open_existing {
         unsafe { splinterdb_sys::splinterdb_open(&sdb_cfg, &mut self._inner) }
      } else {
         unsafe { splinterdb_sys::splinterdb_create(&sdb_cfg, &mut self._inner) }
      };
      as_result(rc)
   }

   pub fn db_create<P: AsRef<Path>>(&mut self, path: &P, cfg: &DBConfig) -> Result<()>
   {
      self.db_create_or_open(path, cfg, false)
   }

   pub fn db_open<P: AsRef<Path>>(&mut self, path: &P, cfg: &DBConfig) -> Result<()>
   {
      self.db_create_or_open(path, cfg, true)
   }

   pub fn create_column_family(&mut self, max_key_size: u64) -> SplinterColumnFamily
   {
      splinter_cf::create_column_family(self._inner, max_key_size, &mut self.data_cfg)
   }

   pub fn register_thread(&self)
   {
      unsafe { splinterdb_sys::splinterdb_register_thread(self._inner) };
   }

   pub fn deregister_thread(&self)
   {
      unsafe { splinterdb_sys::splinterdb_deregister_thread(self._inner) };
   }

   pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()>
   {
      let key_slice: splinterdb_sys::slice = create_splinter_slice(key);
      let val_slice: splinterdb_sys::slice = create_splinter_slice(value);

      let rc = unsafe {
         splinterdb_sys::splinterdb_insert(
            self._inner,
            key_slice,
            val_slice,
         )
      };
      as_result(rc)
   }

   pub fn update(&self, key: &[u8], delta: &[u8]) -> Result<()>
   {
      let key_slice: splinterdb_sys::slice = create_splinter_slice(key);
      let delta_slice: splinterdb_sys::slice = create_splinter_slice(delta);

      let rc = unsafe {
         splinterdb_sys::splinterdb_update(
            self._inner,
            key_slice,
            delta_slice,
         )
      };
      as_result(rc)
   }

   pub fn delete(&self, key: &[u8]) -> Result<()>
   {
      let rc = unsafe {
         splinterdb_sys::splinterdb_delete(
            self._inner,
            create_splinter_slice(key),
         )
      };
      as_result(rc)
   }

   pub fn lookup(&self, key: &[u8]) -> Result<LookupResult>
   {
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
            create_splinter_slice(key),
            &mut lr,
         );
         as_result(rc)?;

         let found = splinterdb_sys::splinterdb_lookup_found(&lr) as i32;
         if found == 0 {
            return Ok(LookupResult::NotFound);
         }

         let mut val: splinterdb_sys::slice = splinterdb_sys::slice{
            length: 0,
            data: std::mem::zeroed(),
         };
         let rc = splinterdb_sys::splinterdb_lookup_result_value(
            &lr,
            &mut val,
         );
         as_result(rc)?;

         // TODO: Can we avoid this memory init and copy?
         let mut value: Vec<u8> = vec![0; val.length as usize];
         std::ptr::copy(
            val.data,
            std::mem::transmute(value.as_mut_ptr()),
            val.length as usize,
         );
         Ok(LookupResult::Found(value))
      }
   }

   pub fn range(&self, start_key: Option<&[u8]>) -> Result<RangeIterator>
   {
      let mut cf_iter: *mut splinterdb_sys::splinterdb_iterator = std::ptr::null_mut();

      let rc = unsafe {
         let start_slice: splinterdb_sys::slice = match start_key {
            Some(s) => splinterdb_sys::slice {
               length: s.len() as u64,
               data: ::std::mem::transmute(s.as_ptr()),
            },
            None => splinterdb_sys::slice {
               length: 0,
               data: ::std::ptr::null(),
            },
         };
         splinterdb_sys::splinterdb_iterator_init(
            self._inner,
            &mut cf_iter,
            start_slice,
         )
      };
      as_result(rc)?;
      Ok(RangeIterator::new(cf_iter.iter))
   }
}

mod tests;
