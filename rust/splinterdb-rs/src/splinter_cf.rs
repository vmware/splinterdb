use std::io::{Result};
use std::collections::LinkedList;
use crate::*;

#[derive(Debug)]
pub struct SplinterDBWithColumnFamilies {
   _inner:    *mut splinterdb_sys::splinterdb,
   cf_cfg:    *mut splinterdb_sys::data_config,
   data_cfgs: LinkedList<splinterdb_sys::data_config>,
}

unsafe impl Sync for SplinterDBWithColumnFamilies {}
unsafe impl Send for SplinterDBWithColumnFamilies {}

#[derive(Debug)]
pub struct SplinterColumnFamily {
   _inner: splinterdb_sys::splinterdb_column_family,
}

unsafe impl Sync for SplinterColumnFamily {}
unsafe impl Send for SplinterColumnFamily {}

impl Drop for SplinterDBWithColumnFamilies {
   fn drop(&mut self) {
      unsafe {
         splinterdb_sys::splinterdb_close(&mut self._inner);
         splinterdb_sys::column_family_config_deinit(self.cf_cfg);
      }
   }
}

impl Drop for SplinterColumnFamily {
   fn drop(&mut self) {
      unsafe {
         splinterdb_sys::column_family_delete(self._inner);
      }
   }
}

impl SplinterDBWithColumnFamilies {
   pub fn new() -> SplinterDBWithColumnFamilies {
      SplinterDBWithColumnFamilies {
         _inner:    std::ptr::null_mut(),
         cf_cfg:    unsafe {std::mem::zeroed() },
         data_cfgs: LinkedList::new(),
      }
   }

   fn db_create_or_open<P: AsRef<Path>>(
      &mut self,
      path: &P,
      cfg: &DBConfig,
      open_existing: bool,
   ) -> Result<()> {
      let path = path_as_cstring(path); // don't drop until init is done

      // initialize the cf_data_config
      unsafe { splinterdb_sys::column_family_config_init(cfg.max_key_size as u64, &mut self.cf_cfg) };

      // set up the splinterdb config
      let mut sdb_cfg: splinterdb_sys::splinterdb_config = unsafe { std::mem::zeroed() };
      sdb_cfg.filename = path.as_ptr();
      sdb_cfg.cache_size = cfg.cache_size_bytes as u64;
      sdb_cfg.disk_size = cfg.disk_size_bytes as u64;
      sdb_cfg.data_cfg = self.cf_cfg;

      // Open or create the database
      let rc = if open_existing {
         unsafe { splinterdb_sys::splinterdb_open(&sdb_cfg, &mut self._inner) }
      } else {
         unsafe { splinterdb_sys::splinterdb_create(&sdb_cfg, &mut self._inner) }
      };
      as_result(rc)
   }

   pub fn db_create<P: AsRef<Path>>(&mut self, path: &P, cfg: &DBConfig) -> Result<()> {
      self.db_create_or_open(path, cfg, false)
   }

   pub fn db_open<P: AsRef<Path>>(&mut self, path: &P, cfg: &DBConfig) -> Result<()> {
      self.db_create_or_open(path, cfg, true)
   }

   pub fn register_thread(&self)
   {
      unsafe { splinterdb_sys::splinterdb_register_thread(self._inner) };
   }

   pub fn deregister_thread(&self)
   {
      unsafe { splinterdb_sys::splinterdb_deregister_thread(self._inner) };
   }

   pub fn column_family_create<T: rust_cfg::SdbRustDataFuncs>(&mut self, max_key_size: u64) -> SplinterColumnFamily
   {
      let data_cfg: *mut splinterdb_sys::data_config;
      // add this data config to our linked list of configs
      let new_cfg: splinterdb_sys::data_config = new_sdb_data_config::<T>(0);
      self.data_cfgs.push_back(new_cfg);
      data_cfg = match self.data_cfgs.back_mut() {
         None => panic!(),
         Some(x) => x,
      };

      let mut ret: SplinterColumnFamily = SplinterColumnFamily {
         _inner: unsafe { std::mem::zeroed() },
      };
      ret._inner = unsafe { 
         splinterdb_sys::column_family_create(self._inner, max_key_size, data_cfg)
      };
      ret
   }
}

impl SplinterColumnFamily {
   pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()>
   {
      let key_slice: splinterdb_sys::slice = create_splinter_slice(key);
      let val_slice: splinterdb_sys::slice = create_splinter_slice(value);

      let rc = unsafe {
         splinterdb_sys::splinterdb_cf_insert(
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
         splinterdb_sys::splinterdb_cf_update(
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
         splinterdb_sys::splinterdb_cf_delete(
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
         splinterdb_sys::splinterdb_cf_lookup_result_init(
            self._inner,
            &mut lr,
            0,
            std::ptr::null_mut(),
         );

         let rc = splinterdb_sys::splinterdb_cf_lookup(
            self._inner,
            create_splinter_slice(key),
            &mut lr,
         );
         as_result(rc)?;

         let found = splinterdb_sys::splinterdb_cf_lookup_found(&lr) as i32;
         if found == 0 {
            return Ok(LookupResult::NotFound);
         }

         let mut val: splinterdb_sys::slice = splinterdb_sys::slice{
            length: 0,
            data: std::mem::zeroed(),
         };
         let rc = splinterdb_sys::splinterdb_cf_lookup_result_value(
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

   pub fn range(&self, start_key: Option<&[u8]>) -> Result<SplinterCursor>
   {
      let mut cf_iter: *mut splinterdb_sys::splinterdb_cf_iterator = std::ptr::null_mut();


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
         splinterdb_sys::splinterdb_cf_iterator_init(
            self._inner,
            &mut cf_iter,
            start_slice,
         )
      };
      as_result(rc)?;
      SplinterCursor::new(cf_iter as *mut splinterdb_sys::splinterdb_iterator, true)
   }
}
