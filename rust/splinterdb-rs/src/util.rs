use std::io::{Error, Result};
use std::path::Path;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
pub struct DBConfig {
   pub cache_size_bytes: usize,
   pub disk_size_bytes: usize,
   pub max_key_size: usize,
   pub max_value_size: usize,
}

#[derive(Debug, PartialEq)]
pub enum LookupResult {
   Found(Vec<u8>),
   FoundTruncated(Vec<u8>),
   NotFound,
}

pub fn as_result(rc: ::std::os::raw::c_int) -> Result<()> {
   if rc != 0 {
      Err(Error::from_raw_os_error(rc))
   } else {
      Ok(())
   }
}

pub fn create_splinter_slice(ref v: &[u8]) -> splinterdb_sys::slice {
   unsafe {
      splinterdb_sys::slice {
         length: v.len() as u64,
         data: ::std::mem::transmute(v.as_ptr()),
      }
   }
}

#[derive(Debug)]
pub struct IteratorResult<'a> {
   pub key: &'a [u8],
   pub value: &'a [u8],
}

pub fn path_as_cstring<P: AsRef<Path>>(path: P) -> std::ffi::CString {
   let as_os_str = path.as_ref().as_os_str();
   let as_str = as_os_str.to_str().unwrap();
   std::ffi::CString::new(as_str).unwrap()
}
