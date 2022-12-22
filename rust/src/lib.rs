use libc;
use libc::c_void;
use splinterdb::{allocator, cache, cache_config, cache_config_ops, page_handle, page_type, platform_status , cache_ops };

/// cbindgen:ignore
mod splinterdb;

#[repr(C)]
pub struct stubcache_config {
    super_: cache_config,

    extent_size: u64,
    extent_mask: u64,
    disk_capacity_bytes: u64,
    page_size: u64,
    disk_capacity_pages: u64,
}

unsafe extern "C" fn stubcache_config_page_size_virtual(cfg: *const cache_config) -> u64 {
    let scfg: *const stubcache_config = cfg as *const stubcache_config;
    (*scfg).page_size
}

unsafe extern "C" fn stubcache_config_extent_size_virtual(cfg: *const cache_config) -> u64 {
    let scfg: *const stubcache_config = cfg as *const stubcache_config;
    (*scfg).extent_size
}

const stubcache_config_ops: cache_config_ops = cache_config_ops {
    page_size: Some(stubcache_config_page_size_virtual),
    extent_size: Some(stubcache_config_extent_size_virtual),
};

#[no_mangle]
pub unsafe extern "C" fn stubcache_config_init(
    cfg: &mut stubcache_config,
    page_size: u64,
    extent_size: u64,
    al: &mut allocator,
) {
   cfg.super_.ops = &stubcache_config_ops;
   //let ops = &(*(al.ops));
   cfg.disk_capacity_bytes = (*(al.ops)).get_capacity.unwrap()(al); 
       // kinda evil, but we can't link to static inlines in allocator.h,


   cfg.page_size = page_size;
   assert!(cfg.page_size == 4096);
   cfg.disk_capacity_pages = cfg.disk_capacity_bytes / cfg.page_size;

   cfg.extent_size = extent_size;
   let log_extent_size: u64   = 63u64 - (extent_size.leading_zeros() as u64);
   cfg.extent_mask   = !((1u64 << log_extent_size) - 1);

}

#[repr(C)]
pub struct stubcache {
    super_: cache,
    cfg: *mut stubcache_config,
    al: *mut allocator,
    page_handles: *mut page_handle,
    the_disk: *mut char,
}

/// Probably unsafe.
#[no_mangle]
pub unsafe extern "C" fn stubcache_page_alloc(
    sc: *mut stubcache,
    addr: u64,
    type_: page_type,
) -> *mut page_handle {
    return std::ptr::null_mut();
}

fn unimpl() {
    assert!(false);
}

const stubcache_ops: cache_ops = cache_ops {
   page_alloc : None,

   extent_hard_evict : None,
   page_get : None,
   page_get_async : None,
   page_async_done : None,
   page_unget : None,
   page_claim : None,
   page_unclaim : None,
   page_lock : None,
   page_unlock : None,
   page_prefetch : None,
   page_mark_dirty : None,
   page_pin : None,
   page_unpin : None,
   page_sync : None,
   extent_sync : None,
   flush : None,
   evict : None,
   cleanup : None,
   assert_ungot : None,
   assert_free : None,
   validate_page : None,
   cache_present : None,
   print : None,
   print_stats : None,
   io_stats : None,
   reset_stats : None,
   count_dirty : None,
   page_get_read_ref : None,
   enable_sync_get : None,
   get_allocator : None,
   get_config : None,
};

#[no_mangle]
pub unsafe extern "C" fn stubcache_init(
    sc: &mut stubcache,
    cfg: &mut stubcache_config,
     al: &mut allocator) -> platform_status
{
   sc.super_.ops = &stubcache_ops;
   sc.cfg = cfg;
   sc.al = al;

   // platform_heap_id hid_0 = 0;
   sc.page_handles = std::ptr::null_mut();
   //sc.page_handles = TYPED_ARRAY_MALLOC(hid_0, sc->page_handles, sc->cfg->disk_capacity_pages);
   sc.the_disk = libc::malloc(std::mem::size_of::<char>() * (*(sc.cfg)).disk_capacity_bytes as usize) as *mut char;

   let STATUS_OK = platform_status { r : 0 };
   return STATUS_OK;
}

#[no_mangle]
pub unsafe extern "C" fn stubcache_deinit(sc: &mut stubcache)
{
    libc::free(sc.the_disk as *mut c_void);
}

