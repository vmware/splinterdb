use libc;
use libc::c_void;
use splinterdb::{
    allocator, cache, cache_config, cache_config_ops, cache_ops, page_handle, page_type,
    platform_status, bool_
};

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
    let log_extent_size: u64 = 63u64 - (extent_size.leading_zeros() as u64);
    cfg.extent_mask = !((1u64 << log_extent_size) - 1);
}

#[repr(C)]
pub struct stubcache {
    super_: cache,
    cfg: *mut stubcache_config,
    al: *mut allocator,
    page_handles: Option<Vec<page_handle>>,    // Needed Box to mitigate "incomplete type" in cbindgen output, since Vec layout is unspecified.
    the_disk: *mut i8,
}

unsafe extern "C" fn page_alloc(
    cc: *mut cache,
    addr: u64,
    type_: page_type,
) -> *mut page_handle {
    let sc = cc as *mut stubcache;
    return &mut ((*sc).page_handles.as_mut().unwrap()[(addr / (*(*sc).cfg).page_size) as usize]);
}

unsafe extern "C" fn extent_hard_evict(cc: *mut cache, addr: u64, type_: page_type) {
}

unsafe extern "C" fn page_get(
    cc: *mut cache,
    addr: u64,
    blocking: bool_,
    type_: page_type,
) -> *mut page_handle {
    // TODO duplicate code with page_alloc.
    let sc = cc as *mut stubcache;
    return &mut ((*sc).page_handles.as_mut().unwrap()[(addr / (*(*sc).cfg).page_size) as usize]);
}

unsafe extern "C" fn page_unget(cc: *mut cache, page: *mut page_handle) {
}

unsafe extern "C" fn page_claim(cc: *mut cache, page: *mut page_handle) -> bool_ {
    true as bool_
}

unsafe extern "C" fn page_unclaim(cc: *mut cache, page: *mut page_handle) {
}

unsafe extern "C" fn page_lock(cc: *mut cache, page: *mut page_handle) {
}

unsafe extern "C" fn page_unlock(cc: *mut cache, page: *mut page_handle) {
}

unsafe extern "C" fn page_mark_dirty(cc: *mut cache, page: *mut page_handle) {
}

unsafe extern "C" fn page_pin(cc: *mut cache, page: *mut page_handle) {
}

unsafe extern "C" fn page_unpin(cc: *mut cache, page: *mut page_handle) {
}

unsafe extern "C" fn page_sync(cc: *mut cache, page: *mut page_handle, is_blocking: bool_, type_: page_type) {
}

unsafe extern "C" fn flush(cc: *mut cache) {
}

unsafe extern "C" fn validate_page(cc: *mut cache, page: *mut page_handle, addr: u64) {
}

unsafe extern "C" fn stubcache_get_allocator(
    cc: *const cache,
) -> *mut allocator {
    let sc = cc as *mut stubcache;
    return (*sc).al;
}

unsafe extern "C" fn stubcache_get_config(
    cc: *const cache,
) -> *mut cache_config {
    let sc = cc as *mut stubcache;
    return (*sc).cfg as *mut cache_config;
}

fn unimpl() {
    assert!(false);
}

const stubcache_ops: cache_ops = cache_ops {
    page_alloc: Some(page_alloc),

    extent_hard_evict: Some(extent_hard_evict),
    page_get: Some(page_get),
    page_get_async: None,
    page_async_done: None,
    page_unget: Some(page_unget),
    page_claim: Some(page_claim),
    page_unclaim: Some(page_unclaim),
    page_lock: Some(page_lock),
    page_unlock: Some(page_unlock),
    page_prefetch: None,
    page_mark_dirty: Some(page_mark_dirty),
    page_pin: Some(page_pin),
    page_unpin: Some(page_unpin),
    page_sync: Some(page_sync),
    extent_sync: None,
    flush: Some(flush),
    evict: None,
    cleanup: None,
    assert_ungot: None,
    assert_free: None,
    validate_page: Some(validate_page),
    cache_present: None,
    print: None,
    print_stats: None,
    io_stats: None,
    reset_stats: None,
    count_dirty: None,
    page_get_read_ref: None,
    enable_sync_get: None,
    get_allocator: Some(stubcache_get_allocator),
    get_config: Some(stubcache_get_config),
};

#[no_mangle]
pub unsafe extern "C" fn stubcache_init(
    sc: &mut stubcache,
    cfg: &mut stubcache_config,
    al: &mut allocator,
) -> platform_status {
    sc.super_.ops = &stubcache_ops;
    sc.cfg = cfg;
    sc.al = al;

    sc.page_handles = Some(Vec::with_capacity(cfg.disk_capacity_pages as usize));
    sc.the_disk =
        libc::malloc(std::mem::size_of::<char>() * (*(sc.cfg)).disk_capacity_bytes as usize)
            as *mut i8;

    for i in 0..cfg.disk_capacity_pages {
        let addr = i * cfg.page_size;
        sc.page_handles.as_mut().unwrap().push(page_handle {
            data: sc.the_disk.add(addr as usize),
            disk_addr: addr,
        });
    }

    let STATUS_OK = platform_status { r: 0 };
    return STATUS_OK;
}

#[no_mangle]
pub unsafe extern "C" fn stubcache_deinit(sc: &mut stubcache) {
    libc::free(sc.the_disk as *mut c_void);
}
