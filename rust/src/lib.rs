#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(dead_code)]
// allow some uses of u128 in extern blocks
#![allow(improper_ctypes)]
#![allow(clippy::upper_case_acronyms)]

use splinterdb::{btree_config, btree_hdr, btree_node, cache, page_type, TRUE};

/// cbindgen:ignore
mod splinterdb;

/// # Safety
///
/// Probably unsafe.
#[no_mangle]
pub unsafe extern "C" fn btree_node_get(
    cc: *mut cache,
    _cfg: *const btree_config,
    node: *mut btree_node,
    type_: page_type,
) {
    debug_assert!((*node).addr != 0);
    (*node).page = (*(*cc).ops).page_get.unwrap()(cc, (*node).addr, TRUE as i32, type_);
    (*node).hdr = (*(*node).page).data as *mut btree_hdr;
}
