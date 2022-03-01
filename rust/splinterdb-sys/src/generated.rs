/* automatically generated by rust-bindgen 0.59.2 */

pub const MAX_KEY_SIZE: u32 = 24;
pub const MAX_MESSAGE_SIZE: u32 = 128;
pub const SPLINTERDB_MAX_KEY_SIZE: u32 = 23;
pub type __int32_t = ::std::os::raw::c_int;
pub type __uint32_t = ::std::os::raw::c_uint;
pub type __uint64_t = ::std::os::raw::c_ulong;
pub type int32 = i32;
pub type uint32 = u32;
pub type uint64 = u64;
pub type bool_ = int32;
#[repr(C)]
#[derive(Debug)]
pub struct writable_buffer {
    _unused: [u8; 0],
}
pub const message_type_MESSAGE_TYPE_INSERT: message_type = 0;
pub const message_type_MESSAGE_TYPE_UPDATE: message_type = 1;
pub const message_type_MESSAGE_TYPE_DELETE: message_type = 2;
pub const message_type_MESSAGE_TYPE_INVALID: message_type = 3;
pub type message_type = ::std::os::raw::c_uint;
pub type key_compare_fn = ::std::option::Option<
    unsafe extern "C" fn(
        cfg: *const data_config,
        key1_len: uint64,
        key1: *const ::std::os::raw::c_void,
        key2_len: uint64,
        key2: *const ::std::os::raw::c_void,
    ) -> ::std::os::raw::c_int,
>;
pub type key_hash_fn = ::std::option::Option<
    unsafe extern "C" fn(
        input: *const ::std::os::raw::c_void,
        length: usize,
        seed: uint32,
    ) -> uint32,
>;
pub type message_class_fn = ::std::option::Option<
    unsafe extern "C" fn(
        cfg: *const data_config,
        raw_message_len: uint64,
        raw_message: *const ::std::os::raw::c_void,
    ) -> message_type,
>;
pub type merge_tuple_fn = ::std::option::Option<
    unsafe extern "C" fn(
        cfg: *const data_config,
        key_len: uint64,
        key: *const ::std::os::raw::c_void,
        old_message_len: uint64,
        old_message: *const ::std::os::raw::c_void,
        new_message: *mut writable_buffer,
    ) -> ::std::os::raw::c_int,
>;
pub type merge_tuple_final_fn = ::std::option::Option<
    unsafe extern "C" fn(
        cfg: *const data_config,
        key_len: uint64,
        key: *const ::std::os::raw::c_void,
        oldest_message: *mut writable_buffer,
    ) -> ::std::os::raw::c_int,
>;
pub type key_or_message_to_str_fn = ::std::option::Option<
    unsafe extern "C" fn(
        cfg: *const data_config,
        key_or_message_len: uint64,
        key_or_message: *const ::std::os::raw::c_void,
        str_: *mut ::std::os::raw::c_char,
        max_len: usize,
    ),
>;
pub type encode_message_fn = ::std::option::Option<
    unsafe extern "C" fn(
        type_: message_type,
        value_len: usize,
        value: *const ::std::os::raw::c_void,
        dst_msg_buffer_len: usize,
        dst_msg_buffer: *mut ::std::os::raw::c_void,
        out_encoded_len: *mut usize,
    ) -> ::std::os::raw::c_int,
>;
pub type decode_message_fn = ::std::option::Option<
    unsafe extern "C" fn(
        msg_buffer_len: usize,
        msg_buffer: *const ::std::os::raw::c_void,
        out_value_len: *mut usize,
        out_value: *mut *const ::std::os::raw::c_char,
    ) -> ::std::os::raw::c_int,
>;
#[repr(C)]
#[derive(Debug)]
pub struct data_config {
    pub key_size: uint64,
    pub message_size: uint64,
    pub min_key: [::std::os::raw::c_char; 24usize],
    pub min_key_length: usize,
    pub max_key: [::std::os::raw::c_char; 24usize],
    pub max_key_length: usize,
    pub key_compare: key_compare_fn,
    pub key_hash: key_hash_fn,
    pub message_class: message_class_fn,
    pub merge_tuples: merge_tuple_fn,
    pub merge_tuples_final: merge_tuple_final_fn,
    pub key_to_string: key_or_message_to_str_fn,
    pub message_to_string: key_or_message_to_str_fn,
    pub encode_message: encode_message_fn,
    pub decode_message: decode_message_fn,
    pub context: *mut ::std::os::raw::c_void,
}
#[test]
fn bindgen_test_layout_data_config() {
    assert_eq!(
        ::std::mem::size_of::<data_config>(),
        160usize,
        concat!("Size of: ", stringify!(data_config))
    );
    assert_eq!(
        ::std::mem::align_of::<data_config>(),
        8usize,
        concat!("Alignment of ", stringify!(data_config))
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).key_size as *const _ as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(key_size)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).message_size as *const _ as usize },
        8usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(message_size)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).min_key as *const _ as usize },
        16usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(min_key)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).min_key_length as *const _ as usize },
        40usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(min_key_length)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).max_key as *const _ as usize },
        48usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(max_key)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).max_key_length as *const _ as usize },
        72usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(max_key_length)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).key_compare as *const _ as usize },
        80usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(key_compare)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).key_hash as *const _ as usize },
        88usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(key_hash)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).message_class as *const _ as usize },
        96usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(message_class)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).merge_tuples as *const _ as usize },
        104usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(merge_tuples)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).merge_tuples_final as *const _ as usize },
        112usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(merge_tuples_final)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).key_to_string as *const _ as usize },
        120usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(key_to_string)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).message_to_string as *const _ as usize },
        128usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(message_to_string)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).encode_message as *const _ as usize },
        136usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(encode_message)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).decode_message as *const _ as usize },
        144usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(decode_message)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<data_config>())).context as *const _ as usize },
        152usize,
        concat!(
            "Offset of field: ",
            stringify!(data_config),
            "::",
            stringify!(context)
        )
    );
}
extern "C" {
    pub fn splinterdb_get_version() -> *const ::std::os::raw::c_char;
}
#[repr(C)]
#[derive(Debug)]
pub struct splinterdb_config {
    pub filename: *const ::std::os::raw::c_char,
    pub cache_size: uint64,
    pub disk_size: uint64,
    pub data_cfg: data_config,
    pub heap_handle: *mut ::std::os::raw::c_void,
    pub heap_id: *mut ::std::os::raw::c_void,
    pub page_size: uint64,
    pub extent_size: uint64,
    pub io_flags: ::std::os::raw::c_int,
    pub io_perms: uint32,
    pub io_async_queue_depth: uint64,
    pub cache_use_stats: bool_,
    pub cache_logfile: *const ::std::os::raw::c_char,
    pub btree_rough_count_height: uint64,
    pub filter_remainder_size: uint64,
    pub filter_index_size: uint64,
    pub use_log: bool_,
    pub memtable_capacity: uint64,
    pub fanout: uint64,
    pub max_branches_per_node: uint64,
    pub use_stats: uint64,
    pub reclaim_threshold: uint64,
}
#[test]
fn bindgen_test_layout_splinterdb_config() {
    assert_eq!(
        ::std::mem::size_of::<splinterdb_config>(),
        320usize,
        concat!("Size of: ", stringify!(splinterdb_config))
    );
    assert_eq!(
        ::std::mem::align_of::<splinterdb_config>(),
        8usize,
        concat!("Alignment of ", stringify!(splinterdb_config))
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<splinterdb_config>())).filename as *const _ as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(filename)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<splinterdb_config>())).cache_size as *const _ as usize },
        8usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(cache_size)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<splinterdb_config>())).disk_size as *const _ as usize },
        16usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(disk_size)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<splinterdb_config>())).data_cfg as *const _ as usize },
        24usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(data_cfg)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<splinterdb_config>())).heap_handle as *const _ as usize },
        184usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(heap_handle)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<splinterdb_config>())).heap_id as *const _ as usize },
        192usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(heap_id)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<splinterdb_config>())).page_size as *const _ as usize },
        200usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(page_size)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<splinterdb_config>())).extent_size as *const _ as usize },
        208usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(extent_size)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<splinterdb_config>())).io_flags as *const _ as usize },
        216usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(io_flags)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<splinterdb_config>())).io_perms as *const _ as usize },
        220usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(io_perms)
        )
    );
    assert_eq!(
        unsafe {
            &(*(::std::ptr::null::<splinterdb_config>())).io_async_queue_depth as *const _ as usize
        },
        224usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(io_async_queue_depth)
        )
    );
    assert_eq!(
        unsafe {
            &(*(::std::ptr::null::<splinterdb_config>())).cache_use_stats as *const _ as usize
        },
        232usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(cache_use_stats)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<splinterdb_config>())).cache_logfile as *const _ as usize },
        240usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(cache_logfile)
        )
    );
    assert_eq!(
        unsafe {
            &(*(::std::ptr::null::<splinterdb_config>())).btree_rough_count_height as *const _
                as usize
        },
        248usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(btree_rough_count_height)
        )
    );
    assert_eq!(
        unsafe {
            &(*(::std::ptr::null::<splinterdb_config>())).filter_remainder_size as *const _ as usize
        },
        256usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(filter_remainder_size)
        )
    );
    assert_eq!(
        unsafe {
            &(*(::std::ptr::null::<splinterdb_config>())).filter_index_size as *const _ as usize
        },
        264usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(filter_index_size)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<splinterdb_config>())).use_log as *const _ as usize },
        272usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(use_log)
        )
    );
    assert_eq!(
        unsafe {
            &(*(::std::ptr::null::<splinterdb_config>())).memtable_capacity as *const _ as usize
        },
        280usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(memtable_capacity)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<splinterdb_config>())).fanout as *const _ as usize },
        288usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(fanout)
        )
    );
    assert_eq!(
        unsafe {
            &(*(::std::ptr::null::<splinterdb_config>())).max_branches_per_node as *const _ as usize
        },
        296usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(max_branches_per_node)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<splinterdb_config>())).use_stats as *const _ as usize },
        304usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(use_stats)
        )
    );
    assert_eq!(
        unsafe {
            &(*(::std::ptr::null::<splinterdb_config>())).reclaim_threshold as *const _ as usize
        },
        312usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_config),
            "::",
            stringify!(reclaim_threshold)
        )
    );
}
#[repr(C)]
#[derive(Debug)]
pub struct splinterdb {
    _unused: [u8; 0],
}
extern "C" {
    pub fn splinterdb_create(
        cfg: *const splinterdb_config,
        kvs: *mut *mut splinterdb,
    ) -> ::std::os::raw::c_int;
}
extern "C" {
    pub fn splinterdb_open(
        cfg: *const splinterdb_config,
        kvs: *mut *mut splinterdb,
    ) -> ::std::os::raw::c_int;
}
extern "C" {
    pub fn splinterdb_close(kvs: *mut splinterdb);
}
extern "C" {
    pub fn splinterdb_register_thread(kvs: *mut splinterdb);
}
extern "C" {
    pub fn splinterdb_deregister_thread(kvs: *mut splinterdb);
}
extern "C" {
    pub fn splinterdb_insert(
        kvsb: *const splinterdb,
        key_len: usize,
        key: *const ::std::os::raw::c_char,
        val_len: usize,
        value: *const ::std::os::raw::c_char,
    ) -> ::std::os::raw::c_int;
}
extern "C" {
    pub fn splinterdb_insert_raw_message(
        kvs: *const splinterdb,
        key_length: usize,
        key: *const ::std::os::raw::c_char,
        raw_message_length: usize,
        raw_message: *const ::std::os::raw::c_char,
    ) -> ::std::os::raw::c_int;
}
extern "C" {
    pub fn splinterdb_delete(
        kvsb: *const splinterdb,
        key_len: usize,
        key: *const ::std::os::raw::c_char,
    ) -> ::std::os::raw::c_int;
}
#[repr(C)]
#[derive(Debug)]
pub struct splinterdb_lookup_result {
    pub opaque: [::std::os::raw::c_char; 48usize],
}
#[test]
fn bindgen_test_layout_splinterdb_lookup_result() {
    assert_eq!(
        ::std::mem::size_of::<splinterdb_lookup_result>(),
        48usize,
        concat!("Size of: ", stringify!(splinterdb_lookup_result))
    );
    assert_eq!(
        ::std::mem::align_of::<splinterdb_lookup_result>(),
        1usize,
        concat!("Alignment of ", stringify!(splinterdb_lookup_result))
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<splinterdb_lookup_result>())).opaque as *const _ as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(splinterdb_lookup_result),
            "::",
            stringify!(opaque)
        )
    );
}
extern "C" {
    pub fn splinterdb_lookup_result_init(
        kvs: *const splinterdb,
        result: *mut splinterdb_lookup_result,
        buffer_len: usize,
        buffer: *mut ::std::os::raw::c_char,
    );
}
extern "C" {
    pub fn splinterdb_lookup_result_deinit(result: *mut splinterdb_lookup_result);
}
extern "C" {
    pub fn splinterdb_lookup_result_parse(
        kvs: *const splinterdb,
        result: *const splinterdb_lookup_result,
        found: *mut bool_,
        value_size: *mut usize,
        value: *mut *const ::std::os::raw::c_char,
    ) -> ::std::os::raw::c_int;
}
extern "C" {
    pub fn splinterdb_lookup(
        kvs: *const splinterdb,
        key_length: usize,
        key: *const ::std::os::raw::c_char,
        result: *mut splinterdb_lookup_result,
    ) -> ::std::os::raw::c_int;
}
#[repr(C)]
#[derive(Debug)]
pub struct splinterdb_iterator {
    _unused: [u8; 0],
}
extern "C" {
    pub fn splinterdb_iterator_init(
        kvs: *const splinterdb,
        iter: *mut *mut splinterdb_iterator,
        start_key_length: usize,
        start_key: *const ::std::os::raw::c_char,
    ) -> ::std::os::raw::c_int;
}
extern "C" {
    pub fn splinterdb_iterator_deinit(iter: *mut splinterdb_iterator);
}
extern "C" {
    pub fn splinterdb_iterator_valid(iter: *mut splinterdb_iterator) -> bool_;
}
extern "C" {
    pub fn splinterdb_iterator_next(iter: *mut splinterdb_iterator);
}
extern "C" {
    pub fn splinterdb_iterator_get_current(
        iter: *mut splinterdb_iterator,
        key_len: *mut usize,
        key: *mut *const ::std::os::raw::c_char,
        val_len: *mut usize,
        value: *mut *const ::std::os::raw::c_char,
    );
}
extern "C" {
    pub fn splinterdb_iterator_status(iter: *const splinterdb_iterator) -> ::std::os::raw::c_int;
}
extern "C" {
    pub fn default_data_config_init(
        max_key_size: usize,
        max_value_size: usize,
        out_cfg: *mut data_config,
    );
}
