use std::io::Result;
use std::cmp::Ordering;
use splinterdb_sys::*;
use crate::create_splinter_slice;
use xxhash_rust::xxh32::xxh32;

#[derive(Debug)]
pub enum CompareResult {
    LESS,      // first less than second
    EQUAL,     // first and second equal
    GREATER,   // first greater than second
}

#[derive(Debug)]
pub enum SdbMessageType {
    INVALID,
    INSERT,
    UPDATE,
    DELETE,
    OTHER, // TODO: IS THIS POSSIBLE?
}

// Rust side representation of a splinterDB message
#[derive(Debug)]
pub struct SdbMessage {
    pub msg_type: SdbMessageType,
    pub data: Vec<u8>,
}

fn sdb_slice_to_vec(s: &slice) -> Vec<u8>
{
    unsafe {
        std::slice::from_raw_parts(s.data as *const u8, s.length as usize).to_vec()
    }
}

fn raw_to_vec(data: *const ::std::os::raw::c_void, length: usize) -> Vec<u8>
{
    unsafe {
        std::slice::from_raw_parts(data as *const u8, length as usize).to_vec()
    }
}

fn int_to_msg_type(i: ::std::os::raw::c_uint) -> SdbMessageType
{
    match i {
        0 => SdbMessageType::INVALID,
        1 => SdbMessageType::INSERT,
        2 => SdbMessageType::UPDATE,
        3 => SdbMessageType::DELETE,
        _ => SdbMessageType::OTHER,
    }
}

fn create_sdb_message(msg: &message) -> SdbMessage
{
    SdbMessage {
        msg_type: int_to_msg_type(msg.type_),
        data: sdb_slice_to_vec(&msg.data),
    }
}

fn sdb_msg_from_acc(ma: &merge_accumulator) -> SdbMessage
{
    unsafe {
        SdbMessage {
            msg_type: int_to_msg_type(splinterdb_sys::merge_accumulator_message_class(ma)),
            data: sdb_slice_to_vec(&splinterdb_sys::merge_accumulator_to_slice(ma)),
        }
    }
}

// Trait defining the rust callbacks for SplinterDB's data_config
// By default we do not implement merge functionality
pub trait SdbRustDataFuncs {
    // Compare two keys, returning if key1 is less than/equal/greater than key2
    fn key_comp(key1: &[u8], key2: &[u8]) -> Ordering
    {
        key1.cmp(&key2)
    }
    // Return the hash of key, seeding the hash with seed
    fn key_hash(key: &[u8], seed: u32) -> u32
    {
        xxh32(key, seed)
    }

    // Combine two splinterDB messages into one given that
    // 1. new_msg is of type UPDATE
    // 2. old_msg is of type INSERT or UPDATE
    // The returned message may be either an update or an insert
    fn merge(_key: &[u8], _old_msg: SdbMessage, new_msg: SdbMessage) -> Result<SdbMessage>
    {
        Ok(new_msg)
    }

    // Resolve an update message when there is no older record to apply the update to.
    // Must return either an INSERT or DELETE type message
    fn merge_final(_key: &[u8], oldest_msg: SdbMessage) -> Result<SdbMessage>
    {
        Ok(oldest_msg)
    }

    // Convert a key to a string
    fn str_key(key: &[u8], dst: &mut [u8]) -> ()
    {
        // 2 characters per byte
        if 2 * key.len() > dst.len() as usize {
            panic!("Key too long to convert to string!");
        }
        let hex_str: String = hex::encode(key);
        for (i, c) in hex_str.chars().enumerate() {
            dst[i] = c as u8;
        }
    }

    // Convert a SplinterDB message to a string
    fn str_msg(msg: SdbMessage, dst: &mut [u8]) -> ()
    {
        if 2 * msg.data.len() > dst.len() as usize {
            panic!("Msg too long to convert to string!");
        }
        let hex_str: String = hex::encode(msg.data);
        for (i, c) in hex_str.chars().enumerate() {
            dst[i] = c as u8;
        }
    }
}

pub fn new_sdb_data_config<T: SdbRustDataFuncs>(key_size: u64) -> data_config
{
    data_config {
        max_key_size: key_size,
        key_compare: Some(key_compare::<T>),
        key_hash: Some(key_hash::<T>),
        merge_tuples: Some(merge_tuples::<T>),
        merge_tuples_final: Some(merge_tuples_final::<T>),
        key_to_string: Some(key_to_string::<T>),
        message_to_string: Some(message_to_string::<T>),
    }
}

// Implement all the default data functions
pub struct DefaultSdb {}
impl SdbRustDataFuncs for DefaultSdb {}

// These functions are templatized by the SdbRustDataFuncs a structure specified by the 
// user that implements the key functions in rust.
//
// These functions act as a wrapper for the data config functions converting from
// the SplinterDB C API to a rust friendly API.
pub extern "C" fn key_compare<T: SdbRustDataFuncs>(
    _cfg: *const data_config,
    key1: slice,
    key2: slice,
) -> ::std::os::raw::c_int
{
    let res: Ordering = T::key_comp(&sdb_slice_to_vec(&key1), &sdb_slice_to_vec(&key2));
    match res {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

pub extern "C" fn key_hash<T: SdbRustDataFuncs>(
    input: *const ::std::os::raw::c_void,
    length: usize,
    seed: uint32,
) -> uint32
{
    T::key_hash(&raw_to_vec(input, length), seed)
}

pub extern "C" fn merge_tuples<T: SdbRustDataFuncs>(
    _cfg: *const data_config,
    key: slice,
    old_message: message,
    new_message: *mut merge_accumulator,
) -> ::std::os::raw::c_int
{
    // convert the merge_accumulator to a message
    let new_msg: SdbMessage = unsafe {
        sdb_msg_from_acc(&*new_message)
    };

    // pass the old message and new message to user's merge() function
    let res: SdbMessage = match T::merge(
        &sdb_slice_to_vec(&key), 
        create_sdb_message(&old_message), 
        new_msg
    ) 
    {
        Ok(r) => r,
        Err(..) => return -1,
    };

    // update the merge_accumulator with the results of the user's func
    unsafe {
        splinterdb_sys::merge_accumulator_copy_message(
            new_message, 
            message {
                type_: res.msg_type as ::std::os::raw::c_uint,
                data: create_splinter_slice(&res.data),
            }
        );
    }

    return 0;
}

pub extern "C" fn merge_tuples_final<T: SdbRustDataFuncs>(
    _cfg: *const data_config,
    key: slice,
    oldest_message: *mut merge_accumulator,
) -> ::std::os::raw::c_int
{
    // convert the accumulator to a message
    let new_msg: SdbMessage = unsafe {
        sdb_msg_from_acc(&*oldest_message)
    };

    // call user's merge_final() function
    let res: SdbMessage = match T::merge_final(
        &sdb_slice_to_vec(&key),
        new_msg,
    ) 
    {
        Ok(r) => r,
        Err(..) => return -1,
    };

    // update the merge_accumulator with results of merge_final()
    unsafe {
        splinterdb_sys::merge_accumulator_copy_message(
            oldest_message, 
            message {
                type_: res.msg_type as ::std::os::raw::c_uint,
                data: create_splinter_slice(&res.data),
            }
        );
    }

    return 0;
}

pub extern "C" fn key_to_string<T: SdbRustDataFuncs>(
    _cfg: *const data_config,
    key: slice,
    str_: *mut ::std::os::raw::c_char,
    max_len: uint64,
) -> ()
{
    T::str_key(
        &sdb_slice_to_vec(&key), 
        &mut raw_to_vec(str_ as *const ::std::os::raw::c_void, max_len as usize)
    );
}

pub extern "C" fn message_to_string<T: SdbRustDataFuncs>(
    _cfg: *const data_config,
    msg: message,
    str_: *mut ::std::os::raw::c_char,
    max_len: uint64,
) -> ()
{
    T::str_msg(
        create_sdb_message(&msg), 
        &mut raw_to_vec(str_ as *const ::std::os::raw::c_void, max_len as usize)
    );
}
