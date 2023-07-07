/*
 * column_family.h --
 *
 *     The Column Family public API for SplinterDB.
 *
 *
 */


#ifndef _SPLINTERDB_COLUMN_FAMILY_H_
#define _SPLINTERDB_COLUMN_FAMILY_H_

#include "splinterdb/splinterdb.h"
#include "splinterdb/public_platform.h"

// Size of stack buffer we allocate for column family keys.
// This can be fairly large because these buffers are short
// lived. If keys are relatively small then the conversion
// can be done upon the stack. We use writable buffers to
// support larger keys.
#define CF_KEY_DEFAULT_SIZE 512

typedef uint32 column_family_id;

typedef struct splinterdb_column_family {
   column_family_id id;
   splinterdb      *kvs;
} splinterdb_column_family;

typedef struct splinterdb_cf_iterator splinterdb_cf_iterator;

// Initialize the cf_data_config and give a pointer to it to the user.
// This pointer is then passed to SplinterDB to add support for
// column families. The memory for the column_family_config is managed
// by SplinterDB. Not the user.
void
column_family_config_init(const uint64  max_key_size, // IN
                          data_config **cf_cfg        // OUT
);

// Delete the cf_data_config, freeing all associated memory.
//
// This should only be called after closing the SplinterDB instance
void
column_family_config_deinit(data_config *cf_cfg);

// Create a new column family
// Returns a new column family struct
splinterdb_column_family
column_family_create(splinterdb  *kvs,
                     const uint64 max_key_size,
                     data_config *data_cfg);

// Delete the column family cf
// IMPORTANT: The user may NOT delete their data_config even after
//            calling this function. All data_configs must persist
//            for the lifetime of the SplinterDB instance
void
column_family_delete(splinterdb_column_family cf);

// ====== SPLINTERDB Functions ======
// We wrap these for column family support
// Column families and standard splinterdb should not be mixed
int
splinterdb_cf_insert(const splinterdb_column_family cf, slice key, slice value);

int
splinterdb_cf_delete(const splinterdb_column_family cf, slice key);

int
splinterdb_cf_update(const splinterdb_column_family cf, slice key, slice delta);

// column family lookups

void
splinterdb_cf_lookup_result_init(const splinterdb_column_family cf,    // IN
                                 splinterdb_lookup_result *result,     // IN/OUT
                                 uint64                    buffer_len, // IN
                                 char                     *buffer      // IN
);

void
splinterdb_cf_lookup_result_deinit(splinterdb_lookup_result *result); // IN

_Bool
splinterdb_cf_lookup_found(const splinterdb_lookup_result *); // IN

int
splinterdb_cf_lookup_result_value(const splinterdb_lookup_result *result, // IN
                                  slice                          *value   // OUT
);

int
splinterdb_cf_lookup(const splinterdb_column_family cf,    // IN
                     slice                          key,   // IN
                     splinterdb_lookup_result      *result // IN/OUT
);


// Range iterators for column families

int
splinterdb_cf_iterator_init(const splinterdb_column_family cf,       // IN
                            splinterdb_cf_iterator       **cf_iter,  // OUT
                            slice                          start_key // IN
);

void
splinterdb_cf_iterator_deinit(splinterdb_cf_iterator *cf_iter);

void
splinterdb_cf_iterator_next(splinterdb_cf_iterator *cf_iter);

_Bool
splinterdb_cf_iterator_valid(splinterdb_cf_iterator *cf_iter);

void
splinterdb_cf_iterator_get_current(splinterdb_cf_iterator *cf_iter, // IN
                                   slice                  *key,     // OUT
                                   slice                  *value    // OUT
);

int
splinterdb_cf_iterator_status(const splinterdb_cf_iterator *cf_iter);

#endif // _SPLINTERDB_COLUMN_FAMILY_H_
