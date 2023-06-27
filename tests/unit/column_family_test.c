/*
 * -----------------------------------------------------------------------------
 * splinter_cf_test.c --
 *
 *     Quick test of the Column Family public API for SplinterDB
 *
 * -----------------------------------------------------------------------------
 */

#include "splinterdb/column_family.h"
#include "splinterdb/data.h"
#include "splinterdb/public_platform.h"
#include "splinterdb/default_data_config.h"
#include "unit_tests.h"
#include "util.h"
#include "test_data.h"
#include "ctest.h" // This is required for all test-case files.
#include "btree.h" // for MAX_INLINE_MESSAGE_SIZE

#define TEST_MAX_KEY_SIZE   16
#define TEST_MAX_VALUE_SIZE 32

// Hard-coded format strings to generate key and values
// static const char key_fmt[] = "key-%04x";
// static const char val_fmt[] = "val-%04x";
// #define KEY_FMT_LENGTH (8)
// #define VAL_FMT_LENGTH (8)


CTEST_DATA(column_family)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;

   // the global_data_cfg is used to route to the right data_config
   // for each column family
   cf_data_config global_data_cfg;

   // default data config for when we don't want to be special
   data_config default_data_cfg;
};

CTEST_SETUP(column_family)
{
   default_data_config_init(TEST_MAX_KEY_SIZE, &data->default_data_cfg);
   column_family_config_init(TEST_MAX_KEY_SIZE, &data->global_data_cfg);
   data->cfg =
      (splinterdb_config){.filename   = TEST_DB_NAME,
                          .cache_size = 64 * Mega,
                          .disk_size  = 128 * Mega,
                          .data_cfg   = (data_config *)&data->global_data_cfg};

   int rc = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(TEST_MAX_VALUE_SIZE
               < MAX_INLINE_MESSAGE_SIZE(LAIO_DEFAULT_PAGE_SIZE));
}

CTEST_TEARDOWN(column_family)
{
   if (data->kvsb) {
      splinterdb_close(&data->kvsb);
   }
   column_family_config_deinit(&data->global_data_cfg);
}

/*
 *
 * Basic test case that ensures we can create and use a single column family
 * correctly Tests:
 *  - column_family_create()
 *  - column_family_delete()
 *  - splinterdb_cf_insert()
 *  - splinterdb_cf_delete()
 *  - splinterdb_cf_lookup()
 *
 * We evaluate that these functions perform as expected and provide the correct
 * outputs
 */
CTEST2(column_family, test_single_column)
{
   // create a column family
   splinterdb_column_family cf = column_family_create(
      data->kvsb, TEST_MAX_KEY_SIZE, &data->default_data_cfg);

   // create some basic data to insert and lookup
   char  *key_data = "some-key";
   size_t key_len  = strlen(key_data);
   slice  user_key = slice_create(key_len, key_data);

   splinterdb_lookup_result result;
   splinterdb_cf_lookup_result_init(cf, &result, 0, NULL);

   int rc = splinterdb_cf_lookup(cf, user_key, &result);
   ASSERT_EQUAL(0, rc);

   // Lookup of a non-existent key should return not-found.
   ASSERT_FALSE(splinterdb_cf_lookup_found(&result));

   static char *to_insert_data = "some-value";
   size_t       to_insert_len  = strlen(to_insert_data);
   slice        to_insert      = slice_create(to_insert_len, to_insert_data);

   // Basic insert of new key should succeed.
   rc = splinterdb_cf_insert(cf, user_key, to_insert);
   ASSERT_EQUAL(0, rc);

   // Lookup of inserted key should succeed.
   rc = splinterdb_cf_lookup(cf, user_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_cf_lookup_found(&result));

   // Lookup should return inserted value
   slice value;
   rc = splinterdb_cf_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(to_insert_len, slice_length(value));
   ASSERT_STREQN(to_insert_data, slice_data(value), slice_length(value));

   // Delete key
   rc = splinterdb_cf_delete(cf, user_key);
   ASSERT_EQUAL(0, rc);

   // Deleted key should not be found
   rc = splinterdb_cf_lookup(cf, user_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(splinterdb_cf_lookup_found(&result));

   splinterdb_cf_lookup_result_deinit(&result);
   column_family_delete(cf);
}

/*
 * Ensure keys and values of maximum length work
 * with column families
 */
CTEST2(column_family, test_max_length)
{
   // create a column family
   splinterdb_column_family cf = column_family_create(
      data->kvsb, TEST_MAX_KEY_SIZE, &data->default_data_cfg);

   char   large_key_data[TEST_MAX_KEY_SIZE];
   size_t large_key_len = TEST_MAX_KEY_SIZE;
   memset(large_key_data, 'k', TEST_MAX_KEY_SIZE);
   slice large_key = slice_create(large_key_len, large_key_data);

   char   large_val_data[TEST_MAX_VALUE_SIZE];
   size_t large_val_len = TEST_MAX_VALUE_SIZE;
   memset(large_val_data, 'v', TEST_MAX_VALUE_SIZE);
   slice large_val = slice_create(large_val_len, large_val_data);

   // Insert of large key and value should exceed
   int rc = splinterdb_cf_insert(cf, large_key, large_val);
   ASSERT_EQUAL(0, rc);

   splinterdb_lookup_result result;
   splinterdb_cf_lookup_result_init(cf, &result, 0, NULL);

   // Lookup of inserted key should succeed.
   rc = splinterdb_cf_lookup(cf, large_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_cf_lookup_found(&result));

   // Lookup should return inserted value
   slice value;
   rc = splinterdb_cf_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(large_val_len, slice_length(value));
   ASSERT_STREQN(large_val_data, slice_data(value), slice_length(value));

   // Delete key
   rc = splinterdb_cf_delete(cf, large_key);
   ASSERT_EQUAL(0, rc);

   // Deleted key should not be found
   rc = splinterdb_cf_lookup(cf, large_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(splinterdb_cf_lookup_found(&result));

   splinterdb_cf_lookup_result_deinit(&result);
}

/*
 * Test key/value operations upon multiple column families.
 * Ensure that the keys can be operated upon independently.
 */
CTEST2(column_family, test_multiple_cf_same_key)
{
   // create a few column families
   splinterdb_column_family cf0 = column_family_create(
      data->kvsb, TEST_MAX_KEY_SIZE, &data->default_data_cfg);
   splinterdb_column_family cf1 = column_family_create(
      data->kvsb, TEST_MAX_KEY_SIZE, &data->default_data_cfg);
   splinterdb_column_family cf2 = column_family_create(
      data->kvsb, TEST_MAX_KEY_SIZE, &data->default_data_cfg);
   splinterdb_column_family cf3 = column_family_create(
      data->kvsb, TEST_MAX_KEY_SIZE, &data->default_data_cfg);

   // Insert a single key to each column family
   char  key_data[]  = "key";
   char  val0_data[] = "val0";
   char  val1_data[] = "val1";
   char  val2_data[] = "val2";
   char  val3_data[] = "val3";
   slice key         = slice_create(3, key_data);
   slice val0        = slice_create(4, val0_data);
   slice val1        = slice_create(4, val1_data);
   slice val2        = slice_create(4, val2_data);
   slice val3        = slice_create(4, val3_data);

   slice                    values[] = {val0, val1, val2, val3};
   splinterdb_column_family cfs[]    = {cf0, cf1, cf2, cf3};

   // Perform insertions
   for (int idx = 0; idx < 4; idx++) {
      splinterdb_cf_insert(cfs[idx], key, values[idx]);
   }

   // lookup the key from each column family
   // and ensure the right value is returned
   for (int idx = 0; idx < 4; idx++) {
      splinterdb_lookup_result result;
      splinterdb_cf_lookup_result_init(cfs[idx], &result, 0, NULL);

      int rc = splinterdb_cf_lookup(cfs[idx], key, &result);
      ASSERT_EQUAL(0, rc);
      ASSERT_TRUE(splinterdb_cf_lookup_found(&result));

      // Lookup should return correct values
      slice value;
      rc = splinterdb_cf_lookup_result_value(&result, &value);
      ASSERT_EQUAL(0, rc);
      ASSERT_EQUAL(slice_length(values[idx]), slice_length(value));
      ASSERT_STREQN(
         slice_data(values[idx]), slice_data(value), slice_length(value));

      splinterdb_cf_lookup_result_deinit(&result);
   }
}

/*
 * Custom key compare function that reverses the keys
 */
static int
rev_key_compare(const data_config *cfg, slice key1, slice key2)
{
   platform_assert(slice_data(key1) != NULL);
   platform_assert(slice_data(key2) != NULL);

   return slice_lex_cmp(key2, key1);
}

/*
 * Test multiple column families with range iterators
 * ensure that keys are found in the order defined by their
 * custom key comparison functions
 */
CTEST2(column_family, test_multiple_cf_range)
{
   // create the default column family
   splinterdb_column_family cf_default = column_family_create(
      data->kvsb, TEST_MAX_KEY_SIZE, &data->default_data_cfg);

   // create a config with a reversed key compare function
   // and create a column family that will reverse the keys
   data_config rev_data_config;
   default_data_config_init(TEST_MAX_KEY_SIZE, &rev_data_config);
   rev_data_config.key_compare = rev_key_compare;

   splinterdb_column_family cf_reverse =
      column_family_create(data->kvsb, TEST_MAX_KEY_SIZE, &rev_data_config);

   // Insert a few key/value pairs to each cf
   char key1_data[] = "aaaa";
   char key2_data[] = "bbbb";
   char key3_data[] = "cccc";
   char key4_data[] = "dddd";
   char cf1_value[] = "val-in-cf1";
   char cf2_value[] = "val-in-cf2";

   slice key1 = slice_create(4, key1_data);
   slice key2 = slice_create(4, key2_data);
   slice key3 = slice_create(4, key3_data);
   slice key4 = slice_create(4, key4_data);
   slice val1 = slice_create(10, cf1_value);
   slice val2 = slice_create(10, cf2_value);

   ASSERT_EQUAL(0, splinterdb_cf_insert(cf_default, key1, val1));
   ASSERT_EQUAL(0, splinterdb_cf_insert(cf_default, key2, val1));
   ASSERT_EQUAL(0, splinterdb_cf_insert(cf_default, key3, val1));
   ASSERT_EQUAL(0, splinterdb_cf_insert(cf_default, key4, val1));

   ASSERT_EQUAL(0, splinterdb_cf_insert(cf_reverse, key1, val2));
   ASSERT_EQUAL(0, splinterdb_cf_insert(cf_reverse, key2, val2));
   ASSERT_EQUAL(0, splinterdb_cf_insert(cf_reverse, key3, val2));
   ASSERT_EQUAL(0, splinterdb_cf_insert(cf_reverse, key4, val2));

   // Perform a range query over all cf1 keys
   splinterdb_cf_iterator *it;
   ASSERT_EQUAL(0, splinterdb_cf_iterator_init(cf_default, &it, NULL_SLICE));

   slice keys[] = {key1, key2, key3, key4};
   slice key;
   slice val;
   int   idx = 0;
   for (; splinterdb_cf_iterator_get_current(it, &key, &val);
        splinterdb_cf_iterator_next(it))
   {
      ASSERT_EQUAL(slice_length(keys[idx]), slice_length(key));
      ASSERT_STREQN(slice_data(keys[idx]), slice_data(key), slice_length(key));

      ASSERT_EQUAL(slice_length(val1), slice_length(val));
      ASSERT_STREQN(slice_data(val1), slice_data(val), slice_length(val));
      ++idx;
   }
   ASSERT_EQUAL(4, idx);

   splinterdb_cf_iterator_deinit(it);

   // Perform a range query over all cf2 keys
   ASSERT_EQUAL(0, splinterdb_cf_iterator_init(cf_reverse, &it, NULL_SLICE));

   idx = 0;
   for (; splinterdb_cf_iterator_get_current(it, &key, &val);
        splinterdb_cf_iterator_next(it))
   {
      ASSERT_EQUAL(slice_length(keys[3 - idx]), slice_length(key));
      ASSERT_STREQN(
         slice_data(keys[3 - idx]), slice_data(key), slice_length(key));

      ASSERT_EQUAL(slice_length(val2), slice_length(val));
      ASSERT_STREQN(slice_data(val2), slice_data(val), slice_length(val));
      ++idx;
   }
   ASSERT_EQUAL(4, idx);

   splinterdb_cf_iterator_deinit(it);
}


/*
 * These functions implement merge functionality so that we can test the update
 * function within column families
 */

// merge two messages, with result in new_message
static int
merge_tuples(const data_config *cfg,
             slice              key,
             message            old_message,
             merge_accumulator *new_message)
{
   platform_assert(slice_data(key) != NULL);
   message_type     type = old_message.type;
   writable_buffer *wb   = &new_message->data;

   // extract value slices
   slice old_value = old_message.data;
   slice new_value = writable_buffer_to_slice(wb);

   // use the cfg's key compare function to find maximal value
   // and retain that value
   if (cfg->key_compare(cfg, old_value, new_value) < 0) {
      platform_status rc = writable_buffer_copy_slice(wb, new_value);
      if (!SUCCESS(rc))
         return rc.r;
   } else {
      platform_status rc = writable_buffer_copy_slice(wb, old_value);
      if (!SUCCESS(rc))
         return rc.r;
   }

   if (type == MESSAGE_TYPE_INSERT)
      new_message->type = MESSAGE_TYPE_INSERT;
   else
      new_message->type = MESSAGE_TYPE_UPDATE;

   return 0;
}

static int
merge_tuple_final(const data_config *cfg,
                  slice              key,
                  merge_accumulator *oldest_message)
{
   platform_assert(slice_data(key) != NULL);

   // simply set type to INSERT
   oldest_message->type = MESSAGE_TYPE_INSERT;
   return 0;
}


CTEST2(column_family, multiple_cf_with_updates)
{
   // create the default column family
   data->default_data_cfg.merge_tuples       = merge_tuples;
   data->default_data_cfg.merge_tuples_final = merge_tuple_final;
   splinterdb_column_family cf_default       = column_family_create(
      data->kvsb, TEST_MAX_KEY_SIZE, &data->default_data_cfg);

   // create a config with a reversed key compare function
   // and create a column family that will reverse the keys
   data_config rev_data_config;
   default_data_config_init(TEST_MAX_KEY_SIZE, &rev_data_config);
   rev_data_config.key_compare        = rev_key_compare;
   rev_data_config.merge_tuples       = merge_tuples;
   rev_data_config.merge_tuples_final = merge_tuple_final;

   splinterdb_column_family cf_reverse =
      column_family_create(data->kvsb, TEST_MAX_KEY_SIZE, &rev_data_config);

   // Insert a few key/value pairs to each cf
   char key1_data[] = "aaaa";
   char key2_data[] = "bbbb";
   char key3_data[] = "cccc";
   char key4_data[] = "dddd";
   char cf1_value[] = "val-in-cf1";
   char cf2_value[] = "val-in-cf2";

   slice key1 = slice_create(4, key1_data);
   slice key2 = slice_create(4, key2_data);
   slice key3 = slice_create(4, key3_data);
   slice key4 = slice_create(4, key4_data);
   slice val1 = slice_create(10, cf1_value);
   slice val2 = slice_create(10, cf2_value);

   slice keys[] = {key1, key2, key3, key4};

   for (int i = 0; i < 4; i++)
      ASSERT_EQUAL(0, splinterdb_cf_insert(cf_default, keys[i], val1));

   for (int i = 0; i < 4; i++)
      ASSERT_EQUAL(0, splinterdb_cf_insert(cf_reverse, keys[i], val2));

   // Now update these key-value pairs
   char small_val[] = "aaaaaa-cf2";
   char big_val[]   = "zzzzzz-cf1";

   slice new_val1 = slice_create(10, big_val);
   slice new_val2 = slice_create(10, small_val);

   // apply both updates to all keys in cf_default
   for (int i = 0; i < 4; i++) {
      ASSERT_EQUAL(0, splinterdb_cf_update(cf_default, keys[i], new_val1));
      ASSERT_EQUAL(0, splinterdb_cf_update(cf_default, keys[i], new_val2));
   }

   // apply both updates to all keys in cf_reverse
   for (int i = 0; i < 4; i++) {
      ASSERT_EQUAL(0, splinterdb_cf_update(cf_reverse, keys[i], new_val1));
      ASSERT_EQUAL(0, splinterdb_cf_update(cf_reverse, keys[i], new_val2));
   }

   splinterdb_column_family cfs[] = {cf_default, cf_reverse};

   // now lookup each key in both cfs
   for (int idx = 0; idx < 2; idx++) {
      for (int i = 0; i < 4; i++) {
         splinterdb_lookup_result result;
         splinterdb_cf_lookup_result_init(cfs[idx], &result, 0, NULL);

         int rc = splinterdb_cf_lookup(cfs[idx], keys[i], &result);
         ASSERT_EQUAL(0, rc);
         ASSERT_TRUE(splinterdb_cf_lookup_found(&result));

         // Lookup should return correct values
         slice value;
         rc = splinterdb_cf_lookup_result_value(&result, &value);
         ASSERT_EQUAL(0, rc);
         if (idx == 0) {
            ASSERT_EQUAL(slice_length(new_val1), slice_length(value));
            ASSERT_STREQN(
               slice_data(new_val1), slice_data(value), slice_length(value));
         } else {
            ASSERT_EQUAL(slice_length(new_val2), slice_length(value));
            ASSERT_STREQN(
               slice_data(new_val2), slice_data(value), slice_length(value));
         }

         splinterdb_cf_lookup_result_deinit(&result);
      }
   }
}
