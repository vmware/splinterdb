#include "../tests/test.h"
#include "../tests/test_data.h"
#include "dynamic_btree.c"

static int leaf_hdr_tests(dynamic_btree_config *cfg, dynamic_btree_scratch *scratch)
{
  char leaf_buffer[cfg->page_size];
  dynamic_btree_hdr *hdr = (dynamic_btree_hdr *)leaf_buffer;
  int nkvs = 240;

  dynamic_btree_init_hdr(cfg, hdr);

  for (uint32 i = 0; i < nkvs; i++) {
    if (!dynamic_btree_set_leaf_entry(cfg, hdr, i, slice_create(i % sizeof(i), &i), slice_create(i % sizeof(i), &i)))
      platform_log("failed to insert 4-byte %d\n", i);
  }

  for (uint32 i = 0; i < nkvs; i++) {
    slice key = dynamic_btree_get_tuple_key(cfg, hdr, i);
    slice message = dynamic_btree_get_tuple_message(cfg, hdr, i);
    if (slice_lex_cmp(slice_create(i % sizeof(i), &i), key))
      platform_log("bad 4-byte key %d\n", i);
    if (slice_lex_cmp(slice_create(i % sizeof(i), &i), message))
      platform_log("bad 4-byte message %d\n", i);
  }

  for (uint64 i = 0; i < nkvs; i++) {
    if (!dynamic_btree_set_leaf_entry(cfg, hdr, i, slice_create(i % sizeof(i), &i), slice_create(i % sizeof(i), &i)))
      platform_log("failed to insert 8-byte %ld\n", i);
  }

  for (uint64 i = 0; i < nkvs; i++) {
    slice key = dynamic_btree_get_tuple_key(cfg, hdr, i);
    slice message = dynamic_btree_get_tuple_message(cfg, hdr, i);
    if (slice_lex_cmp(slice_create(i % sizeof(i), &i), key))
      platform_log("bad 4-byte key %ld\n", i);
    if (slice_lex_cmp(slice_create(i % sizeof(i), &i), message))
      platform_log("bad 4-byte message %ld\n", i);
  }

  dynamic_btree_defragment_leaf(cfg, scratch, hdr, -1);

  for (uint64 i = 0; i < nkvs; i++) {
    slice key = dynamic_btree_get_tuple_key(cfg, hdr, i);
    slice message = dynamic_btree_get_tuple_message(cfg, hdr, i);
    if (slice_lex_cmp(slice_create(i % sizeof(i), &i), key))
      platform_log("bad 4-byte key %ld\n", i);
    if (slice_lex_cmp(slice_create(i % sizeof(i), &i), message))
      platform_log("bad 4-byte message %ld\n", i);
  }

  return 0;
}

static int leaf_hdr_search_tests(dynamic_btree_config *cfg, dynamic_btree_scratch *scratch)
{
  char leaf_buffer[cfg->page_size];
  dynamic_btree_hdr *hdr = (dynamic_btree_hdr *)leaf_buffer;
  int nkvs = 256;

  dynamic_btree_init_hdr(cfg, hdr);

  for (int i = 0; i < nkvs; i++) {
    uint64 generation;
    uint8 keybuf[1];
    uint8 messagebuf[8];
    keybuf[0] = 17 * i;
    messagebuf[0] = i;

    slice key     = slice_create(1, &keybuf);
    slice message = slice_create(i % 8, messagebuf);
    leaf_incorporate_spec spec;
    bool result = dynamic_btree_leaf_incorporate_tuple(cfg, scratch, hdr, key, message, &spec, &generation);
    if (!result)
      platform_log("couldn't incorporate kv pair %d\n", i);
    if (generation != i)
      platform_log("bad generation %d %lu\n", i, generation);
  }

  for (int i = 0; i < nkvs; i++) {
    slice key = dynamic_btree_get_tuple_key(cfg, hdr, i);
    uint8 ui = i;
    if (slice_lex_cmp(slice_create(1, &ui), key))
      platform_log("bad 4-byte key %d\n", i);
  }

  return 0;
}

static int index_hdr_tests(dynamic_btree_config *cfg, dynamic_btree_scratch *scratch)
{
  char index_buffer[cfg->page_size];
  dynamic_btree_hdr *hdr = (dynamic_btree_hdr *)index_buffer;
  int nkvs = 100;

  dynamic_btree_init_hdr(cfg, hdr);
  hdr->height = 1;

  for (uint32 i = 0; i < nkvs; i++) {
    if (!dynamic_btree_set_index_entry(cfg, hdr, i, slice_create(i % sizeof(i), &i), i, 0, 0, 0))
      platform_log("failed to insert 4-byte %d\n", i);
  }

  for (uint32 i = 0; i < nkvs; i++) {
    slice key = dynamic_btree_get_pivot(cfg, hdr, i);
    uint64 childaddr = dynamic_btree_get_child_addr(cfg, hdr, i);
    if (slice_lex_cmp(slice_create(i % sizeof(i), &i), key))
      platform_log("bad 4-byte key %d\n", i);
    if (childaddr != i)
      platform_log("bad childaddr %d\n", i);
  }

  for (uint64 i = 0; i < nkvs; i++) {
    if (!dynamic_btree_set_index_entry(cfg, hdr, i, slice_create(i % sizeof(i), &i), i, 0, 0, 0))
      platform_log("failed to insert 8-byte %ld\n", i);
  }

  for (uint64 i = 0; i < nkvs; i++) {
    slice key = dynamic_btree_get_pivot(cfg, hdr, i);
    uint64 childaddr = dynamic_btree_get_child_addr(cfg, hdr, i);
    if (slice_lex_cmp(slice_create(i % sizeof(i), &i), key))
      platform_log("bad 4-byte key %ld\n", i);
    if (childaddr != i)
      platform_log("bad childaddr %ld\n", i);
  }

  dynamic_btree_defragment_index(cfg, scratch, hdr);

  for (uint64 i = 0; i < nkvs; i++) {
    slice key = dynamic_btree_get_pivot(cfg, hdr, i);
    uint64 childaddr = dynamic_btree_get_child_addr(cfg, hdr, i);
    if (slice_lex_cmp(slice_create(i % sizeof(i), &i), key))
      platform_log("bad 4-byte key %ld\n", i);
    if (childaddr != i)
      platform_log("bad childaddr %ld\n", i);
  }

  return 0;
}

static int index_hdr_search_tests(dynamic_btree_config *cfg)
{
  char index_buffer[cfg->page_size];
  dynamic_btree_hdr *hdr = (dynamic_btree_hdr *)index_buffer;
  int nkvs = 100;

  dynamic_btree_init_hdr(cfg, hdr);
  hdr->height = 1;

  for (int i = 0; i < nkvs; i += 2) {
    uint8 keybuf[1];
    keybuf[0] = i;
    slice key     = slice_create(1, &keybuf);
    if (!dynamic_btree_set_index_entry(cfg, hdr, i / 2, key, i, 0, 0, 0))
      platform_log("couldn't insert pivot %d\n", i);
  }

  for (int i = 0; i < nkvs; i++) {
    bool found;
    uint8 keybuf[1];
    keybuf[0] = i;
    slice key     = slice_create(1, &keybuf);
    int64 idx = dynamic_btree_find_pivot(cfg, hdr, key, &found);
    if (idx != i / 2)
      platform_log("bad pivot search result %ld for %d\n",
                   idx, i);
  }

  return 0;
}

static int leaf_split_tests(dynamic_btree_config *cfg, dynamic_btree_scratch *scratch, int nkvs)
{
  char leaf_buffer[cfg->page_size];
  char msg_buffer[cfg->page_size];
  dynamic_btree_hdr *hdr = (dynamic_btree_hdr *)leaf_buffer;

  dynamic_btree_init_hdr(cfg, hdr);

  int msgsize = cfg->page_size / (nkvs + 1);
  slice msg    = slice_create(msgsize, msg_buffer);
  slice bigger_msg = slice_create(msgsize + sizeof(table_entry) + 1, msg_buffer);

  uint8 realnkvs = 0;
  while (realnkvs < nkvs) {
    uint8 keybuf[1];
    keybuf[0] = 2 * realnkvs + 1;
    if (!dynamic_btree_set_leaf_entry(cfg, hdr, realnkvs, slice_create(1, &keybuf), msg))
      break;
    realnkvs++;
  }

  for (uint8 i = 0; i < 2 * realnkvs + 1; i++) {
    uint64 generation;
    leaf_incorporate_spec spec;
    slice key = slice_create(1, &i);
    bool success = dynamic_btree_leaf_incorporate_tuple(cfg, scratch, hdr, key, bigger_msg, &spec, &generation);
    if (success) {
      platform_log("Weird.  An incorporate that was supposed to fail actually succeeded (nkvs=%d, realnkvs=%d, i=%d).\n",
                   nkvs, realnkvs, i);
      dynamic_btree_print_locked_node(cfg, 0, hdr, PLATFORM_ERR_LOG_HANDLE);
    }
    leaf_splitting_plan plan = dynamic_btree_build_leaf_splitting_plan(cfg, hdr, spec);
    platform_assert(realnkvs / 2 - 1 <= plan.split_idx);
    platform_assert(plan.split_idx <= realnkvs / 2 + 1);
  }

  return 0;
}

/* static int insert_tests(dynamic_btree_config *cfg, dynamic_btree_scratch *scratch, int nkvs) */
/* { */
/*   return 0; */
/* } */

static int init_data_config_from_master_config(data_config *data_cfg, master_config *master_cfg)
{
   data_cfg->key_size           = master_cfg->key_size;
   data_cfg->message_size       = master_cfg->message_size;
   return 1;
}

static int init_io_config_from_master_config(io_config *io_cfg, master_config *master_cfg)
{
   io_config_init(io_cfg, master_cfg->page_size, master_cfg->extent_size,
                  master_cfg->io_flags, master_cfg->io_perms,
                  master_cfg->io_async_queue_depth, master_cfg->io_filename);
   return 1;
}

static int init_rc_allocator_config_from_master_config(rc_allocator_config *allocator_cfg, master_config *master_cfg)
{
   rc_allocator_config_init(allocator_cfg, master_cfg->page_size,
                            master_cfg->extent_size,
                            master_cfg->allocator_capacity);
   return 1;
}

static int init_clockcache_config_from_master_config(clockcache_config *cache_cfg, master_config *master_cfg)
{
   clockcache_config_init(cache_cfg, master_cfg->page_size,
                          master_cfg->extent_size, master_cfg->cache_capacity,
                          master_cfg->cache_logfile, master_cfg->use_stats);
   return 1;
}

static int init_dynamic_btree_config_from_master_config(dynamic_btree_config *dbtree_cfg, master_config *master_cfg, data_config *data_cfg)
{
  dynamic_btree_config_init(dbtree_cfg,
                            data_cfg,
                            master_cfg->btree_rough_count_height,
                            master_cfg->page_size,
                            master_cfg->extent_size);
  return 1;
}

int main(int argc, char **argv)
{
  master_config         master_cfg;
  data_config           data_cfg;
  io_config             io_cfg;
  rc_allocator_config   allocator_cfg;
  clockcache_config     cache_cfg;
  uint64                seed;
  dynamic_btree_scratch test_scratch;
  dynamic_btree_config  dbtree_cfg;
  /*   .page_size = 4096, */
  /*   .extent_size = 32 * 4096, */
  /*   .rough_count_height = 1, */
  /*   .data_cfg = &test_data_config */

  config_set_defaults(&master_cfg);
  data_cfg = test_data_config;
  if (!SUCCESS(config_parse(&master_cfg, 1, argc - 1, argv + 1))
      || !init_data_config_from_master_config(&data_cfg, &master_cfg)
      || !init_io_config_from_master_config(&io_cfg, &master_cfg)
      || !init_rc_allocator_config_from_master_config(&allocator_cfg, &master_cfg)
      || !init_clockcache_config_from_master_config(&cache_cfg, &master_cfg)
      || !init_dynamic_btree_config_from_master_config(&dbtree_cfg, &master_cfg, &data_cfg)
      )
    return -1;
  seed = master_cfg.seed;

  leaf_hdr_tests(&dbtree_cfg, &test_scratch);
  leaf_hdr_search_tests(&dbtree_cfg, &test_scratch);

  index_hdr_tests(&dbtree_cfg, &test_scratch);
  index_hdr_search_tests(&dbtree_cfg);

  for (int nkvs = 2; nkvs < 100; nkvs++)
    leaf_split_tests(&dbtree_cfg, &test_scratch, nkvs);

  return 0;
}
