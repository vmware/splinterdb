#include "../tests/test_data.h"
#include "../src/dynamic_btree.c"

static dynamic_btree_config test_config = {
  .page_size = 4096,
  .extent_size = 32 * 4096,
  .rough_count_height = 1,
  .data_cfg = &test_data_config
};

dynamic_btree_scratch test_scratch;

static int leaf_hdr_tests(dynamic_btree_config *cfg, dynamic_btree_scratch *scratch)
{
  char leaf_buffer[cfg->page_size];
  dynamic_btree_hdr *hdr = (dynamic_btree_hdr *)leaf_buffer;
  dynamic_btree_init_hdr(cfg, hdr);
  int nkvs = 302;

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

  dynamic_btree_defragment_leaf(cfg, scratch, hdr);

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
  dynamic_btree_init_hdr(cfg, hdr);
  int nkvs = 256;

  for (int i = 0; i < nkvs; i++) {
    uint64 generation;
    uint8 keybuf[1];
    uint8 messagebuf[8];
    keybuf[0] = 17 * i;
    messagebuf[0] = i;

    slice key     = slice_create(1, &keybuf);
    slice message = slice_create(i % 8, messagebuf);
    leaf_add_result result = dynamic_btree_leaf_incorporate_tuple(cfg, scratch, hdr, key, message, &generation);
    if (result != leaf_add_result_new_key)
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

int main(int argc, char **argv)
{
  leaf_hdr_tests(&test_config, &test_scratch);
  leaf_hdr_search_tests(&test_config, &test_scratch);
}
