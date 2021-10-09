#include "../tests/test_data.h"
#include "dynamic_btree.c"

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
  int nkvs = 302;

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

static int leaf_split_tests(dynamic_btree_config *cfg)
{
  char leaf_buffer[cfg->page_size];
  char msg_buffer[cfg->page_size];
  dynamic_btree_hdr *hdr = (dynamic_btree_hdr *)leaf_buffer;

  slice dog    = slice_create(3, "dog");
  slice marmut = slice_create(6, "marmut");
  slice msg    = slice_create(4 * cfg->page_size / 10, msg_buffer);

  dynamic_btree_init_hdr(cfg, hdr);
  if (!dynamic_btree_set_leaf_entry(cfg, hdr, 0, dog, msg))
    platform_log("failed to insert kvpair dog\n");
  if (!dynamic_btree_set_leaf_entry(cfg, hdr, 1, marmut, msg))
    platform_log("failed to insert kvpair marmut\n");

  slice cat   = slice_create(3, "cat");
  slice emu   = slice_create(3, "emu");
  slice zebra = slice_create(5, "zebra");

  uint64 idx;
  slice splitting_key;

  idx = dynamic_btree_choose_leaf_split(cfg, hdr, cat, msg, &splitting_key);
  if (idx != 0)
    platform_log("bad splitting index %lu (expected 0)\n", idx);

  idx = dynamic_btree_choose_leaf_split(cfg, hdr, emu, msg, &splitting_key);
  if (idx != 1)
    platform_log("bad splitting index %lu (expected 1)\n", idx);

  idx = dynamic_btree_choose_leaf_split(cfg, hdr, zebra, msg, &splitting_key);
  if (idx != 2)
    platform_log("bad splitting index %lu (expected 2)\n", idx);

  return 0;
}

int main(int argc, char **argv)
{
  leaf_hdr_tests(&test_config, &test_scratch);
  leaf_hdr_search_tests(&test_config, &test_scratch);

  index_hdr_tests(&test_config, &test_scratch);
  index_hdr_search_tests(&test_config);

  leaf_split_tests(&test_config);
}
