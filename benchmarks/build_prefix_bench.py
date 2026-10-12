#!/usr/bin/env python3
"""Build identical prefix workload code against a baseline or candidate tree.

PREFIX_PATTERN=common|random|nonlexical selects 64-byte keys. Random keys have
uniformly dispersed first words, random tails, and a rank for validation. All
patterns preserve the kvbench logical-id ordering under their comparator.
PREFIX_DISABLE=1 disables compression on a capable candidate, for A/B control.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

p = argparse.ArgumentParser()
p.add_argument('tree', type=Path)
p.add_argument('--build-root', default='build')
a = p.parse_args()
tree = a.tree.resolve()
source = Path(__file__).with_name('kvbench.c').read_text()
source = source.replace('#define KEY_SIZE    24', '#define KEY_SIZE    64')
begin = source.index('static inline void\nmake_key(')
end = source.index('static inline void\nmake_value(', begin)
source = source[:begin] + r'''
static int prefix_pattern;
static uint64_t *prefix_random_words;

static int
prefix_word_compare(const void *a, const void *b)
{
   uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
   return (x > y) - (x < y);
}

static int
prefix_nonlexical_compare(const data_config *cfg, user_key a, user_key b)
{
   (void)cfg;
   if (slice_length(a.key) != KEY_SIZE || slice_length(b.key) != KEY_SIZE)
      abort();
   return memcmp((const char *)slice_data(a.key) + 56,
                 (const char *)slice_data(b.key) + 56, 8);
}

static void
prefix_configure(void)
{
   const char *pattern = getenv("PREFIX_PATTERN");
   if (pattern == NULL || strcmp(pattern, "common") == 0) prefix_pattern = 0;
   else if (strcmp(pattern, "random") == 0) prefix_pattern = 1;
   else if (strcmp(pattern, "nonlexical") == 0) prefix_pattern = 2;
   else { fprintf(stderr, "Unknown PREFIX_PATTERN %s\n", pattern); exit(2); }
   if (prefix_pattern == 2) data_cfg.key_compare = prefix_nonlexical_compare;
#ifdef KVBENCH_PREFIX_SUPPORT
   data_cfg.key_compare_is_lexical = prefix_pattern != 2
                                   && getenv("PREFIX_DISABLE") == NULL;
#endif
   if (prefix_pattern == 1) {
      if (opt.n > SIZE_MAX / 2 / sizeof(uint64_t)) abort();
      prefix_random_words = malloc(2 * opt.n * sizeof(uint64_t));
      if (!prefix_random_words) abort();
      for (uint64_t i = 0; i < 2 * opt.n; i++)
         prefix_random_words[i] = mix(i + opt.seed * 1000003);
      qsort(prefix_random_words, 2 * opt.n, sizeof(uint64_t), prefix_word_compare);
   }
   printf("prefix-pattern=%s key_bytes=%d compression_disabled=%s\n",
          pattern ? pattern : "common", KEY_SIZE,
          getenv("PREFIX_DISABLE") ? "true" : "false");
}

static inline void
prefix_encode_word(uint64_t word, char *key)
{
   for (int i = 0; i < 8; i++) key[i] = (char)(word >> (56 - 8 * i));
}

static inline void
make_key(uint64_t k, char *key)
{
   if (prefix_pattern == 0) {
      memset(key, 'p', 48);
      prefix_encode_word(k, key + 48);
   } else {
      for (unsigned i = 0; i < 7; i++)
         prefix_encode_word(mix(k + (uint64_t)i * UINT64_C(0x9e3779b97f4a7c15)),
                            key + i * 8);
      if (prefix_pattern == 1) prefix_encode_word(prefix_random_words[k], key);
   }
   prefix_encode_word(k, key + 56);
}

static inline bool
decode_key(slice s, uint64_t *k)
{
   if (slice_length(s) != KEY_SIZE) return false;
   const unsigned char *bytes = slice_data(s);
   uint64_t rank = 0;
   for (unsigned i = 56; i < 64; i++) rank = (rank << 8) | bytes[i];
   if (rank >= 2 * opt.n) return false;
   char expected[KEY_SIZE];
   make_key(rank, expected);
   *k = rank;
   return memcmp(bytes, expected, KEY_SIZE) == 0;
}

''' + source[end:]
source = source.replace('static const char KEY_PAD[16] = "kvbench-padding!";', '')
source = source.replace('default_data_config_init(&data_cfg);',
                        'default_data_config_init(&data_cfg); prefix_configure();')
binary = tree / a.build_root / 'release/bin/prefix_kvbench'
binary.parent.mkdir(parents=True, exist_ok=True)
generated = binary.with_suffix('.c')
generated.write_text(source)
command = ['cc', '-D_GNU_SOURCE', '-ggdb', '-Wall', '-pthread', '-Werror',
           '-DXXH_STATIC_LINKING_ONLY', '-fPIC', '-DSPLINTERDB_PLATFORM_DIR=platform_linux',
           '-O3', '-ffast-math', '-flto', '-march=native', '-DKVBENCH_CLOSE_HAS_FORCE',
           '-DKVBENCH_SPACE_MODE']
if 'key_compare_is_lexical' in (tree / 'include/splinterdb/data.h').read_text():
    command.append('-DKVBENCH_PREFIX_SUPPORT')
command += ['-I', str(tree / 'include'), str(generated),
            str(tree / a.build_root / 'release/lib/libsplinterdb.a'), '-laio', '-lxxhash', '-lm',
            '-lpthread', '-o', str(binary)]
subprocess.run(command, check=True)
binary.with_suffix('.json').write_text(json.dumps(dict(
    command=command, source_sha256=hashlib.sha256(source.encode()).hexdigest(),
    binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest()), indent=2) + '\n')
print(binary)
