#include "simple_set.h"
#include "platform_linux/platform_types.h"

static inline uint64
hash(const char *key, uint64 size, uint64 seed)
{
   int i = 0;

   while (i < size) {
      seed = ((seed << 5) + seed) + key[i++];
   }

   return seed;

   /* return (uint64)XXH64(key, size, seed); */
}

void
simple_set_init(simple_set *set)
{
   for (int i = 0; i < SIMPLE_SET_SIZE_LIMIT; ++i) {
      set->nodes[i].meta = 0;
   }
   set->seed = 220523;
   set->size = 0;
}

int
simple_set_insert(simple_set *set, transaction_op_meta *meta)
{
   if (set->size >= SIMPLE_SET_SIZE_LIMIT)
      return -1;

   uint64 idx =
      hash((const char *)meta, sizeof(transaction_op_meta *), set->seed)
      % SIMPLE_SET_SIZE_LIMIT;

   uint64 start_idx = idx;
   while (set->nodes[idx].meta) {
      if (transaction_op_meta_is_key_equal(set->nodes[idx].meta, meta) == 0) {
         return 0;
      }

      idx = (idx + 1) % SIMPLE_SET_SIZE_LIMIT;
      if (idx == start_idx) {
         // set size is full
         // TODO: growing the table instead of returning an error
         return -1;
      }
   }

   set->nodes[idx].meta = meta;
   transaction_op_meta_inc_ref(meta);

   ++set->size;

   return 0;
}

int
simple_set_delete(simple_set *set, transaction_op_meta *meta)
{
   uint64 idx =
      hash((const char *)meta, sizeof(transaction_op_meta *), set->seed)
      % SIMPLE_SET_SIZE_LIMIT;

   uint64 start_idx = idx;
   do {
      if (transaction_op_meta_is_key_equal(set->nodes[idx].meta, meta) == 0) {
         set->nodes[idx].meta = 0;
         transaction_op_meta_dec_ref(meta);
         --set->size;
         return 0;
      }

      idx = (idx + 1) % SIMPLE_SET_SIZE_LIMIT;
   } while (idx != start_idx);

   return -1;
}

bool
simple_set_is_overlap(simple_set *s1, simple_set *s2)
{
   for (int i = 0; i < s1->size; ++i) {
      for (int j = 0; j < s2->size; ++j) {
         if (s1->nodes[i].meta && s2->nodes[j].meta
             && transaction_op_meta_is_key_equal(s1->nodes[i].meta,
                                                 s2->nodes[j].meta)
                   == 0)
         {
            return TRUE;
         }
      }
   }

   return FALSE;
}

simple_set_iter
simple_set_first(simple_set *set)
{
   simple_set_iter it = {.set = set, .i = 0};
   while (!set->nodes[it.i].meta) {
      ++it.i;
   }

   return it;
}

simple_set_iter
simple_set_iter_next(simple_set_iter it)
{
   do {
      ++it.i;
   } while (!it.set->nodes[it.i].meta);

   return it;
}

bool
simple_set_iter_is_valid(simple_set_iter it)
{
   return it.i < SIMPLE_SET_SIZE_LIMIT;
}

transaction_op_meta *
simple_set_iter_data(simple_set_iter it)
{
   return it.set->nodes[it.i].meta;
}
