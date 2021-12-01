#include "fault_injection_allocator.h"

static inline MUST_CHECK_RESULT platform_status
fault_injection_allocator_alloc(allocator *al, uint64 *addr, page_type type)
{
   fault_injection_allocator *fia = (fault_injection_allocator *)al;

   uint64 burst_size;
   while ((burst_size = fia->current_burst_size)) {
      bool part_of_burst = __sync_bool_compare_and_swap(
         &fia->current_burst_size, burst_size, burst_size - 1);
      if (part_of_burst) {
         return STATUS_NO_SPACE;
      }
   }

   if (random_next_uint64(&fia->rs) < fia->failure_probability) {
      burst_size = random_next_uint64(&fia->rs) % fia->burst_size;
      __sync_fetch_and_add(&fia->current_burst_size, burst_size);
      return STATUS_NO_SPACE;
   }

   return allocator_alloc(fia->base, addr, type);
}

static uint8
fault_injection_allocator_inc_ref(allocator *a, uint64 addr)
{
   fault_injection_allocator *fia = (fault_injection_allocator *)a;
   return allocator_inc_ref(fia->base, addr);
}

static uint8
fault_injection_allocator_dec_ref(allocator *a, uint64 addr, page_type type)
{
   fault_injection_allocator *fia = (fault_injection_allocator *)a;
   return allocator_dec_ref(fia->base, addr, type);
}

static uint8
fault_injection_allocator_get_ref(allocator *a, uint64 addr)
{
   fault_injection_allocator *fia = (fault_injection_allocator *)a;
   return allocator_get_ref(fia->base, addr);
}

static platform_status
fault_injection_allocator_get_super_addr(allocator *       a,
                                         allocator_root_id spl_id,
                                         uint64 *          addr)
{
   fault_injection_allocator *fia = (fault_injection_allocator *)a;
   return allocator_get_super_addr(fia->base, spl_id, addr);
}

static platform_status
fault_injection_allocator_alloc_super_addr(allocator *       a,
                                           allocator_root_id spl_id,
                                           uint64 *          addr)
{
   fault_injection_allocator *fia = (fault_injection_allocator *)a;
   return allocator_alloc_super_addr(fia->base, spl_id, addr);
}

static void
fault_injection_allocator_remove_super_addr(allocator *       a,
                                            allocator_root_id spl_id)
{
   fault_injection_allocator *fia = (fault_injection_allocator *)a;
   return allocator_remove_super_addr(fia->base, spl_id);
}

static uint64
fault_injection_allocator_get_capacity(allocator *a)
{
   fault_injection_allocator *fia = (fault_injection_allocator *)a;
   return allocator_get_capacity(fia->base);
}

static uint64
fault_injection_allocator_extent_size(allocator *a)
{
   fault_injection_allocator *fia = (fault_injection_allocator *)a;
   return allocator_extent_size(fia->base);
}

static uint64
fault_injection_allocator_page_size(allocator *a)
{
   fault_injection_allocator *fia = (fault_injection_allocator *)a;
   return allocator_page_size(fia->base);
}

static void
fault_injection_allocator_assert_noleaks(allocator *a)
{
   fault_injection_allocator *fia = (fault_injection_allocator *)a;
   return allocator_assert_noleaks(fia->base);
}

static void
fault_injection_allocator_print_stats(allocator *a)
{
   fault_injection_allocator *fia = (fault_injection_allocator *)a;
   return allocator_print_stats(fia->base);
}

static void
fault_injection_allocator_debug_print(allocator *a)
{
   fault_injection_allocator *fia = (fault_injection_allocator *)a;
   return allocator_debug_print(fia->base);
}

const static allocator_ops fault_injection_allocator_ops = {
   .alloc             = fault_injection_allocator_alloc,
   .inc_ref           = fault_injection_allocator_inc_ref,
   .dec_ref           = fault_injection_allocator_dec_ref,
   .get_ref           = fault_injection_allocator_get_ref,
   .get_super_addr    = fault_injection_allocator_get_super_addr,
   .alloc_super_addr  = fault_injection_allocator_alloc_super_addr,
   .remove_super_addr = fault_injection_allocator_remove_super_addr,
   .get_capacity      = fault_injection_allocator_get_capacity,
   .get_extent_size   = fault_injection_allocator_extent_size,
   .get_page_size     = fault_injection_allocator_page_size,
   .assert_noleaks    = fault_injection_allocator_assert_noleaks,
   .print_stats       = fault_injection_allocator_print_stats,
   .debug_print       = fault_injection_allocator_debug_print,
};


MUST_CHECK_RESULT platform_status
fault_injection_allocator_init(fault_injection_allocator *al,
                               allocator *                base,
                               uint64                     failure_probability,
                               uint64                     burst_size,
                               uint64                     seed)
{
   al->super.ops           = &fault_injection_allocator_ops;
   al->base                = base;
   al->failure_probability = failure_probability;
   al->burst_size          = burst_size;
   random_init(&al->rs, seed, 0);
   al->current_burst_size = 0;
   return STATUS_OK;
}
