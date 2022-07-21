#include "atomic_counter.h"

void
atomic_counter_init(atomic_counter *counter)
{
   counter->num = 0;
}

void
atomic_counter_deinit(atomic_counter *counter)
{}

uint64
atomic_counter_get_next(atomic_counter *counter)
{
   return __sync_add_and_fetch(&counter->num, 1);
}
