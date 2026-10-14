#include <splinterdb/splinterdb.h>

static bool
accept_key(void *context, slice key)
{
   (void)context;
   (void)key;
   return true;
}

int
check_public_api(splinterdb *db, splinterdb_iterator **it, slice key)
{
   int rc = splinterdb_iterator_init(db, it, greater_than_or_equal, key);
   rc += splinterdb_iterator_init_with_bounds(db, it, greater_than_or_equal,
                                             key, less_than, key,
                                             less_than_or_equal, key);
   rc += splinterdb_iterator_init_with_filter(db, it, greater_than_or_equal,
                                             key, accept_key, 0, key);
   rc += splinterdb_iterator_init_with_bounds_and_filter(
      db, it, greater_than_or_equal, key, less_than, key,
      less_than_or_equal, key, accept_key, 0, key);
   return rc;
}
