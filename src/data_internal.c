
#include "data_internal.h"

static const char  neginfbuf                         = 0;
static const char  posinfbuf                         = 0;
const void        *data_key_negative_infinity_buffer = &neginfbuf;
const void        *data_key_positive_infinity_buffer = &posinfbuf;
const slice   data_key_negative_infinity        = (const slice){0, (void *)&neginfbuf};
const slice   data_key_positive_infinity        = (const slice){0, (void *)&posinfbuf};
