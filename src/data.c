
#include "data.h"

static const char  neginfbuf                         = 0;
static const char  posinfbuf                         = 0;
const void        *data_key_negative_infinity_buffer = &neginfbuf;
const void        *data_key_positive_infinity_buffer = &posinfbuf;
const bytebuffer   data_key_negative_infinity        = (const bytebuffer){0, (void *)&neginfbuf};
const bytebuffer   data_key_positive_infinity        = (const bytebuffer){0, (void *)&posinfbuf};
