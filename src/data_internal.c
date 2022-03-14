
#include "data_internal.h"

message NULL_MESSAGE = {.type = MESSAGE_TYPE_INVALID,
                        .data = {.length = 0, .data = NULL}};

message DELETE_MESSAGE = {.type = MESSAGE_TYPE_DELETE,
                          .data = {.length = 0, .data = NULL}};
