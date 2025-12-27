#pragma once

#include <string.h>

#define MAX_STRING_LENGTH 256

typedef struct {
   char *token_str;
   char *last_token;
   int   last_token_len;
} platform_strtok_ctx;

static inline char *
platform_strtok_r(char *str, const char *delim, platform_strtok_ctx *ctx)
{
   return strtok_r(str, delim, &ctx->token_str);
}
