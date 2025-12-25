#pragma once

typedef struct {
   char *token_str;
   char *last_token;
   int   last_token_len;
} platform_strtok_ctx;

char *
platform_strtok_r(char *str, const char *delim, platform_strtok_ctx *ctx);
