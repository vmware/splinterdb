//
// Created by Aaditya Rangarajan on 3/13/24.
//
#include <unistd.h>
#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#define DB_FILE_NAME    "splinterdb_intro_db"
#define DB_FILE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define CACHE_SIZE_MB   64
#define USER_MAX_KEY_SIZE ((int)100)


int next_command(FILE *input, int *op, uint64_t *arg) {
    int ret;
    char command[64];

    ret = fscanf(input, "%s %ld", command, arg);
    if (ret == EOF)
        return EOF;
    else if (ret != 2) {
        fprintf(stderr, "Parse error\n");
        exit(3);
    }

    if (strcmp(command, "Inserting") == 0) {
        *op = 0;
    } else if (strcmp(command, "Updating") == 0) {
        *op = 1;
    } else if (strcmp(command, "Deleting") == 0) {
        *op = 2;
    } else if (strcmp(command, "Query") == 0) {
        *op = 3;
        if (1 != fscanf(input, " -> %s", command)) {
            fprintf(stderr, "Parse error\n");
            exit(3);
        }
    } else if (strcmp(command, "Full_scan") == 0) {
        *op = 4;
    } else if (strcmp(command, "Lower_bound_scan") == 0) {
        *op = 5;
    } else if (strcmp(command, "Upper_bound_scan") == 0) {
        *op = 6;
    } else {
        fprintf(stderr, "Unknown command: %s\n", command);
        exit(1);
    }

    return 0;
}


int test(splinterdb* spl_handle, FILE *script_input, int nops) {
    slice key, value;;
    for (unsigned int i = 0; i < nops; i++) {
        int op;
        uint64_t u;
        char t[100];
        if (script_input) {
            int r = next_command(script_input, &op, &u);
            if (r == EOF)
                exit(0);
            else if (r < 0)
                exit(4);
        } else {
            op = rand() % 7;
            u = rand() % 100000;
        }
        sprintf(t, "%ld", u);
        switch (op) {
            case 0:  // insert
                key   = slice_create((size_t)strlen(t), t);
                value = slice_create((size_t)strlen(t), t);
                splinterdb_insert(spl_handle, key, value);
                break;
            case 1:  // update
                key   = slice_create((size_t)strlen(t), t);
                value = slice_create((size_t)strlen(t), t);
                splinterdb_insert(spl_handle, key, value);
                break;
            case 3:  // query
                splinterdb_lookup_result result;
                splinterdb_lookup_result_init(spl_handle, &result, 0, NULL);
                key   = slice_create((size_t)strlen(t), t);
                value = slice_create((size_t)strlen(t), t);
                printf("\nLookup\n");
                splinterdb_lookup(spl_handle, key, &result);
                splinterdb_lookup_result_value(&result, &value);
                break;
            default:
                abort();
        }
    }
    return 0;
}

void timer_start(uint64_t *timer) {
    struct timeval t;
    assert(!gettimeofday(&t, NULL));
    timer -= 1000000 * t.tv_sec + t.tv_usec;
}

void timer_stop(uint64_t *timer) {
    struct timeval t;
    assert(!gettimeofday(&t, NULL));
    timer += 1000000 * t.tv_sec + t.tv_usec;
}

int main(int argc, char **argv) {
    char *script_infile = NULL;
    unsigned int random_seed = time(NULL) * getpid();
    srand(random_seed);
    int opt;
    char *term;
    int nops = 0;
    while ((opt = getopt(argc, argv, "i:n:")) != -1) {
        switch(opt) {
            case 'i':
                script_infile = optarg;
                break;
            case 'n':
                nops = strtoull(optarg, &term, 10);
                break;
            default:
                exit(1);
        }
    }

    FILE *script_input = NULL;
    if (script_infile) {
        script_input = fopen(script_infile, "r");
        if (script_input == NULL) {
            perror("Couldn't open input file");
            exit(1);
        }
    }
    data_config splinter_data_cfg;
    default_data_config_init(USER_MAX_KEY_SIZE, &splinter_data_cfg);

    // Basic configuration of a SplinterDB instance
    splinterdb_config splinterdb_cfg;
    memset(&splinterdb_cfg, 0, sizeof(splinterdb_cfg));
    splinterdb_cfg.filename   = DB_FILE_NAME;
    splinterdb_cfg.disk_size  = (DB_FILE_SIZE_MB * 1024 * 1024);
    splinterdb_cfg.cache_size = (CACHE_SIZE_MB * 1024 * 1024);
    splinterdb_cfg.data_cfg   = &splinter_data_cfg;

    splinterdb *spl_handle = NULL; // To a running SplinterDB instance

    int rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
    printf("Created SplinterDB instance, dbname '%s'.\n\n", DB_FILE_NAME);
    uint64_t timer = 0;
    timer_start(&timer);
    test(spl_handle, script_input, nops);
    timer_stop(&timer);
    return rc;
}