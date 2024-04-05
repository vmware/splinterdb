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
#include <inttypes.h>

#define DB_FILE_NAME    "splinterdb_intro_db"
#define DB_FILE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define CACHE_SIZE_MB   64
#define USER_MAX_KEY_SIZE ((int)100)

enum {
    YCSB,
    CUSTOM
};

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


int next_command(FILE *input, int *op, uint64_t *arg, int mode) {
    int ret;
    char command[64];
    char *insert = mode == YCSB ? "I" : "Inserting";
    char *read = mode == YCSB ? "R" : "Query";
    char *update = mode == YCSB ? "U" : "Updating";
    ret = fscanf(input, "%s %ld", command, arg);
    if (ret == EOF)
        return EOF;
    else if (ret != 2) {
        fprintf(stderr, "Parse error\n");
        exit(3);
    }

    if (strcmp(command, insert) == 0) {
        *op = 0;
    } else if (strcmp(command, update) == 0) {
        *op = 1;
    } else if (strcmp(command, "Deleting") == 0) {
        *op = 2;
    } else if (strcmp(command, read) == 0) {
        *op = 3;
        if (mode == CUSTOM) {
            if (1 != fscanf(input, " -> %s", command)) {
                fprintf(stderr, "Parse error\n");
                exit(3);
            }
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


int test(splinterdb *spl_handle, FILE *script_input, uint64_t nops,
         uint64_t num_sections,
         uint64_t count_point1,
         uint64_t count_point2,
         uint64_t count_point3,
         uint64_t count_point4,
         uint64_t count_point5,
         uint64_t count_point6, int mode) {
    slice key, value;;

    uint64_t timer = 0;
    uint64_t count_points_array[] = {count_point1, count_point2,
                                     count_point3, count_point4,
                                     count_point5, count_point6};
    double timer_array[100];
    uint64_t num_of_loads_array[100];
    uint64_t num_of_stores_array[100];
    uint64_t section_index = 0;
    timer_start(&timer);

    for (uint64_t i = 1; i <= nops; i++) {
        int op;
        uint64_t u;
        char t[100];
        if (script_input) {
            int r = next_command(script_input, &op, &u, mode);
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
                key = slice_create((size_t) strlen(t), t);
                value = slice_create((size_t) strlen(t), t);
                splinterdb_insert(spl_handle, key, value);
                break;
            case 1:  // update
                key = slice_create((size_t) strlen(t), t);
                value = slice_create((size_t) strlen(t), t);
                splinterdb_insert(spl_handle, key, value);
                break;
            case 3:  // query
                splinterdb_lookup_result result;
                splinterdb_lookup_result_init(spl_handle, &result, 0, NULL);
                key = slice_create((size_t) strlen(t), t);
                value = slice_create((size_t) strlen(t), t);
                printf("\nLookup\n");
                splinterdb_lookup(spl_handle, key, &result);
                splinterdb_lookup_result_value(&result, &value);
                break;
            default:
                abort();
        }

        if (i == count_point1 || i == count_point2 || i == count_point3 ||
            i == count_point4 || i == count_point5 || i == count_point6) {
            printf("timer stop\n");
            timer_stop(&timer);

            num_of_loads_array[section_index] = splinterdb_get_num_of_loads(spl_handle);
            num_of_stores_array[section_index] = splinterdb_get_num_of_stores(spl_handle);
            splinterdb_clear_stats(spl_handle);

            double timer_s = timer * 1.0 / 1000000;
            timer_array[section_index] = timer_s;
            section_index++;
            printf("Time for phase %f\n", timer_s);
            timer = 0;
            timer_start(&timer);
        }
    }

    printf("Test PASSED\n");
    printf("######## Test result of splinterDB ########");
    double total_runtime = 0;
    uint64_t total_num_of_loads = 0;
    uint64_t total_num_of_stores = 0;

    // print the runtime for each phase
    for (uint64_t i = 0; i < num_sections; i++) {
        total_runtime += timer_array[i];
        printf("\nPhase %" PRIu64 " runtime: %f. Timer stop at the %" PRIu64 "th operation.\n",
               i + 1, timer_array[i], count_points_array[i]);

        uint64_t curr_phase_num_of_loads = num_of_loads_array[i];
        uint64_t curr_phase_num_of_stores = num_of_stores_array[i];
        total_num_of_loads += curr_phase_num_of_loads;
        total_num_of_stores += curr_phase_num_of_stores;

        printf("Number of loads: %" PRIu64 "\n", curr_phase_num_of_loads);
        printf("Number of stores: %" PRIu64 "\n", curr_phase_num_of_stores);
        printf("Total IO: %" PRIu64 "\n", curr_phase_num_of_loads + curr_phase_num_of_stores);
    }

    printf("\nTotal number of loads: %" PRIu64 "\n", total_num_of_loads);
    printf("Total number of stores: %" PRIu64 "\n", total_num_of_stores);
    printf("Total IO: %" PRIu64 "\n", total_num_of_loads + total_num_of_stores);


    return 0;
}


int main(int argc, char **argv) {
    char *script_infile = NULL;
    unsigned int random_seed = time(NULL) * getpid();
    srand(random_seed);
    int opt;
    char *term;
    int mode;
    int nops = 0;
    uint64_t num_sections = 2;
    uint64_t count_point1 = UINT64_MAX;
    uint64_t count_point2 = UINT64_MAX;
    uint64_t count_point3 = UINT64_MAX;
    uint64_t count_point4 = UINT64_MAX;
    uint64_t count_point5 = UINT64_MAX;
    uint64_t count_point6 = UINT64_MAX;

    while ((opt = getopt(argc, argv, "m:i:n:t:u:v:w:x:y:z:")) != -1) {
        switch (opt) {
            case 'm':
                if (strtoull(optarg, &term, 10) == 0) {
                    mode = YCSB;
                } else {
                    mode = CUSTOM;
                }
                break;
            case 'i':
                script_infile = optarg;
                break;
            case 'n':
                nops = strtoull(optarg, &term, 10);
                break;
            case 't':
                num_sections = strtoull(optarg, &term, 10);
                break;
            case 'u':
                count_point1 = strtoull(optarg, &term, 10);
                break;
            case 'v':
                count_point2 = strtoull(optarg, &term, 10);
                break;
            case 'w':
                count_point3 = strtoull(optarg, &term, 10);
                break;
            case 'x':
                count_point4 = strtoull(optarg, &term, 10);
                break;
            case 'y':
                count_point5 = strtoull(optarg, &term, 10);
                break;
            case 'z':
                count_point6 = strtoull(optarg, &term, 10);
                if (count_point6 != UINT64_MAX) {
                    nops = count_point6;
                }
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
    splinterdb_cfg.filename = DB_FILE_NAME;
    splinterdb_cfg.disk_size = (DB_FILE_SIZE_MB * 1024 * 1024);
    splinterdb_cfg.cache_size = (CACHE_SIZE_MB * 1024 * 1024);
    splinterdb_cfg.data_cfg = &splinter_data_cfg;

    splinterdb *spl_handle = NULL; // To a running SplinterDB instance

    int rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
    printf("Created SplinterDB instance, dbname '%s'.\n\n", DB_FILE_NAME);
    uint64_t timer = 0;
    timer_start(&timer);
    test(spl_handle, script_input, nops, num_sections,
         count_point1, count_point2, count_point3, count_point4, count_point5, count_point6, mode);
    timer_stop(&timer);
    splinterdb_print_stats(spl_handle);
    splinterdb_close(&spl_handle);
    return rc;
}