// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/* Read-only inspection of offline SplinterDB images. */
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "superblock.h"
#include "rc_allocator_disk.h"

static void
usage(FILE *stream, const char *program)
{
   fprintf(stream,
           "Usage: %s space IMAGE\n"
           "\n"
           "Report geometry and extent allocation in an offline image.\n"
           "The image is opened read-only; stop its writer before inspection.\n"
           "Allocation totals require a cleanly closed image. Page-type\n"
           "statistics are not stored in the image.\n"
           "\n"
           "Exit status: 0 complete report, 1 error or unavailable allocation\n"
           "totals, 2 invalid arguments.\n",
           program);
}

static platform_status
read_map(void *context, void *buffer, uint64 bytes, uint64 offset)
{
   int    fd   = *(int *)context;
   size_t done = 0;
   while (done < bytes) {
      ssize_t result =
         pread(fd, (char *)buffer + done, bytes - done, offset + done);
      if (result < 0) {
         if (errno == EINTR) {
            continue;
         }
         fprintf(stderr, "Cannot read allocation map: %s\n", strerror(errno));
         return STATUS_IO_ERROR;
      }
      if (result == 0) {
         fprintf(stderr, "Cannot read allocation map: image is truncated\n");
         return STATUS_IO_ERROR;
      }
      done += result;
   }
   return STATUS_OK;
}

static int
print_space(int fd, const superblock *image)
{
   disk_geometry            geometry = image->geometry;
   rc_allocator_disk_layout layout;
   platform_status          status =
      rc_allocator_disk_compute_layout(&geometry, &layout);
   if (!SUCCESS(status)) {
      fprintf(stderr, "Invalid allocator geometry\n");
      return EXIT_FAILURE;
   }
   uint64 extent_count = layout.extent_count;

   printf("Format version: %lu\n", image->format_version);
   printf("Superblock generation: %lu\n", image->generation);
   printf("Allocator format version: %u\n", image->allocator_format_version);
   printf("\nGeometry:\n");
   printf("  Capacity:         %lu bytes\n", geometry.disk_size);
   printf("  Page size:        %lu bytes\n", geometry.page_size);
   printf("  Extent size:      %lu bytes\n", geometry.extent_size);
   printf("  Pages per extent: %lu\n",
          geometry.extent_size / geometry.page_size);
   printf("  Total pages:      %lu\n", geometry.disk_size / geometry.page_size);
   printf("  Total extents:    %lu\n", extent_count);
   printf("\nPage-type statistics: not stored in the image\n");

   if (image->allocator_format_version != RC_ALLOCATOR_DISK_VERSION) {
      fprintf(stderr, "Unsupported allocator format version\n");
      return EXIT_FAILURE;
   }
   printf("\nAllocation:\n");
   if (image->allocation_state_addr == 0) {
      printf("  Totals unavailable: allocation map requires recovery.\n");
      return EXIT_FAILURE;
   }
   /* The current format stores the refcount map starting at extent 1. */
   if (image->allocation_state_addr != geometry.extent_size) {
      fprintf(stderr,
              "Invalid allocation map address: %lu\n",
              image->allocation_state_addr);
      return EXIT_FAILURE;
   }

   struct stat statbuf;
   if (fstat(fd, &statbuf) != 0) {
      fprintf(stderr, "Cannot stat image: %s\n", strerror(errno));
      return EXIT_FAILURE;
   }
   /* Images can grow lazily, so do not require their full capacity at EOF. */
   if (S_ISREG(statbuf.st_mode)
       && (uint64)statbuf.st_size
             < image->allocation_state_addr + layout.fixed_bytes)
   {
      fprintf(stderr, "Cannot read allocation map: image is truncated\n");
      return EXIT_FAILURE;
   }

   rc_allocator_disk_summary summary;
   status = rc_allocator_disk_read(
      &geometry, PROCESS_PRIVATE_HEAP_ID, read_map, NULL, NULL, &fd, &summary);
   if (!SUCCESS(status)) {
      fprintf(stderr,
              "Invalid allocation map: %s\n",
              platform_status_to_string(status));
      return EXIT_FAILURE;
   }
   uint64 allocated = summary.allocated;

   uint64 free_extents = extent_count - allocated;
   printf("  Allocated:        %lu extents, %lu bytes (%.2f%%)\n",
          allocated,
          allocated * geometry.extent_size,
          100.0 * allocated / extent_count);
   printf("  Free:             %lu extents, %lu bytes (%.2f%%)\n",
          free_extents,
          free_extents * geometry.extent_size,
          100.0 * free_extents / extent_count);
   printf("  Fixed reserved metadata (included in allocated): %lu extents, %lu "
          "bytes\n",
          summary.reserved,
          summary.reserved * geometry.extent_size);
   printf("  Allocation overflow chain (included in allocated): "
          "%lu extents, %lu bytes\n",
          summary.chain_extents,
          summary.chain_extents * geometry.extent_size);
   printf("  Overflow refcount records: %lu\n", summary.overflow_records);
   printf(
      "  Allocation is measured in whole extents, including unused pages.\n");
   return EXIT_SUCCESS;
}

int
main(int argc, char **argv)
{
   if (argc == 2
       && (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0))
   {
      usage(stdout, argv[0]);
      return EXIT_SUCCESS;
   }
   if (argc != 3 || strcmp(argv[1], "space") != 0) {
      usage(stderr, argv[0]);
      return 2;
   }

   const char *filename = argv[2];
   int         fd       = open(filename, O_RDONLY | O_CLOEXEC);
   if (fd < 0) {
      fprintf(stderr, "Cannot open '%s': %s\n", filename, strerror(errno));
      return EXIT_FAILURE;
   }

   superblock      image;
   platform_status rc     = superblock_read_bootstrap(filename, &image);
   int             result = EXIT_FAILURE;
   if (SUCCESS(rc)) {
      printf("Image: %s\n", filename);
      result = print_space(fd, &image);
   } else {
      fprintf(stderr,
              "Cannot inspect '%s': no valid superblock for this "
              "format version\n",
              filename);
   }
   if (close(fd) != 0) {
      fprintf(stderr, "Cannot close '%s': %s\n", filename, strerror(errno));
      result = EXIT_FAILURE;
   }
   if (fflush(stdout) != 0) {
      fprintf(stderr, "Cannot write report: %s\n", strerror(errno));
      result = EXIT_FAILURE;
   }
   return result;
}
