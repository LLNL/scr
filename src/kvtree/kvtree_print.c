/* Utility to pretty print a hash file to the screen. */

#include "kvtree.h"
#include "kvtree_io.h"
#include "kvtree_helpers.h"
#include "kvtree.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <getopt.h>

static void print_usage(void)
{
  printf("\n");
  printf("Usage: kvtree_print [options] <file>\n");
  printf("\n");
  printf("  Options:\n");
  printf("    -m, --mode <mode>  Specify print format: \"tree\" or \"keyval\" (default tree)\n");
  printf("    -h, --help         Print usage\n");
  printf("\n");
}

int main(int argc, char* argv[])
{
  int rc = 0;

  static const char *opt_string = "m:h";
  static struct option long_options[] = {
    {"mode",    required_argument, NULL, 'm'},
    {"help",    no_argument,       NULL, 'h'},
    {NULL,      no_argument,       NULL,   0}
  };

  int usage = 0;
  char* mode = NULL;

  int long_index = 0;
  while (1) {
    int c = getopt_long(argc, argv, opt_string, long_options, &long_index);
    if (c == -1) {
      break;
    }

    switch(c) {
      case 'm':
        mode = strdup(optarg);
        break;
      case 'h':
        usage = 1;
        break;
      default:
        printf("ERROR: Unknown option: `%s'\n", argv[optind-1]);
        usage = 1;
        rc = 1;
        break;
    }
  }

  /* check that we were given exactly one filename argument */
  int numargs = argc - optind;
  if (!usage && numargs != 1) {
    printf("ERROR: Missing file name or too many files\n");
    usage = 1;
    rc = 1;
  }

  /* parse the print mode option, if one is given */
  int print_mode = KVTREE_PRINT_TREE;
  if (mode != NULL) {
    if (strcmp(mode, "tree") == 0) {
      print_mode = KVTREE_PRINT_TREE;
    } else if (strcmp(mode, "keyval") == 0) {
      print_mode = KVTREE_PRINT_KEYVAL;
    } else {
      printf("ERROR: Invalid mode name: `%s'\n", mode);
      usage = 1;
      rc = 1;
    }
    free(mode);
  }

  if (usage) {
    print_usage();
    return rc;
  }

  /* get the file name */
  char* filename = argv[optind];

  /* read in the file */
  kvtree* hash = kvtree_new();
  if (kvtree_read_file(filename, hash) == KVTREE_SUCCESS) {
    /* we read the file, now print it out */
    kvtree_print_mode(hash, 0, print_mode);
  } else {
    printf("ERROR: Failed to read file: `%s'\n", filename);
    rc = 1;
  }
  kvtree_delete(&hash);

  return rc;
}
