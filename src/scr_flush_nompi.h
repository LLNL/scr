#ifndef SCR_FLUSH_FILE_H
#define SCR_FLUSH_FILE_H

#include "kvtree.h"
#include "spath.h"
#include "scr_dataset.h"

void scr_flush_file_dataset_remove_with_path(
  int id,
  const spath* flush_file
);

void scr_flush_file_location_unset_with_path(
  int id,
  const char* location,
  const char* flush_file_path
);

/* write summary file for flush */
int scr_flush_summary_file(
  const scr_dataset* dataset,
  int complete,
  const char* summary_file
);

#endif
