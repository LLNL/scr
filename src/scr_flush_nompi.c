#include "kvtree.h"
#include "spath.h"
#include "scr_globals.h"
#include "scr_index_api.h"
#include "scr_dataset.h"

/* Remove a particular dataset from the flush file. */
void scr_flush_file_dataset_remove_with_path(int id, const spath* flush_file)
{
    kvtree* hash = kvtree_new();
    kvtree_read_path(flush_file, hash);

    /* delete this dataset id from the flush file */
    kvtree_unset_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);

    /* write the hash back to the flush file */
    kvtree_write_path(flush_file, hash);

    /* delete the hash */
    kvtree_delete(&hash);
}

void scr_flush_file_location_unset_with_path(
  int id,
  const char* location,
  const char* flush_file_path)
{
  kvtree* hash = kvtree_new();
  kvtree_read_file(flush_file_path, hash);

  /* unset the location for this dataset */
  kvtree* dset_hash = kvtree_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
  kvtree_unset_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, location);

  /* write the hash back to the flush file */
  kvtree_write_file(flush_file_path, hash);

  /* delete the hash */
  kvtree_delete(&hash);
}

/* write summary file for flush */
int scr_flush_summary_file(
  const scr_dataset* dataset,
  int complete,
  const char* summary_file)
{
  int rc = SCR_SUCCESS;

  /* create file and write header */
  mode_t mode = scr_getmode(1, 1, 0);
  int fd = scr_open(summary_file, O_WRONLY | O_CREAT | O_TRUNC, mode);
  if (fd < 0) {
    scr_err("Error opening hash file for write: %s @ %s:%d, Error: %s",
      summary_file, __FILE__, __LINE__, strerror(errno)
    );
    rc = SCR_FAILURE;
  }

  /* write data to file */
  if (fd >= 0) {
    /* create an empty hash to build our summary info */
    kvtree* summary_hash = kvtree_new();

    /* write the summary file version number */
    kvtree_util_set_int(summary_hash, SCR_SUMMARY_KEY_VERSION, SCR_SUMMARY_FILE_VERSION_6);

    /* mark whether the flush is complete in the summary file */
    kvtree_util_set_int(summary_hash, SCR_SUMMARY_6_KEY_COMPLETE, complete);

    /* write the dataset descriptor */
    kvtree* dataset_hash = kvtree_new();
    kvtree_merge(dataset_hash, dataset);
    kvtree_set(summary_hash, SCR_SUMMARY_6_KEY_DATASET, dataset_hash);

    /* write the hash to a file */
    ssize_t write_rc = kvtree_write_fd(summary_file, fd, summary_hash);
    if (write_rc < 0) {
      printf("Failure writing %s\n", summary_file);
      rc = SCR_FAILURE;
    }

    /* free the hash object */
    kvtree_delete(&summary_hash);

    /* close the file */
    scr_close(summary_file, fd);
  }

  return rc;
}
