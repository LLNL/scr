#define _GNU_SOURCE 1

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mpi.h"

#include "scr.h"
#include "scr_param.h"
#include "scr_globals.h"

int verbose = 0;

static int test_cfg(const char *cfg, const char *expected) {
  if (verbose) {
    fprintf(stdout, "Getting config '%s', expecting '%s'\n",
            cfg ? cfg : "(null)", expected ? expected : "(null)");
  }
  const char *val = SCR_Config(cfg);
  int rc = ((val == NULL && expected == NULL) ||
            (val != NULL && expected != NULL && 0 == strcmp(val, expected)));

  if (!rc) {
    fprintf(stderr, "Failed to get '%s'. Expected '%s' but got '%s'\n", cfg,
            expected ? expected : "(null)", val ? val : "(null)");
  } else if (verbose) {
    fprintf(stdout, "Successfully got '%s': '%s'\n",
            cfg ? cfg : "(null)", expected ? expected : "(null)");
  }

  free((void*)val);

  return rc;
}

static int test_param_get(const char *cfg, const char *expected) {
  if (verbose) {
    fprintf(stdout, "Getting parameter '%s', expecting '%s'\n",
            cfg ? cfg : "(null)", expected ? expected : "(null)");
  }
  const char *val = scr_param_get(cfg);
  int rc = ((val == NULL && expected == NULL) ||
            (val != NULL && expected != NULL && 0 == strcmp(val, expected)));

  if (!rc) {
    fprintf(stderr, "Failed to get '%s'. Expected '%s' but got '%s'\n", cfg,
            expected ? expected : "(null)", val ? val : "(null)");
  } else if (verbose) {
    fprintf(stdout, "Successfully got '%s': '%s'\n",
            cfg ? cfg : "(null)", expected ? expected : "(null)");
  }

  return rc;
}

#define test_global_var(var, expected) test_global_var_(var, #var, expected)
static int test_global_var_(int var, const char* varname, int expected) {
  if (verbose) {
    fprintf(stdout, "Getting global parameter '%s', expecting '%d'\n", varname, expected);
  }

  int rc = var == expected;

  if (!rc) {
    fprintf(stderr, "Failed to test global var '%s'. Expected '%d' but got '%d'\n", varname, expected, var);
  } else if (verbose) {
    fprintf(stdout, "Successfully got global parameter '%s': '%d'\n", varname, expected);
  }

  return rc;
}

static int test_env(const char *cfg, const char *expected) {
  if (verbose) {
    fprintf(stdout, "Getting env string '%s', expecting '%s'\n",
            cfg ? cfg : "(null)", expected ? expected : "(null)");
  }
  /* must use a unique string for the env var name since scr caches them in the
   * env hash, thankfully anything that is not a '=' or a '\0' is an allowed
   * name for an env name */
  int ierr = setenv(cfg, cfg, 0);
  assert(!ierr);
  const char *val = scr_param_get(cfg);
  ierr = unsetenv(cfg);
  assert(!ierr);
  int rc = ((val == NULL && expected == NULL) ||
            (val != NULL && expected != NULL && 0 == strcmp(val, expected)));

  if (!rc) {
    fprintf(stderr, "Failed to get '%s'. Expected '%s' but got '%s'\n", cfg,
            expected ? expected : "(null)", val ? val : "(null)");
  } else if (verbose) {
    fprintf(stdout, "Successfully got '%s': '%s'\n",
            cfg ? cfg : "(null)", expected ? expected : "(null)");
  }

  return rc;
}

int main (int argc, char* argv[])
{
  int rc = 1;
  MPI_Init(&argc, &argv);

  if (argc == 2 && strcmp(argv[1], "--verbose") == 0)
    verbose = 1;

  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* since I want to test SCR_Config, avoid loading "test.conf" */
  unsetenv("SCR_CONF_FILE");

  int tests_passed = 1;

  /* test basic parsing */
  SCR_Config("DEBUG=1");
  tests_passed &= test_cfg("DEBUG", "1");

  SCR_Config("DEBUG =1");
  tests_passed &= test_cfg("DEBUG", "1");

  SCR_Config("DEBUG= 1");
  tests_passed &= test_cfg("DEBUG", "1");

  SCR_Config("DEBUG  = 1");
  tests_passed &= test_cfg("DEBUG", "1");

  /* clean entry in case anything was read from app.conf */
  SCR_Config("STORE=");
  tests_passed &= test_cfg("STORE", NULL);

  /* set a couple of parameters to be used by SCR */
  SCR_Config("DEBUG=1");
  tests_passed &= test_cfg("DEBUG", "1");

  SCR_Config("SCR_COPY_TYPE =SINGLE");
  tests_passed &= test_cfg("SCR_COPY_TYPE", "SINGLE");

  SCR_Config("STORE= /dev/shm/foo GROUP = NODE COUNT  =1");
  tests_passed &= test_cfg("STORE= /dev/shm/foo COUNT", "1");

  SCR_Config("CKPT=0 INTERNAL=1 GROUP=NODE STORE=/dev/shm TYPE=XOR SET_SIZE=16");
  tests_passed &= test_cfg("CKPT=0 SET_SIZE", "16");

  /* check if values are all set */
  tests_passed &= test_cfg("DEBUG", "1");
  tests_passed &= test_cfg("STORE", "/dev/shm/foo");
  tests_passed &= test_cfg("STORE=/dev/shm/foo GROUP", "NODE");
  tests_passed &= test_cfg("FOOBAR", NULL);
  tests_passed &= test_cfg("CKPT=1 FOOBAR", NULL);

  /* modify values */
  SCR_Config("DEBUG=0");
  tests_passed &= test_cfg("DEBUG", "0");

  SCR_Config("STORE=/dev/shm GROUP=NODE COUNT=1");
  tests_passed &= test_cfg("STORE=/dev/shm COUNT", "1");
  tests_passed &= test_cfg("STORE=/dev/shm GROUP", "NODE");

  /* STORE has been set with both /dev/shm/foo and /dev/shm at this point,
   * so a query should print an error and return NULL */
  tests_passed &= test_cfg("STORE", NULL);

  /* delete values */
  SCR_Config("STORE=");
  tests_passed &= test_cfg("STORE", NULL);

  /* test some invalid input */
  tests_passed &= test_cfg(NULL, NULL);
  tests_passed &= test_cfg("", NULL);

  /* I cannot test results for invalid formats since SCR_Config aborts */
  /* SCR_Config(" "); */
  /* SCR_Config("KEY=="); */
  /* SCR_Config("KEY=VALUE=VALUE"); */
  /* SCR_Config("KEY VALUE"); */

  /* test setting parammeters that is not settable */
  /* need to use test_cfg here even though this (tries to) set something */
  tests_passed &= test_cfg("SCR_DB_NAME=dbname1", NULL);

  /* this and the corresponding scr_param_finalize call must surround SCR_Init */
  /* / SCR_Finalize to avoid tha parameter db being cleared by scr_finalize */
  /* before SCR_Init can use it */
  scr_param_init();

  /* test that non-settable parameters can be read from ENV vars */
  int ierr = setenv("SCR_DB_NAME", "dbname2", 1);
  assert(!ierr);
  tests_passed &= test_cfg("SCR_DB_NAME", "dbname2");

  /* test expansion of env variables */
  ierr = setenv("VAR_A", "value a", 1);
  assert(!ierr);
  ierr = setenv("VAR_B", "value b", 1);
  assert(!ierr);
  ierr = unsetenv("VAR_C");
  assert(!ierr);
  tests_passed &= test_env("$VAR_A", "value a");
  tests_passed &= test_env("${VAR_A}", "value a");
  tests_passed &= test_env("${VAR_A", "${VAR_A");
  tests_passed &= test_env("${VAR_A}>", "value a>");
  tests_passed &= test_env("$VAR_A>", "value a>");
  tests_passed &= test_env("$VAR_A ${VAR_B}", "value a value b");
  tests_passed &= test_env("$VAR_A ${VAR_B}:", "value a value b:");
  tests_passed &= test_env(":$VAR_A ${VAR_B}:", ":value a value b:");
  tests_passed &= test_env("$VAR_A ${VAR_B>}", "value a ${VAR_B>}");
  tests_passed &= test_env("$VAR_C", "");

  scr_param_finalize();

  /* test reading in values from app.conf file */
  /* I cannot use SCR_Config to test for non-existence since it will read in
   * app.conf */
  tests_passed &= test_param_get("SCR_COPY_TYPE", NULL);
  scr_param_init();
  tests_passed &= test_param_get("SCR_COPY_TYPE", "SINGLE");
  scr_param_finalize();

  /* re-enable debugging */
  SCR_Config("DEBUG=1");
  tests_passed &= test_cfg("DEBUG", "1");

  if (SCR_Init() == SCR_SUCCESS) {

    tests_passed &= test_global_var(scr_copy_type, SCR_COPY_SINGLE);
    tests_passed &= test_global_var(scr_debug, 0);

    SCR_Finalize();
  } else {
    fprintf(stderr, "Failed initializing SCR\n");
  }

  MPI_Finalize();

  rc = tests_passed ? 0 : 1;
  if (rc != 0) {
    fprintf(stderr, "%s failed", argv[0]);
  }

  return rc;
}
