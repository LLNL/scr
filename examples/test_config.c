#define _GNU_SOURCE 1

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mpi.h"

#include "scr.h"
#include "scr_param.h"
#include "scr_globals.h"

int verbose = 0;

static int test_cfg(const char *cfg, const char *expected, int line) {
  if (verbose) {
    fprintf(stdout, "Getting config '%s', expecting '%s' in line %d\n",
            cfg ? cfg : "(null)", expected ? expected : "(null)", line);
  }
  const char *val = SCR_Config(cfg);
  int rc = ((val == NULL && expected == NULL) ||
            (val != NULL && expected != NULL && 0 == strcmp(val, expected)));

  if (!rc) {
    fprintf(stderr,
            "Failed to get '%s'. Expected '%s' but got '%s' in line %d\n",
            cfg, expected ? expected : "(null)", val ? val : "(null)", line);
  } else if (verbose) {
    fprintf(stdout, "Successfully got '%s': '%s in line %d'\n",
            cfg ? cfg : "(null)", expected ? expected : "(null)", line);
  }

  free((void*)val);

  return rc;
}

#define test_global_var(var, expected, line) \
  test_global_var_(var, #var, expected, line)
static int test_global_var_(int var, const char* varname, int expected,
                            int line) {
  if (verbose) {
    fprintf(stdout,
            "Getting global parameter '%s', expecting '%d' in line %d\n",
            varname, expected, line);
  }

  int rc = var == expected;

  if (!rc) {
    fprintf(stderr,
            "Failed to test global var '%s'. Expected '%d' but got '%d' in line %d\n",
            varname, expected, var, line);
  } else if (verbose) {
    fprintf(stdout, "Successfully got global parameter '%s': '%d' in line %d\n",
            varname, expected, line);
  }

  return rc;
}

static int test_env(const char *cfg, const char *expected, int line) {
  if (verbose) {
    fprintf(stdout, "Getting env string '%s', expecting '%s' in line %d\n",
            cfg ? cfg : "(null)", expected ? expected : "(null)", line);
  }
  /* must use a unique string for the env var name since scr caches them in the
   * env hash, thankfully anything that is not a '=' or a '\0' is an allowed
   * name for an env name */
  setenv(cfg, cfg, 0);
  const char *val = scr_param_get(cfg);
  unsetenv(cfg);
  int rc = ((val == NULL && expected == NULL) ||
            (val != NULL && expected != NULL && 0 == strcmp(val, expected)));

  if (!rc) {
    fprintf(stderr,
            "Failed to get '%s'. Expected '%s' but got '%s' in line %d\n",
            cfg, expected ? expected : "(null)", val ? val : "(null)", line);
  } else if (verbose) {
    fprintf(stdout, "Successfully got '%s': '%s' in line %d\n",
            cfg ? cfg : "(null)", expected ? expected : "(null)", line);
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

  /* test (heuristically) that nothing is left behind */
  remove(".scr/app.conf");
  remove(".scr/test_config.d/.scr/app.conf");
  remove(".scr/test_config.d/.scr");
  remove(".scr/test_config.d/");
  mkdir(".scr", S_IRWXU);
  mkdir(".scr/test_config.d", S_IRWXU);
  tests_passed &= test_cfg("DEBUG", NULL, __LINE__);

  /* test basic parsing */
  SCR_Config("DEBUG=1");
  tests_passed &= test_cfg("DEBUG", "1", __LINE__);

  SCR_Config("DEBUG =1");
  tests_passed &= test_cfg("DEBUG", "1", __LINE__);

  SCR_Config("DEBUG= 1");
  tests_passed &= test_cfg("DEBUG", "1", __LINE__);

  SCR_Config("DEBUG  = 1");
  tests_passed &= test_cfg("DEBUG", "1", __LINE__);

  /* clean entry in case anything was read from app.conf */
  SCR_Config("STORE=");
  tests_passed &= test_cfg("STORE", NULL, __LINE__);

  /* set a couple of parameters to be used by SCR */
  SCR_Config("DEBUG=1");
  tests_passed &= test_cfg("DEBUG", "1", __LINE__);

  SCR_Config("SCR_COPY_TYPE =SINGLE");
  tests_passed &= test_cfg("SCR_COPY_TYPE", "SINGLE", __LINE__);

  SCR_Config("STORE= /dev/shm/foo GROUP = NODE COUNT  =1");
  tests_passed &= test_cfg("STORE= /dev/shm/foo COUNT", "1", __LINE__);

  SCR_Config("CKPT=0 INTERNAL=1 GROUP=NODE STORE=/dev/shm TYPE=XOR SET_SIZE=16");
  tests_passed &= test_cfg("CKPT=0 SET_SIZE", "16", __LINE__);

  /* check if values are all set */
  tests_passed &= test_cfg("DEBUG", "1", __LINE__);
  tests_passed &= test_cfg("STORE", "/dev/shm/foo", __LINE__);
  tests_passed &= test_cfg("STORE=/dev/shm/foo GROUP", "NODE", __LINE__);
  tests_passed &= test_cfg("FOOBAR", NULL, __LINE__);
  tests_passed &= test_cfg("CKPT=1 FOOBAR", NULL, __LINE__);

  /* modify values */
  SCR_Config("DEBUG=0");
  tests_passed &= test_cfg("DEBUG", "0", __LINE__);

  SCR_Config("STORE=/dev/shm GROUP=NODE COUNT=1");
  tests_passed &= test_cfg("STORE=/dev/shm COUNT", "1", __LINE__);
  tests_passed &= test_cfg("STORE=/dev/shm GROUP", "NODE", __LINE__);

  /* STORE has been set with both /dev/shm/foo and /dev/shm at this point,
   * so a query should print an error and return NULL */
  tests_passed &= test_cfg("STORE", NULL, __LINE__);

  /* delete values */
  SCR_Config("STORE=");
  tests_passed &= test_cfg("STORE", NULL, __LINE__);

  /* test some invalid input */
  tests_passed &= test_cfg(NULL, NULL, __LINE__);
  tests_passed &= test_cfg("", NULL, __LINE__);

  /* I cannot test results for invalid formats since SCR_Config aborts */
  /* SCR_Config(" "); */
  /* SCR_Config("KEY=="); */
  /* SCR_Config("KEY=VALUE=VALUE"); */
  /* SCR_Config("KEY VALUE"); */

  /* test setting parammeters that is not settable */
  /* need to use test_cfg here even though this (tries to) set something */
  tests_passed &= test_cfg("SCR_DB_NAME=dbname1", NULL, __LINE__);

  /* test that non-settable parameters can be read from ENV vars */
  setenv("SCR_DB_NAME", "dbname2", 1);
  tests_passed &= test_cfg("SCR_DB_NAME", "dbname2", __LINE__);

  /* test expansion of env variables */
  setenv("VAR_A", "value a", 1);
  setenv("VAR_B", "value b", 1);
  unsetenv("VAR_C");
  tests_passed &= test_env("$VAR_A", "value a", __LINE__);
  tests_passed &= test_env("${VAR_A}", "value a", __LINE__);
  tests_passed &= test_env("${VAR_A", "${VAR_A", __LINE__);
  tests_passed &= test_env("${VAR_A}>", "value a>", __LINE__);
  tests_passed &= test_env("$VAR_A>", "value a>", __LINE__);
  tests_passed &= test_env("$VAR_A ${VAR_B}", "value a value b", __LINE__);
  tests_passed &= test_env("$VAR_A ${VAR_B}:", "value a value b:", __LINE__);
  tests_passed &= test_env(":$VAR_A ${VAR_B}:", ":value a value b:", __LINE__);
  tests_passed &= test_env("$VAR_A ${VAR_B>}", "value a ${VAR_B>}", __LINE__);
  tests_passed &= test_env("$VAR_C", "", __LINE__);

  /* test that value has been forgotten since finalize was called */
  scr_param_finalize(); /* de-initialize all set parameters */
  tests_passed &= test_cfg("SCR_COPY_TYPE", NULL, __LINE__);
  scr_param_finalize();

  /* test setting SCR_PREFIX from various sources */
  const char usrcfgfn[] = ".scr/test_config.d/user.conf";
  FILE *usrcfg = fopen(usrcfgfn, "w");
  if (usrcfg != NULL) {
    fputs("SCR_PREFIX=.scr/test_config.d\n", usrcfg);
    fclose(usrcfg);

    /* from a user config file */
    setenv("SCR_CONF_FILE", usrcfgfn, 1);
    tests_passed &= test_cfg("SCR_COPY_TYPE", NULL, __LINE__);
    tests_passed &= test_cfg("SCR_PREFIX", ".scr/test_config.d", __LINE__);
    scr_param_finalize();
    unsetenv("SCR_CONF_FILE");

    /* from a user config file with path from app */
    SCR_Configf("SCR_CONF_FILE=%s", usrcfgfn);
    tests_passed &= test_cfg("SCR_COPY_TYPE", NULL, __LINE__);
    tests_passed &= test_cfg("SCR_PREFIX", ".scr/test_config.d", __LINE__);
    scr_param_finalize();

    /* from env overriding user config file */
    setenv("SCR_CONF_FILE", usrcfgfn, 1);
    SCR_Config("SCR_PREFIX=.");
    tests_passed &= test_cfg("SCR_COPY_TYPE", NULL, __LINE__);
    tests_passed &= test_cfg("SCR_PREFIX", ".scr/test_config.d", __LINE__);
    scr_param_finalize();
    unsetenv("SCR_CONF_FILE");
  } else {
    fprintf(stderr, "Failed to create file: %s: %s\n", usrcfgfn,
            strerror(errno));
    tests_passed = 0;
  }

  /* create some config files with different settings */
  const char usrcfgfn1[] = "user1.conf";
  usrcfg = fopen(usrcfgfn1, "w");
  if (usrcfg != NULL) {
    fputs("SCR_COPY_TYPE=PARTNER\n", usrcfg);
    fclose(usrcfg);
  } else {
    fprintf(stderr, "Failed to create file: %s: %s\n", usrcfgfn1,
            strerror(errno));
    tests_passed = 0;
  }

  /* create some config files with different settings */
  const char usrcfgfn2[] = "user2.conf";
  usrcfg = fopen(usrcfgfn2, "w");
  if (usrcfg != NULL) {
    fputs("SCR_COPY_TYPE=XOR\n", usrcfg);
    fclose(usrcfg);
  } else {
    fprintf(stderr, "Failed to create file: %s: %s\n", usrcfgfn2,
            strerror(errno));
    tests_passed = 0;
  }

  tests_passed &= test_cfg("SCR_COPY_TYPE", NULL, __LINE__);
  SCR_Configf("SCR_CONF_FILE=%s", usrcfgfn1);
  tests_passed &= test_cfg("SCR_COPY_TYPE", "PARTNER", __LINE__);
  SCR_Configf("SCR_CONF_FILE=%s", usrcfgfn2);
  tests_passed &= test_cfg("SCR_COPY_TYPE", "XOR", __LINE__);
  SCR_Config("SCR_CONF_FILE=");
  tests_passed &= test_cfg("SCR_COPY_TYPE", NULL, __LINE__);

  /* re-enable debugging */
  SCR_Config("DEBUG=1");
  tests_passed &= test_cfg("DEBUG", "1", __LINE__);

  /* test setting SCR_CACHE_BASE from with various names */
  tests_passed &= test_cfg("SCR_CACHE_BASE=/dev/shm/boring_name", NULL, __LINE__);
  tests_passed &= test_cfg("SCR_CACHE_BASE", "/dev/shm/boring_name", __LINE__);

  tests_passed &= test_cfg("SCR_CACHE_BASE==/dev/shm/name_contains_preceding_=", NULL, __LINE__);
  tests_passed &= test_cfg("SCR_CACHE_BASE", "=/dev/shm/name_contains_preceding_=", __LINE__);

  tests_passed &= test_cfg("SCR_CACHE_BASE= =/dev/shm/==name_contains_=_signs_everywhere", NULL, __LINE__);
  tests_passed &= test_cfg("SCR_CACHE_BASE", "=/dev/shm/==name_contains_=_signs_everywhere", __LINE__);

  SCR_Config("SCR_COPY_TYPE=RS");
  if (SCR_Init() == SCR_SUCCESS) {

    tests_passed &= test_global_var(scr_copy_type, SCR_COPY_RS, __LINE__);
    tests_passed &= test_global_var(scr_debug, 0, __LINE__);

    SCR_Finalize();
  } else {
    fprintf(stderr, "Failed initializing SCR\n");
    tests_passed = 0;
  }

  MPI_Finalize();

  rc = tests_passed ? 0 : 2;
  if (rc != 0) {
    fprintf(stderr, "%s failed\n", argv[0]);
  }

  return rc;
}
