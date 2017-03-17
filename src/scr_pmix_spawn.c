/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file is part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
 */

/* All rights reserved. This program and the accompanying materials
 * are made available under the terms of the BSD-3 license which accompanies this
 * distribution in LICENSE.TXT
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the BSD-3  License in
 * LICENSE.TXT for more details.
 *
 * GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform,
 * display, or disclose this software are subject to the terms of the BSD-3
 * License as provided in Contract No. B609815.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 *
 * Author: Christopher Holguin <christopher.a.holguin@intel.com>
 *
 * (C) Copyright 2015-2016 Intel Corporation.
 */

/* scr_pmix_spawn.c
 * command line program that launches any executable using PMIx APIs
 * must have orte-dvm running and "submit it" with orterun -
 * it will not boostrap itself.  Essentially treat it like srun
 * or aprun - they both depend on their job launchers up and running
 * on the nodes before they can properly be used for launching
 * executables
 */

/* Please note a planned code cleanup task is in place for the command line 
 * options */

#if (SCR_MACHINE_TYPE == SCR_PMIX)
#define _GNU_SOURCE
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

#include "pmix.h"
#include "pmix_tool.h"
#include "pmix_common.h"
static pmix_proc_t main_proc;
static pmix_proc_t tool_proc;
static volatile bool done_flag = false;
static volatile bool experimental = false;
char hostn[500];
int errhandler_ref_number = 0;
int _g_errhandler_ref;
int _g_counter = 0;
static bool verbose_print;

static void error_helper(pmix_status_t status, char *host, char *note)
{
        printf("note: \"%s\", rank %d, host %s, status number: %d\n",
               note,
               main_proc.rank,
               host,
               status);
}

/* generic error handling callback function */
static void errhandler_cb(size_t evtHdlr_reg_id,
                          pmix_status_t status,
                          const pmix_proc_t *source,
                          pmix_info_t info[], size_t ninfo,
                          pmix_info_t *results, size_t nresults,
                          pmix_event_notification_cbfunc_fn_t cbfunc,
                          void *cbdata)
{
        int i;

        printf("\n------Master spawn proc %s:%d NOTIFIED!!! with status %d, results #%d, ninfo #%d, called %d times by rank: %d \n",
               main_proc.nspace,
               main_proc.rank,
               status,
               (int)nresults,
               (int)ninfo,
               ++_g_counter,
               source->rank);


        for(i = 0; i < ninfo; i++){
                printf("(%d) info: key %s, value type %d, value %x\n",
                       i,
                       info[i].key,
                       info[i].value.type,
                       (unsigned int)info[i].value.data.uint64);
        }

        for(i = 0; i < nresults; i++){
                printf("(%d) info: key %s, value type %d, value %x\n",
                       i,
                       results[i].key,
                       results[i].value.type,
                       (unsigned int)results[i].value.data.uint64);
        }
        fflush(stdout);

        if(experimental == true){
                done_flag = true;
        }

        /* indicate that we handled the notification */
        if (NULL != cbfunc) {
                cbfunc(PMIX_SUCCESS, NULL, 0, NULL, NULL, cbdata);
        }
}


/* callback for when the error handler is registered */
static void errhandler_reg_callbk (pmix_status_t status,
                                    size_t errhandler_ref,
                                   void *cbdata)
{
        _g_errhandler_ref = errhandler_ref;
        if(verbose_print)error_helper(status, hostn,"error handler registered callback:");
}

/* fence_helper declaration  */
int fence_helper(void);

void print_usage(char *exe)
{
        printf("Usage: %s [options] program_to_spawn [cmd line arguments for program_to_spawn]\n\
          options: -n <number of processing elements>\n\
                   -N <number of processing elements per node>\n\
                   -L <node list in CSV format>\n\
                   -x <environment variable, e.g. 'PATH'> - export a specified environment variable THAT EXISTS IN THE CURRENT ENVIRONMENT to <program_to_spawn>\n\
                   -x SCR - check for any SCR environment variables in the current environment,\
                   and if they're defined, pass them to <program_to_spawn>\n\
                   -p pmix mode - the spawned process is expected to call PMIx_spawn()\n\
                   -P non-pmix mode (default behavior) - the spawned process is\
                   not expected to call PMIx_spawn()\n\
                   the above two switches, -p and -P are mutually exclusive.\n\
                   TODO HACK IMPLEMENTED-b non-blocking mode - spawn <program_to_spawn> and exit, returning pid\n\
                   -B blocking mode (default default behavior) - spawn <program_to_spawn> and block until spawned app returns\n\
                   the above two switches, -b and -B are mutually exclusive.\n\
                   -v verbose debug printing\n\
                   -h this help message\n", exe);
}

#ifndef PMIX_FWD_STDOUT
#define PMIX_FWD_STDOUT "OOPS_OLD_PMIXSTDOUT"
#endif

#ifndef PMIX_FWD_STDERR
#define PMIX_FWD_STDERR "OOPS_OLD_PMIX_STDERR"
#endif

int spawn_process(const int spawned_argc,
                  const char **spawned_argv,
                  const int spawned_env_vars_count,
                  const char **spawned_env_vars,
                  const int size_of_job_info_array,
                  pmix_info_t **info_array_for_job,
                  const int size_of_array_for_app,
                  pmix_info_t **info_array_for_app,
                  int *returned_pid);

static void parse_all_scr_envs(char ***env_array, const char **_environ);
static void append_to_env_array(const char *keyandval, char ***env_array);
static void printer_func(char **array);
static void finalize_array(char **array);
static void handle_standard_env_var(const char *name, char ***env_array);
/* TODO: write this function with the following signature to cleanup the code
 * in main() */
static void append_to_info_array(const char *key, void *value,
                                 pmix_info_t **info_array);

const char *SCR_STRING = "SCR";

int main(int argc, char **argv, const char **environ)
{

        pmix_status_t rc;

        pmix_info_t *info = NULL;

        bool flag;

        pmix_status_t retval;
        pmix_app_t *spawned_app = NULL;

        pmix_info_t *job_info = NULL;
        pmix_info_t *proc_info = NULL;
        int job_info_count = 0;
        int job_info_index = 0;
        int proc_info_count = 0;
        int proc_info_index = 0;

        char spawned_nsp[PMIX_MAX_NSLEN+1];
        char *path_to_app = NULL;
        char *host_to_use = NULL;
        int number_of_clients = 0;
        int temp_counter = 0;
        done_flag = false;
        gethostname(hostn, 500);
        int spawned_app_argc = 0;

        char **scr_environ = NULL;

        int proc_count = 1;
        int node_count = 0;
        bool blocking_mode = true;
        char *node_list = NULL;
        bool forward_all_scr_envs = false;
        bool pmix_mode = false;


        const char *optstring = "+n:N:L:x:bB:pPvhe";
        int temp_slen=0;
        /* todo: add arg parsing with ompi schizo */
        verbose_print = false;

        int sleep_max = 30;
        const int fixed_sleep = 5;
        int c;
        while((c = getopt(argc, argv, optstring)) != -1){
                switch(c){
                case 'h':
                        print_usage(argv[0]);
                        exit(0);
                        break;
                case 'n':
                        proc_count = atoi(optarg);
                        if(proc_count <= 0 || proc_count > 100){
                                printf("outside the range of allowable instances to spawn [1-100]\n");
                                exit(1);
                        }
                        if(verbose_print) {
                                printf("proc_count = %d\n", proc_count);
                        }
                        break;
                case 'N':
                        /* node_count = atoi(optarg); */
                        node_count = 1;
                        if(verbose_print) {
                                printf("node_count = %d\n", node_count);
                        }
                        break;
                case 'B':
                        blocking_mode = true;
                        sleep_max = atoi(optarg);
                        if(sleep_max < 0){
                                printf("can't sleep for less than 0 seconds\n");
                                exit(1);
                        }
                        if(verbose_print){
                                printf("blocking mode = %x\n", blocking_mode);
                        }
                        break;
                case 'b':
                        blocking_mode = false;
                        if(verbose_print){
                                printf("blocking mode = %x\n", blocking_mode);
                        }
                        break;
                case 'L':
                        node_list = optarg;
                        host_to_use = node_list;
                        if(verbose_print){
                                printf("node_list = '%s'\n", node_list);
                        }
                        break;
                case 'x':
                        temp_slen = strlen(optarg);
                        /*  check if the string is the same length as 'SCR', if so compare them */
                        if(temp_slen == strlen(SCR_STRING)){
                                if(strncmp(optarg, SCR_STRING, strlen(SCR_STRING)) == 0){
                                        /* if the string is SCR, then forward all SCR related env vars */
                                        if(verbose_print) printf("all scr envs will be forwarded\n");
                                        forward_all_scr_envs = true;
                                }
                                else{
                                        /* handled like a normal env var */
                                        handle_standard_env_var(optarg, &scr_environ);
                                }
                        }
                        else{
                                /*handled like a normal env var */
                                handle_standard_env_var(optarg, &scr_environ);
                        }
                        break;
                case 'v':
                        verbose_print = true;
                        break;
                case 'p':
                        pmix_mode = true;
                        if(verbose_print){
                                printf("pmix_mode = %x\n", pmix_mode);
                        }
                        break;
                case 'P':
                        pmix_mode = false;
                        if(verbose_print){
                                printf("pmix_mode = %x\n", pmix_mode);
                        }
                        break;
                case 'e':
                        experimental = true;
                        break;
                case '?':
                        printf("missing a required argument or invalid option: %x\n", optopt);
                        print_usage(argv[0]);
                        exit(1);
                        break;
                default:
                        printf("Unrecognized argument: %c\n", c);
                        print_usage(argv[0]);
                        exit(1);
                        break;
                }
        }

        /* number of instances to spawn */
        number_of_clients = proc_count;


        /* check to make sure an application was specified to launch */
        if( optind < argc ){
                /* if optind is < argc, it means there is at least one more arg
                 * beyond the args for this program */
                path_to_app = argv[optind];
                spawned_app_argc = argc - optind;
                if(verbose_print) {
                        printf("app to launch: %s @ %s:%d\n",
                               path_to_app, __FILE__, __LINE__);
                }
        }
        else{
                printf("program_to_spawn option was not provded\n");
                print_usage(argv[0]);
                exit(1);
        }

        if(verbose_print){
                printf("master process will spawn %d instances; app to run: %s\n\n",
                       number_of_clients, path_to_app);
                printf("pmix version: %s (host: %s)\n", PMIx_Get_version(), hostn);
        }
        /* init pmix */
        retval = PMIx_Init(&main_proc, NULL, 0);
        if(retval != PMIX_SUCCESS){
                error_helper(retval, hostn, "error initializing pmix");
                exit(0);
        }

        if(verbose_print){
                printf("rank %d, host '%s', nspace: '%s' init'd pmix succesfully\n\n",
                       main_proc.rank, hostn, main_proc.nspace);
        }



        /* we need to attach to a "system" PMIx server so we
         * can ask it to spawn applications for us. There can
         * only be one such connection on a node, so we will
         * instruct the tool library to only look for it */
        int ninfo = 1;
        PMIX_INFO_CREATE(info, ninfo);
        flag = true;
        PMIX_INFO_LOAD(&info[0], PMIX_CONNECT_TO_SYSTEM, &flag, PMIX_BOOL);

        /* initialize the library and make the connection */
        if (PMIX_SUCCESS != (rc = PMIx_tool_init(&tool_proc, NULL, 0 ))) {
                fprintf(stderr, "PMIx_tool_init failed: %d\n", rc );
                exit(rc);
        }
        if (0 < ninfo) {
                PMIX_INFO_FREE(info, ninfo);
        }




        /* first call fence to sync all processes */
        retval = fence_helper();
        if(retval != PMIX_SUCCESS)
        {
                error_helper(retval, hostn, "error fencing");
                exit(retval);
        }

        /* Process SCR env vars if needed */

        if(forward_all_scr_envs){
                parse_all_scr_envs(&scr_environ, environ);
        }

        /* finalize the env array so a NULL is in place */
        finalize_array(scr_environ);

        /* Setup info structs to pass to this: */
        /* pmix_info_t *error_info = NULL; */
        /*  PMIX_INFO_CREATE(error_info, 1); */
        /*
          strncpy(error_info[0].key, PMIX_ERROR_GROUP_ABORT, PMIX_MAX_KEYLEN);
          error_info[0].value.type = PMIX_BOOL;
          error_info[0].value.data.flag = true;
        */

        /*  strncpy(error_info[0].key, PMIX_ERROR_GROUP_SPAWN, PMIX_MAX_KEYLEN);
            int t_val = 1;
            pmix_value_load(&error_info[1].value, &t_val, PMIX_BOOL);
        */

        /*error_info[1].value.type = PMIX_BOOL;
        error_info[1].value.data.flag = true; */

        /*  strncpy(error_info[2].key, PMIX_ERROR_GROUP_GENERAL, PMIX_MAX_KEYLEN);
            error_info[2].value.type = PMIX_BOOL;
            error_info[2].value.data.flag = true;
        */


        /* TODO: setup error handling when implemented in pmix with the
         * following error codes: */
        /*
        pmix_status_t registered_codes[5];
        registered_codes[0] = PMIX_ERR_JOB_TERMINATED;
        registered_codes[1] = PMIX_ERR_PROC_ABORTED;
        registered_codes[2] = PMIX_ERR_PROC_ABORTING;
        */
        PMIx_Register_event_handler(NULL, 0,
                                    NULL, 0,
                                    errhandler_cb,
                                    errhandler_reg_callbk,
                                    (void *) NULL);

        /*  PMIX_INFO_DESTRUCT(error_info); */

        /* allocate memory to hold the spawend app struct */
        PMIX_APP_CREATE(spawned_app, 1);

        /* maxprocs isn't documented very well, but it appears to control
         * how many instances of the spanwed app are created */
        spawned_app->maxprocs = number_of_clients;

        /* set the app to run */
        (void)asprintf(&spawned_app->cmd, "%s", path_to_app);

        /* set argv for spawned app starting with remaining argv  */
        spawned_app->argv = &argv[optind];

        /* set the environment pointer */
        spawned_app->env = scr_environ;

        /*--START: add all proc level infos */

        /* add things to the proc level info */
        if(!pmix_mode){
                job_info_count++;
        }

        if(host_to_use != NULL){
                proc_info_count++;
        }

        if(verbose_print){
                printf("enabling debug feature for forwarding stdout/stderr\n");
                proc_info_count+=2;
                /* add PMIX_FWD_STDOUT and PMIX_FWD_STDERR later*/

        }

        if(experimental){
                job_info_count++;
        }
        if(node_count == 1){
                job_info_count++;
        }
        /*--END: add all proc level infos */


        /*--START: append actual proc level info */
        PMIX_INFO_CREATE(job_info, job_info_count);
        PMIX_INFO_CREATE(proc_info, proc_info_count);
        /* PMIX_VAL_set_assign(_v, _field, _val )  */
        /* PMIX_VAL_set_strdup(_v, _field, _val )  */

        if(host_to_use != NULL){
                /* add info struct to the spawned app itself for the host */

                /* old way */
                strncpy(proc_info[proc_info_index].key, PMIX_HOST, PMIX_MAX_KEYLEN);
                //proc_info[proc_info_index].value.type = PMIX_STRING;
                /* set the data for host list to use */
                //proc_info[proc_info_index].value.data.string = host_to_use;
                /* end old way */
                if(verbose_print) printf("about to set host val\n");
                PMIX_VAL_SET(&(proc_info[proc_info_index].value), string,
                                    host_to_use );
                proc_info_index++;
        }

        if(!pmix_mode){
                strncpy(job_info[job_info_index].key, PMIX_NON_PMI,
                        PMIX_MAX_KEYLEN);
                if(verbose_print) printf("about to set non pmix flag\n");
                PMIX_VAL_SET(&(job_info[job_info_index].value), flag, true);
                job_info_index++;
        }
        if(verbose_print){
                strncpy(proc_info[proc_info_index].key, PMIX_FWD_STDOUT,
                        PMIX_MAX_KEYLEN);
                if(verbose_print) printf("about to set stdout flag\n");
                PMIX_VAL_SET(&(proc_info[proc_info_index].value), flag, true );
                proc_info_index++;

                strncpy(proc_info[proc_info_index].key, PMIX_FWD_STDERR,
                        PMIX_MAX_KEYLEN);
                if(verbose_print) printf("about to set stderr flag\n");
                PMIX_VAL_SET(&(proc_info[proc_info_index].value), flag, true );
                proc_info_index++;
        }
        if(experimental){
                printf("attempting to perform experiment\n");
                bool local_flag = true;
                PMIX_INFO_LOAD(&job_info[job_info_index], PMIX_NOTIFY_COMPLETION, &local_flag, PMIX_BOOL);
                job_info_index++;
        }
        if(node_count == 1){
                strncpy(job_info[job_info_index].key, PMIX_PPR,
                        PMIX_MAX_KEYLEN);
                PMIX_VAL_SET(&(job_info[job_info_index].value), string,
                             "1:n");
                job_info_index++;
        }
        /*--END: append actual proc level info */

        /* sanity check to make sure we covered all the info structs */
        if(proc_info_index != proc_info_count ){
                printf("bug: mismatch with appending proc info\n");
                exit(1);
        }
        if(job_info_index != job_info_count){
                printf("bug: mismatch with appending job info\n");
                exit(1);
        }

        /* TODO: TEST PMIX_NOTIFY_COMPLETION WHEN IT'S IMPLEMENTED IN PMIX */

        /* fill in job_info */
        /*
        strncpy(job_info[0].key, PMIX_TIMEOUT, PMIX_MAX_KEYLEN);
        job_info[0].value.type = PMIX_INT;
        job_info[0].value.data.integer = 10; */

        /* strncpy(job_info[0].key, PMIX_NOTIFY_COMPLETION, PMIX_MAX_KEYLEN);
           job_info[0].value.type = PMIX_BOOL;
           job_info[0].value.data.flag = true; */

        /*strncpy(spawned_app->info[0].key, PMIX_DISPLAY_MAP, PMIX_MAX_KEYLEN);
          job_info[0].value.type = PMIX_BOOL;
          job_info[0].value.data.flag = true;*/


        /* TODO: TEST PMIX_NOTIFY_COMPLETION WHEN IT'S IMPLEMENTED IN PMIX */
        spawned_app->info = proc_info;
        spawned_app->ninfo = proc_info_count;

        if(verbose_print){
                printf("proc level info count: %d\n", proc_info_count);
        }
        /* call spawn */
        retval = PMIx_Spawn(job_info, job_info_count,
                            spawned_app, 1, spawned_nsp);

        if(verbose_print) {
                printf("rank %d (host %s) just called spawn; spawned nspace: %s, retval:%d\n",
                       main_proc.rank,
                       hostn,
                       spawned_nsp,
                       retval);
        }
        if(retval != PMIX_SUCCESS){
                error_helper(retval,  hostn, "error with spawn");
                goto done;
        }

        /* TODO: TEMPORARY WORKAROUND TO WAIT FOR A SPAWNED PROCESS */
        if(blocking_mode){

                sleep(fixed_sleep);

                /* wait until app completes: */
                while(!done_flag){
                        sleep(fixed_sleep);
                        temp_counter++;
                        if(temp_counter*fixed_sleep >= sleep_max) {
                                if(verbose_print) printf("broke out early\n");
                                break;
                        }
                }
                if(verbose_print){
                        if(done_flag == true) {
                                printf("done_flag was set to true!\n");
                        }
                }

        }

done:
        /* fence first */
        retval = fence_helper();
        if(retval != PMIX_SUCCESS){
                if(verbose_print) printf("error fencing, finalize may fail ! \n");
        }
        /* finalize */

        PMIx_Deregister_event_handler(_g_errhandler_ref, NULL, NULL);

        if(verbose_print){
                fprintf(stdout,
                        "spawn master process (rank %d) (host %s) finalizing\n",
                        main_proc.rank,
                        hostn);
        }

        /* clean up pmix */

        retval = PMIx_tool_finalize();

        if(retval == PMIX_SUCCESS)
        {
                if(verbose_print){
                        printf("spawn master process %d finalize success\n\n",
                               main_proc.rank);
                }
        }
        else
        {
                printf("spawn master process %d pmix_finalize FAILURE: %d\n\n",
                       main_proc.rank,
                       retval);
        }

        retval = PMIx_Finalize(NULL, 0);
        fflush(stdout);

        /*  cleanup before returning */
        PMIX_INFO_FREE(job_info, job_info_count);
        spawned_app->argv = NULL;
        PMIX_APP_FREE(spawned_app, 1);
        if(verbose_print) printf("%s exiting cleanly :)\n", argv[0]);
        return 0;

}

/*  simply a wrapper around PMIx_fence to help with redundant code */
int fence_helper(void)
{

        int retval;


        retval = PMIx_Fence(NULL, 0, NULL, 0);

        if(retval != PMIX_SUCCESS)
        {
                printf("failure fencing: %d, rank: %d\n", retval, main_proc.rank);
        }

        return retval;

}

/* TODO placeholder to wrap all of the spawn logic into a function */
int spawn_process(const int spawned_argc,
                  const char **spawned_argv,
                  const int spawned_env_vars_count,
                  const char **spawned_env_vars,
                  const int size_of_job_info_array,
                  pmix_info_t **info_array_for_job,
                  const int size_of_array_for_app,
                  pmix_info_t **info_array_for_app,
                  int *returned_pid)
{

        return -1;

}

/* parses the current environment variables and looks for "SCR" at he beginning
 * if SCR is at the beginning of the name, then it appends to the array */
void parse_all_scr_envs(char ***env_array, const char **_environ)
{
        /* loop through the entire env array  */
        char *current_keyval = NULL;
        int i=0;
        char *found_string = NULL;

        while(_environ[i] != NULL){
                /* increment and set pointer */
                current_keyval = strdup(_environ[i]);
                i++;
                /* search for SCR in each keyval pair  */
                found_string = strstr(current_keyval, SCR_STRING);
                if(found_string == NULL){
                        free(current_keyval);
                        continue;
                }
                /* ensure SCR appears only at the beginning of the string
                 * by simply comparing the pointer memory values */
                if(found_string == current_keyval){
                        /* add to env array */
                        if(verbose_print) printf("keyval '%s' found and now appending\n",
                                                 current_keyval);

                        append_to_env_array(current_keyval, env_array);
                        free(current_keyval);
                }
                else{
                        /* explicitly continue if SCR is not at the beginning  */
                        free(current_keyval);
                        continue;
                }
        }
        if(verbose_print){
                printf("done searching and appending for scr env keyvals \n");
        }
}

/* given a key val string in the form of "KEY=VAL", append to the current
 * array of environment variables for later use to be passed to pmix spawn */
void append_to_env_array(const char *keyandval, char ***env_array)
{
        int alloced_size = 0;
        int first_null_index = -1;
        int new_size = 0;
        int i = 0;
        char **temp_array = NULL;
        char **env_deref = NULL;

        /* check inputs */
        if(keyandval == NULL || env_array == NULL){
                printf("either the key/val pair was null or the env_array was null\n");
                return;
        }

        /* dereference the triple pointer to make it cleaner to work with */
        env_deref = *env_array;

        /* firt check for NULL to see if it has ever been allocated space */
        if(env_deref == NULL){
                if(verbose_print) printf("first call to append: allocating 2 slots \n");
                /* allocate memory if it's null */
                env_deref = (char **)calloc(2, sizeof(char** ));
                if(env_deref == NULL){
                        printf("error allocating memory for env_deref\n");
                        exit(-1);
                }

                /* set the sentinel to detect array length */
                env_deref[1] = (char *)0xdeadbeef;

                /* set the first keyval pair */

                env_deref[0] = strdup(keyandval);

                *env_array = env_deref;

                if(verbose_print) printer_func(*env_array);
                return;
        }

        /* check size of env_array */
        while((uintptr_t)env_deref[alloced_size] != 0xdeadbeef){

                if(env_deref[alloced_size] == NULL && first_null_index == -1){
                        first_null_index = alloced_size;
                }

                alloced_size++;
        }
        if(verbose_print){
                printf("---debug note: alloced_size is :%d, and first_null is %d\n",
                       alloced_size,
                       first_null_index);
        }

        if(alloced_size < 1 ){
                /* this should never happen,
                 * it means the caller used the function improperly */
                printf("the alloced_size was less than 1, which means the array never got properly allocated\n");
                exit(-1);
        }
        /* the alloced_size variable is zero based, so add one to it */
        alloced_size++;

        /* allocate necessary space for env_array */
        if(first_null_index == -1){
                /* out of space, so need to allocate more */
                /* always double the space allocated to reduce calls to calloc */
                new_size = alloced_size * 2;
                temp_array = (char** )calloc(new_size, sizeof(char **));
                if(temp_array == NULL){
                        printf("error allocating memory \n");
                        exit(1);
                }
                /* set the new sentinel location */
                temp_array[new_size - 1] = (char *)0xdeadbeef;

                /* copy contents to new array - they're just pointers, so set them */
                /* use alloced_size-1 because don't want to copy the old sentinel */
                for(i=0; i < (alloced_size-1); i++){
                        temp_array[i] = env_deref[i];
                }
                temp_array[alloced_size-1] = NULL;
                /* free old array only, not the pointers in it */
                free(env_deref);

                /* set the new keyval pair - alloced_size is the index we need
                 * but have to subtract 1 because it was incremented earlier
                 */
                temp_array[alloced_size-1] = strdup(keyandval);

                /* set the newly alloced array to the passed in env array */
                *env_array = temp_array;
        }
        else{
                /* have enough space, just need copy new keyandval */
                /* first do a sanity check to make sure the indices are in order */
                if(first_null_index >= alloced_size){
                        printf("something went wrong with the algorithm!!\n");
                        return;
                }

                /* now set the new value */
                env_deref[first_null_index] = strdup(keyandval);

                *env_array = env_deref;
        }

        if(verbose_print) printer_func(*env_array);
}


void printer_func(char **array)
{

        int i=0;
        if(array == NULL || *array == NULL){
                if(verbose_print) printf("array is null\n");
                return;
        }
        if(verbose_print) printf("\n");
        while((uintptr_t)array[i] != 0xdeadbeef){
                if(verbose_print) printf("%d: val '%s'\n", i, array[i]);
                i++;
        }
        if(verbose_print) printf("\n");

}

/* specialized free function */

/* check to make sure the NULL is placed correctly */
void finalize_array(char **array)
{

        int i=0;
        int flag = -1;

        if(array == NULL){
                return;
        }

        while((uintptr_t)array[i] != 0xdeadbeef){
                if(array[i] == NULL){
                        flag = i;
                }
                i++;
        }

        if(flag == -1){
                /* deadbeef sentinel is at the end of the array
                 * simlply need to set it to NULL instead
                 */
                array[i] = NULL;
        }
        /* else case just means there is a null already in the array
         * it doesn't need to be handled explicitly*/
}

/* utility to append any env var ALREADY DEFINED to the env var array */
static void handle_standard_env_var(const char *name, char ***env_array)
{

        char *value = NULL;
        char *concatenated_string = NULL;
        int str_size=0;

        /* get the env var value from the environment*/
        value = getenv(name);

        /* if it's null, don't append it */
        if(value == NULL){
                return;
        }

        /* create the appended string */
        str_size = asprintf(&concatenated_string, "%s=%s", name, value);

        /* sanity check */
        if(str_size <= 0){
                printf("failed concatenating strings '%s:%s'\n", name, value);
                exit(0);
        }
        /*add it to the array */
        append_to_env_array(concatenated_string, env_array);

        /* free the string */
        free(concatenated_string);
}

#endif
