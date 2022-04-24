#include <sys/types.h>

/* reliable read from file descriptor (retries, if necessary, until hard error) */
int reliable_read(int fd, void* buf, size_t size);

/* reliable write to file descriptor (retries, if necessary, until hard error) */
int reliable_write(int fd, const void* buf, size_t size);

/* initialize buffer with some well-known value based on rank */
int init_buffer(char* buf, size_t size, int rank, int ckpt);

/* checks buffer for expected value set by init_buffer */
int check_buffer(char* buf, size_t size, int rank, int ckpt);

/* get size of specified file */
unsigned long get_filesize(const char* file);

/* return size of buffer used to store checkpoint timestep */
ssize_t checkpoint_timestep_size();

/* write the checkpoint data to fd, and return whether the write was successful */
int write_checkpoint(int fd, int ckpt, char* buf, size_t size);

/* read the checkpoint data from fd, and return whether the read was successful */
int read_checkpoint(int fd, int *ckpt, char* buf, size_t size);

/* read the checkpoint data from file into buf, and return whether the read was successful */
int read_checkpoint_file(char* file, int* ckpt, char* buf, size_t size);

/* check for truncation on snprintf */
int safe_snprintf(char* buf, size_t size, const char* fmt, ...);
