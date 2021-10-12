// IO.h
#define _GNU_SOURCE

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#define BLOCKSIZE 512
#define PAGESIZE 4096

void direct_read();

void direct_write();

