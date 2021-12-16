#ifndef IO_H
#define IO_H

#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>

void direct_write(
    const char *diskFilePath, 
    u_int64_t offset, 
    u_int64_t size, 
    u_int64_t minOffset, 
    u_int64_t pageSize,
    u_int64_t blockSize);

void direct_read(
    const char *diskFilePath, 
    u_int64_t offset, 
    u_int64_t size, 
    u_int64_t minOffset, 
    u_int64_t pageSize,
    u_int64_t blockSize,
    bool *cacheHitFlagArray);

#endif 