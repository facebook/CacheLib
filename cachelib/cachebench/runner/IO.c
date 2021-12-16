#define _GNU_SOURCE
#include "IO.h"

void direct_write(
    const char *diskFilePath, 
    u_int64_t offset, 
    u_int64_t size, 
    u_int64_t minOffset, 
    u_int64_t pageSize,
    u_int64_t blockSize) {

    mode_t mode = 0644;
    u_int64_t fd = open(diskFilePath, O_RDWR | O_DIRECT, mode);
    if (fd == -1) 
        perror("Opening the disk file failed in direct_write!");

    // if the start offset of write is not aligned with the page size,  
    // you read the part between the start of the page and the start of write request 
    // For page size 4k, if write starts at offset 2k then the first 2k has to be read 
    // and loaded to the cache 
    if (offset % pageSize > 0) {
        u_int64_t pageStartOffset = offset - (offset % pageSize);
        u_int64_t readSize = offset - pageStartOffset;   
        if (readSize > 0) {
            if (lseek(fd, pageStartOffset-minOffset, SEEK_SET) == -1) {
                fprintf(stderr, 
                    "Error seeking to %ld in disk file to read the page when write start byte \\
                    not aligned \n", pageStartOffset-minOffset);
                perror("");
            }
            void *buffer = malloc(readSize);
            if (posix_memalign((void **)&buffer, blockSize, readSize)) 
                perror("Error in memealign when reading the page when write start byte \\
                    not aligned \n");
            if (read(fd, buffer, readSize) == -1) 
                perror("Error reading the disk file when reading the page when write start byte \\
                    not aligned \n");
            free(buffer);
        }
    }

    // write 
    if (lseek(fd, offset-minOffset, SEEK_SET) == -1) {
        fprintf(stderr, "Error seeking to %ld in disk file for direct write\n", 
            offset-minOffset);
        perror("");
    }
    void *buffer = malloc(size);
    if (posix_memalign((void **)&buffer, blockSize, size)) 
        perror("Error in memealign for direct write\n");
    if (write(fd, buffer, size) == -1) 
        perror("Error writing to disk file using direct IO \n");
    free(buffer);

    // if the end offset of write is not aligned with the page size,  
    // you read the part between the end offset and the end offset of the page 
    // For page size 4k, if write ends at offset 2k then the part from 2k-4k 
    // has to be read and loaded to the cache 
    u_int64_t endOffset = offset+size;
    if (endOffset % pageSize > 0) {
        u_int64_t pageEndOffset = ((endOffset/pageSize)+1)*pageSize;
        u_int64_t readSize = pageEndOffset - endOffset;
        if (readSize > 0) {
            if (lseek(fd, endOffset-minOffset, SEEK_SET) == -1) {
                fprintf(stderr, 
                    "Error seeking to %ld in disk file to read the page when write end byte \\
                    not aligned \n", endOffset-minOffset);
                perror("");
            }
            void *buffer = malloc(readSize);
            if (posix_memalign((void **)&buffer, blockSize, readSize)) 
                perror("Error in memealign when reading the page when write end byte \\
                    not aligned \n");
            if (read(fd, buffer, readSize) == -1) 
                perror("Error reading the disk file when reading the page when write end byte \\
                    not aligned \n");
            free(buffer);
        }
    }
    close(fd);
}


void direct_read(
    const char *diskFilePath, 
    u_int64_t offset, 
    u_int64_t size, 
    u_int64_t minOffset, 
    u_int64_t pageSize,
    u_int64_t blockSize,
    bool *cacheHitFlagArray) {

    mode_t mode = 0644;
    u_int64_t fd = open(diskFilePath, O_RDWR | O_DIRECT, mode);
    if (fd == -1) 
        perror("Opening the disk file failed!");

    // start and end byte of the pages affected by the request 
    u_int64_t pageStartOffset = offset - (offset % pageSize);
    u_int64_t pageEndOffset = (((offset+size)/pageSize)+1)*pageSize;
    u_int64_t numPages = (pageEndOffset - pageStartOffset)/pageSize;

    // update the hit stats of the first request 
    bool prevCacheHitFlag = cacheHitFlagArray[0];
    u_int64_t missRun = 0;
    u_int64_t readStartOffset = 0;
    if (!prevCacheHitFlag) {
        missRun++;
        readStartOffset = pageStartOffset;
    }

    for (int curPageIndex=1; curPageIndex<numPages; curPageIndex++) {
        bool caceHitFlag = cacheHitFlagArray[curPageIndex];
        if (prevCacheHitFlag & !prevCacheHitFlag) {
            // last block was a hit, this is a miss 
            // track the offset and start the miss count 
            readStartOffset = pageStartOffset + curPageIndex * pageSize;
            missRun = 1;
        } else if (!prevCacheHitFlag & !prevCacheHitFlag) {
            // last block was a miss, this is also a miss 
            // increase the miss run counter 
            missRun++;
        } else if (!prevCacheHitFlag & prevCacheHitFlag) {
            // prev block was a miss, this is a hit 
            // read the current contiguous section from disk 
            if (lseek(fd, readStartOffset, SEEK_SET) == -1) {
                fprintf(stderr, "Couldn't seek to offset %ld in file %s \n", readStartOffset, diskFilePath);
                perror("");
            }
            u_int64_t readSize = ceil((missRun*pageSize)/blockSize)*blockSize;
            void *buffer = malloc(readSize);
            if (posix_memalign((void **)&buffer, blockSize, readSize)) 
                perror("Error in read buffer memory alignment in direct read!\n");
            if (read(fd, buffer, readSize) == -1) 
                perror("Error reading the disk file during direct read!\n");
            free(buffer);
        }
    }
    close(fd);
}
