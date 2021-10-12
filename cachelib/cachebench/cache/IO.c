#include "cachelib/cachebench/cache/IO.h"


void direct_read() {
  // struct timeval start, end;
  // gettimeofday(&start, NULL);

  void *buffer;
  posix_memalign(&buffer, BLOCKSIZE, PAGESIZE);
  int f = open("/home/pranav/cachelib.data", O_RDONLY|O_DIRECT);
  posix_fadvise(f, 0, PAGESIZE, POSIX_FADV_DONTNEED);
  read(f, buffer, PAGESIZE);
	close(f);
	free(buffer);

  // gettimeofday(&end, NULL);
  // printf("Read: %ld microsecons\n", end.tv_usec - start.tv_usec);
	return 0;
}


void direct_write() {
  // struct timeval start, end;
  // gettimeofday(&start, NULL);

  void *buffer;
  posix_memalign(&buffer, BLOCKSIZE, PAGESIZE);
  int f = open("/home/pranav/cachelib.data", O_WRONLY|O_DIRECT|O_SYNC);
  write(f, buffer, PAGESIZE);
  posix_fadvise(f, 0, PAGESIZE, POSIX_FADV_DONTNEED);
	close(f);
	free(buffer);

  // gettimeofday(&end, NULL);
  // printf("Write: %ld microsecons\n", end.tv_usec - start.tv_usec);
	return 0;
}