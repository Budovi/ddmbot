#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

int main(int argc, char **argv)
{
	if(argc < 2 || argc > 3) {
		fprintf(stderr, "Usage: %s <fifo_path> [new_fifo_size]\n", argv[0]);
	    return 1;
	}
	
	int fd = open(argv[1], O_RDWR);
	if (fd == -1) {
		fprintf(stderr, "Failed to open %s: %s\n", argv[1], strerror(errno));
		return 2;
	}

	int ret;
	if (argc == 3) {
		char* endptr;
		unsigned size = strtoul(argv[2], &endptr, 10);
		if (*endptr != '\0' || size > INT_MAX) {
			fprintf(stderr, "The size given is either invalid or too big\n");
			close(fd);
			return 3;
		}
		ret = fcntl(fd, F_SETPIPE_SZ, (int)size);
	} else {
		ret = fcntl(fd, F_GETPIPE_SZ);
	}
	if (ret == -1) {
		fprintf(stderr, "Operation failed: %s\n", strerror(errno));
		close(fd);
		return 4;
	}
	printf("%d\n", ret);
	close(fd);
	return 0;
}
