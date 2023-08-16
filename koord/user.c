// userspace_program.c
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <stdint.h>
#include <stdlib.h>

#define __user
#include "koord.h"

void touch_region(unsigned long addr) {
    volatile int *p = (volatile int *)addr;
    p[0] = 1;
}

int main() {
    int fd = open("/dev/koord", O_RDWR);
    if (fd < 0) {
        perror("Failed to open character device");
        return 1;
    }

    unsigned long addr = 0x01f000000000;
    int region_size = 512 * 4096;
    int nr = 1;
    pid_t pid = getpid();

    struct koord_region_req *req = malloc(sizeof(struct koord_region_req) +
					  sizeof(unsigned long) * nr);
    req->pid = pid;
    req->region_size = region_size;
    req->nr = nr;
    req->addrs[0] = addr;
    printf("pid: %d\n", req->pid);

    // Register
    if (ioctl(fd, KOORD_REG, pid) < 0) {
        perror("Failed to register");
    }

    // Map
    if (ioctl(fd, KOORD_MAP, req) < 0) {
        perror("Failed to map region");
    }

    touch_region(addr);

    // Unmap
    if (ioctl(fd, KOORD_UNMAP, req) < 0) {
        perror("Failed to unmap region");
    }

    /* This will trigger seg fault */
    // touch_region(addr);

    // Unregister
    if (ioctl(fd, KOORD_UNREG, pid) < 0) {
        perror("Failed to unregister");
    }

    close(fd);
    free(req);

    return 0;
}