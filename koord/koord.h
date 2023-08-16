/*
 * koord.h - the UAPI for Midas coordinator and its ioctl's
 */

#ifndef __KOORD__
#define __KOORD__

#include <linux/types.h>
#include <linux/ioctl.h>

/*
 * [Adapted from caladan] NOTE: normally character devices are dynamically
 * allocated, but for convenience we can use 280.  This value is zoned for
 * "experimental and internal use".
 */
#define KOORD_MAJOR 280
#define KOORD_MINOR 0

enum koord_op_t { KOORD_MAP = 0, KOORD_UNMAP = 1 };

struct koord_region_req {
	pid_t pid;
	int region_size;
	int nr;
	unsigned long addrs[];
};

#define KOORD_MAGIC 0x1F
#define KOORD_IOC_MAXNR 4

#define KOORD_REG _IOW(KOORD_MAGIC, 1, pid_t)
#define KOORD_UNREG _IOW(KOORD_MAGIC, 2, pid_t)
#define KOORD_MAP _IOW(KOORD_MAGIC, 3, struct koord_region_req)
#define KOORD_UNMAP _IOW(KOORD_MAGIC, 4, struct koord_region_req)

#endif // ___KOORD__