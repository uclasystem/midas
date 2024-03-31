#ifndef __MIDAS_ALLOC_H
#define __MIDAS_ALLOC_H

#include <stdbool.h>
#include <stddef.h>

#include "utils.h"

#define __midas_free(session, p)            \
    do {                                 \
        void *__p = &(p);                \
        if (*(void **)__p != NULL)       \
            __midas_free_int(session, __p); \
    } while (0)

#endif // __MIDAS_ALLOC_H