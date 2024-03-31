#ifndef __MIDAS_BUF_H
#define __MIDAS_BUF_H

#include "alloc.h"

/* Adapted for Midas ObjectPtr. If a WT_ITEM data field points somewhere in its allocated memory. */
#define MIDAS_DATA_IN_ITEM(i) ((i)->mem != NULL && (i)->data != NULL)

/*
 * __midas_buf_grow --
 *     Grow a buffer that may be in-use, and ensure that all data is local to the buffer.
 */
static inline int
__midas_buf_grow(WT_SESSION_IMPL *session, WT_ITEM *buf, size_t size)
{
    /*
     * Take any offset in the buffer into account when calculating the size to allocate, it saves
     * complex calculations in our callers to decide if the buffer is large enough in the case of
     * buffers with offset data pointers.
     */
//     WT_ASSERT(session,
//       !MIDAS_DATA_IN_ITEM(buf) || size + WT_PTRDIFF(buf->data->ptr, buf->mem) > buf->memsize);
    return __midas_buf_grow_worker(session, buf, size);
}

/*
 * __midas_buf_set --
 *     Set the contents of the buffer.
 */
static inline int
__midas_buf_set(WT_SESSION_IMPL *session, WT_ITEM *buf, const void *data, size_t size)
{
    /*
     * The buffer grow function does what we need, but expects the data to be referenced by the
     * buffer. If we're copying data from outside the buffer, set it up so it makes sense to the
     * buffer grow function. (No test needed, this works if WT_ITEM.data is already set to "data".)
     */
    buf->data = data;
    buf->size = size;
    return (__midas_buf_grow(session, buf, size));
}

#endif // __MIDAS_BUF_H