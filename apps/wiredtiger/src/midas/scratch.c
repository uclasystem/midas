
#include "wt_internal.h"

#include "softmem.h" // midas

/*
 * __midas_buf_grow_worker --
 *     Grow a buffer that may be in-use, and ensure that all data is local to the buffer.
 */
int
__midas_buf_grow_worker(WT_SESSION_IMPL *session, WT_ITEM *buf, size_t size)
  WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
    size_t offset;
    bool copy_data;

    /*
     * Maintain the existing data: there are 3 cases:
     *
     * 1. No existing data: allocate the required memory, and initialize the data to reference it.
     * 2. Existing data local to the buffer: set the data to the same offset in the re-allocated
     *    memory. The offset in this case is likely a read of an overflow item, the data pointer
     *    is offset in the buffer in order to skip over the leading data block page header. For
     *    the same reason, take any offset in the buffer into account when calculating the size
     *    to allocate, it saves complex calculations in our callers to decide if the buffer is large
     *    enough in the case of buffers with offset data pointers.
     * 3. Existing data not-local to the buffer: copy the data into the buffer and set the data to
     *    reference it.
     *
     * Take the offset of the data pointer in the buffer when calculating the size
     * needed, overflow items use the data pointer to skip the leading data block page header
     */
    WT_ASSERT(session, !MIDAS_DATA_IN_ITEM(buf));
    if (MIDAS_DATA_IN_ITEM(buf)) { // impossible
        offset = WT_PTRDIFF(buf->data, buf->mem);
        size += offset;
        copy_data = false;
    } else {
        offset = 0;
        copy_data = buf->size > 0;
    }
    WT_ASSERT(session, copy_data);

    /*
     * This function is also used to ensure data is local to the buffer, check to see if we actually
     * need to grow anything.
     */
    WT_ASSERT(session, buf->mem == NULL && buf->memsize == 0);
    if (size > buf->memsize) {
        if (F_ISSET(buf, WT_ITEM_ALIGNED))
            WT_RET(__wt_realloc_aligned(session, &buf->memsize, size, &buf->mem));
        else
            WT_RET(__wt_realloc_noclear(session, &buf->memsize, size, &buf->mem));
    }

    WT_ASSERT(session, buf->data != NULL); // for midas cache usages, data must exist
    if (copy_data) {
        /*
         * It's easy to corrupt memory if you pass in the wrong size for the final buffer size,
         * which is harder to debug than this assert.
         */
        const object_ptr_t optr = (const object_ptr_t)buf->data;
        cache_pool_t pool = midas_get_global_cache_pool();
        WT_ASSERT(session, buf->size <= buf->memsize);
        // MIDAS_PRINTF("cache get data %p size %lu\n", optr, buf->size);
        if (!midas_copy_from_soft(pool, buf->mem, optr, buf->size, 0)) {
            __wt_free(session, buf->mem);
            buf->mem = NULL;
            buf->memsize = 0;
            return -1;
        }
    }

    /*
     * There's an edge case where our caller initializes the item to zero bytes, for example if
     * there's no configuration value and we're setting the item to reference it. In which case
     * we never allocated memory and buf.mem == NULL. Handle the case explicitly to avoid
     * sanitizer errors and let the caller continue. It's an error in the caller, but unless
     * caller assumes buf.data points into buf.mem, there shouldn't be a subsequent failure, the
     * item is consistent.
     */
    buf->data = buf->mem == NULL ? NULL : (uint8_t *)buf->mem + offset;

    return (0);
}