#ifndef __MIDAS_EXTERN_H
#define __MIDAS_EXTERN_H

extern void midas_runtime_init(WT_SESSION_IMPL *session);
extern void midas_runtime_destroy(WT_SESSION_IMPL *session);

extern int __midas_malloc(WT_SESSION_IMPL *session, size_t bytes_to_allocate, void *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

extern void __midas_free_int(WT_SESSION_IMPL *session, const void *p_arg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")));

extern int __midas_buf_grow_worker(WT_SESSION_IMPL *session, WT_ITEM *buf, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#endif // __MIDAS_EXTERN_H