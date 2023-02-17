#ifndef __MIDAS_RESOURCE_MANAGER_H
#define __MIDAS_RESOURCE_MANAGER_H

#ifdef __cplusplus
extern "C" {
#endif

typedef void * ResourceManager;

ResourceManager get_global_manager(void);

#ifdef __cplusplus
}
#endif

#endif // __MIDAS_RESOURCE_MANAGER_H