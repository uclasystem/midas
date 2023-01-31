#ifndef __RESOURCE_MANAGER_H__
#define __RESOURCE_MANAGER_H__

#ifdef __cplusplus
extern "C" {
#endif

typedef void * ResourceManager;

ResourceManager get_global_manager();

#ifdef __cplusplus
}
#endif

#endif // __RESOURCE_MANAGER_H__