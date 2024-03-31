#ifndef __MIDAS_UTILS_H
#define __MIDAS_UTILS_H

#define MIDAS_PRINT(format, ...) fprintf(stderr, "%s:%d " format, __func__, __LINE__)
#define MIDAS_PRINTF(format, ...) fprintf(stderr, "%s:%d " format, __func__, __LINE__, __VA_ARGS__)

#endif // __MIDAS_UTILS_H