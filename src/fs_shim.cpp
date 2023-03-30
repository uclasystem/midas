#include "fs_shim.hpp"
#include "logging.hpp"
#include "utils.hpp"

#ifdef HIJACK_FS_SYSCALLS

extern "C" {
namespace midas {
/** Intercept fs-related syscalls */
int open(const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->open != NULL);

  int fd = shim->open(pathname, flags, mode);
  if (fd == -1)
    return fd;
  if (is_shm_file(pathname)) {
    shim->exclude_interpose(fd);
    return fd;
  }
  MIDAS_LOG_PRINTF(kDebug, "open(pathname=%s, flags=0x%x, mode=0%o) = %d\n",
                   pathname, flags, mode, fd);
  assert(shim->on_open(fd));
  return fd;
}

int open64(const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->open64 != NULL);

  int fd = shim->open64(pathname, flags, mode);
  if (fd == -1)
    return fd;
  if (is_shm_file(pathname)) {
    shim->exclude_interpose(fd);
    return fd;
  }
  MIDAS_LOG_PRINTF(kDebug, "open64(pathname=%s, flags=0x%x, mode=0%o) = %d\n",
                   pathname, flags, mode, fd);
  assert(shim->on_open(fd));
  return fd;
}

int creat(const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->creat != NULL);

  int fd = shim->creat(pathname, flags, mode);
  if (fd == -1)
    return fd;
  if (is_shm_file(pathname)) {
    shim->exclude_interpose(fd);
    return fd;
  }
  MIDAS_LOG_PRINTF(kDebug, "creat(pathname=%s, flags=0x%x, mode=0%o) = %d\n",
                   pathname, flags, mode, fd);
  return fd;
}

int creat64(const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->creat64 != NULL);

  int fd = shim->creat64(pathname, flags, mode);
  assert(fd != -1);
  MIDAS_LOG_PRINTF(kDebug, "creat64(pathname=%s, flags=0x%x, mode=0%o) = %d\n",
                   pathname, flags, mode, fd);
  return fd;
}

int openat(int dirfd, const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->openat != NULL);

  int fd = shim->openat(dirfd, pathname, flags, mode);
  if (fd == -1)
    return fd;
  if (is_shm_file(pathname)) {
    shim->exclude_interpose(fd);
    return fd;
  }
  MIDAS_LOG_PRINTF(kDebug,
                   "openat(dirfd=%d, pathname=%s, flags=0x%x, mode=0%o) = %d\n",
                   dirfd, pathname, flags, mode, fd);
  assert(shim->on_open(fd));
  return fd;
}

int openat64(int dirfd, const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->openat64 != NULL);

  int fd = shim->openat64(dirfd, pathname, flags, mode);
  if (fd == -1)
    return fd;
  if (is_shm_file(pathname)) {
    shim->exclude_interpose(fd);
    return fd;
  }
  MIDAS_LOG_PRINTF(
      kDebug, "openat64(dirfd=%d, pathname=%s, flags=0x%x, mode=0%o) = %d\n",
      dirfd, pathname, flags, mode, fd);
  assert(shim->on_open(fd));
  return fd;
}

int dup(int oldfd) {
  auto shim = FSShim::global_shim();
  assert(shim->dup != NULL);

  int fd = shim->dup(oldfd);
  if (fd == -1)
    return fd;
  MIDAS_LOG_PRINTF(kDebug, "dup(oldfd=%d) = %d\n", oldfd, fd);
  return fd;
}

int dup2(int oldfd, int newfd) {
  auto shim = FSShim::global_shim();
  assert(shim->dup2 != NULL);

  /* if newfd is already opened, the kernel will close it directly
   * once dup2 is invoked. So now is the last chance to mark the
   * pages as "DONTNEED" */
  // if (valid_fd(newfd))
  //   free_unclaimed_pages(newfd, true);

  int ret = shim->dup2(oldfd, newfd);
  if (ret == -1)
    return ret;
  MIDAS_LOG_PRINTF(kDebug, "dup2(oldfd=%d, newfd=%d) = %d\n", oldfd, newfd,
                   ret);
  return ret;
}

int close(int fd) {
  auto shim = FSShim::global_shim();
  assert(shim->close != NULL);

  if (shim->is_excluded(fd))
    shim->reset_interpose(fd);
  else if (fd > 0)
    assert(shim->on_close(fd));
  int ret = shim->close(fd);
  if (ret == -1)
    return ret;
  MIDAS_LOG_PRINTF(kDebug, "close(%d) = %d\n", fd, ret);
  return ret;
}

FILE *fopen(const char *path, const char *mode) {
  auto shim = FSShim::global_shim();
  assert(shim->fopen != NULL);

  FILE *fp = shim->fopen(path, mode);
  if (fp == nullptr)
    return fp;
  int fd = fileno(fp);
  if (fd == -1)
    return fp;
  if (is_shm_file(path)) {
    shim->exclude_interpose(fd);
    return fp;
  }
  MIDAS_LOG_PRINTF(kDebug, "fopen(path=%s, mode=%s) = %p\n", path, mode, fp);
  assert(shim->on_open(fd));

  return fp;
}

FILE *fopen64(const char *path, const char *mode) {
  auto shim = FSShim::global_shim();
  assert(shim->fopen64 != NULL);

  FILE *fp = shim->fopen64(path, mode);
  if (fp == nullptr)
    return fp;
  int fd = fileno(fp);
  if (fd == -1)
    return fp;
  if (is_shm_file(path)) {
    shim->exclude_interpose(fd);
    return fp;
  }
  MIDAS_LOG_PRINTF(kDebug, "fopen64(path=%s, mode=%s) = %p\n", path, mode, fp);
  assert(shim->on_open(fd));

  return fp;
}

int fclose(FILE *fp) {
  auto shim = FSShim::global_shim();
  assert(shim->close != NULL);

  int fd = fileno(fp);
  if (shim->is_excluded(fd))
    shim->reset_interpose(fd);
  else if (fd > 0)
    assert(shim->on_close(fd));
  int ret = shim->fclose(fp);
  if (ret != 0)
    return ret;
  MIDAS_LOG_PRINTF(kDebug, "fclose(fp=%p) = %d\n", fp, ret);

  return ret;
}

ssize_t read(int fd, void *buf, size_t count) {
  auto shim = FSShim::global_shim();
  assert(shim->read != NULL);
  if (shim->is_excluded(fd))
    return shim->read(fd, buf, count);

  ssize_t ret_count = shim->on_read(fd, buf, count, true, 0);
  // assert(ret_count >= 0);
  if (ret_count < 0)
    ret_count = shim->read(fd, buf, count);
  MIDAS_LOG_PRINTF(kDebug, "read(fd=%d, buf=%p, count=%ld) = %ld\n", fd, buf,
                   count, ret_count);
  return ret_count;
}

ssize_t write(int fd, const void *buf, size_t count) {
  auto shim = FSShim::global_shim();
  assert(shim->write != NULL);
  if (shim->is_excluded(fd))
    return shim->write(fd, buf, count);

  ssize_t ret_count = shim->on_write(fd, buf, count, true, 0);
  // assert(ret_count >= 0);
  if (ret_count < 0)
    ret_count = shim->write(fd, buf, count);
  MIDAS_LOG_PRINTF(kDebug, "write(fd=%d, buf=%p, count=%ld) = %ld\n", fd, buf,
                   count, ret_count);
  return ret_count;
}

ssize_t pread(int fd, void *buf, size_t count, off_t offset) {
  auto shim = FSShim::global_shim();
  assert(shim->pread != NULL);
  if (shim->is_excluded(fd))
    return shim->pread(fd, buf, count, offset);

  ssize_t ret_count = shim->on_read(fd, buf, count, false, offset);
  // assert(ret_count >= 0);
  if (ret_count < 0)
    ret_count = shim->pread(fd, buf, count, offset);
  MIDAS_LOG_PRINTF(kInfo, "pread(fd=%d, buf=%p, count=%ld, offset=%lu) = %ld\n",
                   fd, buf, count, offset, ret_count);
  return ret_count;
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
  auto shim = FSShim::global_shim();
  assert(shim->pwrite != NULL);
  if (shim->is_excluded(fd))
    return shim->pwrite(fd, buf, count, offset);

  ssize_t ret_count = shim->on_write(fd, buf, count, false, offset);
  // assert(ret_count >= 0);
  if (ret_count < 0)
    ret_count = shim->pwrite(fd, buf, count, offset);
  MIDAS_LOG_PRINTF(kInfo,
                   "pwrite(fd=%d, buf=%p, count=%ld, offset=%lu) = %ld\n", fd,
                   buf, count, offset, ret_count);
  return ret_count;
}

size_t fread(void *ptr, size_t size, size_t nmemb, FILE *stream) {
  auto shim = FSShim::global_shim();
  assert(shim->fread != NULL);
  int fd = fileno(stream);
  if (shim->is_excluded(fd))
    return shim->fread(ptr, size, nmemb, stream);

  // on_read() returns #(eles) rather than total len
  ssize_t ret_nmemb = shim->on_read(fd, ptr, size * nmemb, true, 0) / size;
  assert(ret_nmemb >= 0);
  if (ret_nmemb < 0)
    ret_nmemb = shim->fread(ptr, size, nmemb, stream);
  MIDAS_LOG_PRINTF(kDebug,
                   "fread(ptr=%p, size=%lu, nmemb=%lu, stream=%p) = %lu\n", ptr,
                   size, nmemb, stream, ret_nmemb);
  return ret_nmemb;
}

size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream) {
  auto shim = FSShim::global_shim();
  assert(shim->fwrite != NULL);
  int fd = fileno(stream);
  if (shim->is_excluded(fd))
    return shim->fwrite(ptr, size, nmemb, stream);

  // on_write() returns #(eles) rather than total len
  ssize_t ret_nmemb = shim->on_write(fd, ptr, size * nmemb, true, 0) / size;
  assert(ret_nmemb >= 0);
  if (ret_nmemb < 0)
    ret_nmemb = shim->fwrite(ptr, size, nmemb, stream);
  MIDAS_LOG_PRINTF(kDebug,
                   "fwrite(ptr=%p, size=%lu, nmemb=%lu, stream=%p) = %lu\n",
                   ptr, size, nmemb, stream, ret_nmemb);
  return ret_nmemb;
}

off_t lseek(int fd, off_t offset, int whence) {
  auto shim = FSShim::global_shim();
  assert(shim->lseek != NULL);
  if (shim->is_excluded(fd))
    return shim->lseek(fd, offset, whence);

  off_t abs_offset = shim->lseek(fd, offset, whence);
  shim->on_lseek(fd, abs_offset);
  MIDAS_LOG_PRINTF(kDebug, "lseek(fd=%d, offset=%lu, whence=%d) = %lu\n", fd,
                   offset, whence, abs_offset);
  return abs_offset;
}

/** Implementation of page cache for files */
inline bool FSShim::on_open(int fd) {
  ino_t inode = get_inode(fd);
  if (invalid_inode(inode))
    return false;

  std::unique_lock<std::mutex> ul(mtx_);
  init_cache();
  auto iter = files_.find(inode);
  if (iter != files_.cend()) {
    iter->second.offset = 0;
    return true;
  }
  files_[inode] = File(pool_);
  MIDAS_LOG(kDebug) << "create cache for file " << inode;
  return true;
}

inline bool FSShim::on_close(int fd) {
  ino_t inode = get_inode(fd);
  if (invalid_inode(inode))
    return false;

  std::unique_lock<std::mutex> ul(mtx_);
  init_cache();
  auto iter = files_.find(inode);
  if (iter == files_.cend()) {
    return false;
  }
  auto &file = iter->second;
  file.offset = 0;
  return true;
}

inline ssize_t FSShim::on_read(int fd, void *buf, size_t count, bool upd_offset,
                               off_t offset) {
  ino_t inode = get_inode(fd);
  if (invalid_inode(inode))
    return -1;

  ssize_t ret_count = 0;
  std::unique_lock<std::mutex> ul(mtx_);
  init_cache();
  auto iter = files_.find(inode);
  if (iter == files_.cend()) {
    MIDAS_LOG_PRINTF(kWarning, "file %lu doesn't exist!\n", inode);
    return -1;
  }
  ul.unlock();
  auto &file = iter->second; // page cache for this file
  const off_t prev_off = this->lseek(fd, 0, SEEK_CUR);
  if (file.offset != prev_off) { // FIX: figure out why mismatch can happen
    MIDAS_LOG(kError) << file.offset << " != " << prev_off;
    file.offset = prev_off;
  }

  char *dst_cursor = reinterpret_cast<char *>(buf);
  size_t remaining_cnt = count;
  auto curr_off = prev_off + offset;
  int32_t off_in_pg = curr_off % kPageSize; // may > 0 for the first page
  while (remaining_cnt > 0) {
    pfn_t pg = curr_off / kPageSize;
    ssize_t cnt_rd = 0;
    ssize_t cnt_in_pg =
        std::min<ssize_t>(off_in_pg + remaining_cnt, kPageSize) - off_in_pg;
    auto page = file.cache->get(pg);
    if (page) {
      auto src_cursor = reinterpret_cast<char *>(page.get()) + off_in_pg;
      std::memcpy(dst_cursor, src_cursor, cnt_in_pg);
      cnt_rd = cnt_in_pg;
    } else { // cache miss path
      auto stt = Time::get_cycles_stt();
      cnt_rd = this->pread(fd, dst_cursor, cnt_in_pg, curr_off);

      assert(off_in_pg + cnt_rd <= kPageSize);
      if (off_in_pg + cnt_rd == kPageSize) { // only cache full pages
        Page page;
        this->pread(fd, page, kPageSize, curr_off - off_in_pg);
        assert((curr_off - off_in_pg) % kPageSize == 0);
        file.cache->set(pg, page);
        auto end = Time::get_cycles_end();
        pool_->record_miss_penalty(end - stt, kPageSize);
      }
    }
    ret_count += cnt_rd;
    curr_off += cnt_rd;
    dst_cursor += cnt_rd;
    remaining_cnt -= cnt_rd;
    off_in_pg = 0; // reset off_in_pg after the first page
    if (cnt_rd == -1) // failed to read
      goto failed;
    if (cnt_rd < cnt_in_pg)
      break;
  }
  if (upd_offset) { // adjust file offset
    assert(offset == 0);
    off_t file_off = this->lseek(fd, ret_count, SEEK_CUR);
    assert(file_off == curr_off);
    file.offset = curr_off;
  }
  return ret_count;

failed:
  return -1;
}

inline ssize_t FSShim::on_write(int fd, const void *buf, size_t count,
                                bool upd_offset, off_t offset) {
  ino_t inode = get_inode(fd);
  if (invalid_inode(inode))
    return -1;

  std::unique_lock<std::mutex> ul(mtx_);
  auto iter = files_.find(inode);
  if (iter == files_.cend()) {
    MIDAS_LOG_PRINTF(kWarning, "file %lu doesn't exist!\n", inode);
    return false;
  }
  auto &file = iter->second;
  const off_t prev_off = this->lseek(fd, 0, SEEK_CUR);
  if (file.offset != prev_off) { // FIX: figure out why mismatch can happen
    MIDAS_LOG(kError) << file.offset << prev_off;
    file.offset = prev_off;
  }

  auto curr_off = prev_off + offset;
  ssize_t ret_count = this->pwrite(fd, buf, count, curr_off);
  if (ret_count == -1) // failed to write
    goto failed;

  // Evict all pages from cache for now. Ideally we should cache dirty
  // pages and evict them in fsync().
  for (pfn_t pg = curr_off / kPageSize;
       pg <= (curr_off + ret_count) / kPageSize; pg++)
    file.cache->remove(pg);

  if (upd_offset) {
    assert(offset == 0);
    auto file_off = this->lseek(fd, ret_count, SEEK_CUR);
    curr_off += ret_count;
    if (curr_off != file_off) {
      MIDAS_LOG(kError) << curr_off << " " << file_off << " " << prev_off << " "
                        << ret_count;
    }
    assert(file_off == curr_off);
    file.offset = curr_off;
  }
  return ret_count;

failed:
  return -1;
}

inline bool FSShim::on_lseek(int fd, off_t abs_offset) {
  ino_t inode = get_inode(fd);
  if (invalid_inode(inode))
    return false;

  std::unique_lock<std::mutex> ul(mtx_);
  auto iter = files_.find(inode);
  if (iter == files_.cend()) {
    MIDAS_LOG_PRINTF(kWarning, "file %lu doesn't exist!\n", inode);
    return false;
  }
  auto &file = iter->second;
  file.offset = abs_offset;
  return true;
}

} // namespace midas
} // extern "C"

#endif // HIJACK_FS_SYSCALLS