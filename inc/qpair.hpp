#pragma once

#include <boost/interprocess/creation_tags.hpp>
#include <boost/interprocess/exceptions.hpp>
#include <boost/interprocess/interprocess_fwd.hpp>
#include <boost/interprocess/ipc/message_queue.hpp>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sys/types.h>

#include "utils.hpp"

namespace cachebank {

class QSingle {
public:
  using MsgQueue = boost::interprocess::message_queue;
  // QSingle() = default;
  QSingle(std::string name, bool create, uint32_t qdepth = kClientQDepth,
          uint32_t msgsize = sizeof(CtrlMsg))
      : _qdepth(qdepth), _msgsize(msgsize), _name(name) {
    init(create);
  }

  inline int send(const void *buffer, size_t buffer_size);
  inline int recv(void *buffer, size_t buffer_size);

  inline void init(bool create);

  /* destroy() will remove the shm file so it should be called only once
   * manually by the owner of the QP, usually in its destructor. */
  inline void destroy();

private:
  uint32_t _qdepth;
  uint32_t _msgsize;
  std::string _name;
  std::shared_ptr<MsgQueue> _q;
};

class QPair {
public:
  // QPair() = default;
  QPair(std::string qpname, bool create, uint32_t qdepth = kClientQDepth,
        uint32_t msgsize = sizeof(CtrlMsg));
  QPair(std::shared_ptr<QSingle> sq, std::shared_ptr<QSingle> rq)
      : _sq(sq), _rq(rq) {}

  inline int send(const void *buffer, size_t buffer_size);
  inline int recv(void *buffer, size_t buffer_size);

  inline QSingle &SendQ() const noexcept { return *_sq; }
  inline QSingle &RecvQ() const noexcept { return *_rq; }

  /* destroy() will remove the shm file so it should be called only once
   * manually by the owner of the QP, usually in its destructor. */
  inline void destroy();

private:
  std::shared_ptr<QSingle> _sq;
  std::shared_ptr<QSingle> _rq;
};

#include "impl/qpair.ipp"

} // namespace cachebank