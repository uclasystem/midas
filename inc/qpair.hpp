#pragma once

#include <boost/interprocess/creation_tags.hpp>
#include <boost/interprocess/exceptions.hpp>
#include <boost/interprocess/interprocess_fwd.hpp>
#include <boost/interprocess/ipc/message_queue.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "utils.hpp"
#include "shm_types.hpp"

namespace midas {
constexpr static uint32_t kDaemonQDepth = 1024;
constexpr static uint32_t kClientQDepth = 128;
constexpr static uint32_t kMaxMsgSize = sizeof(CtrlMsg);

constexpr static char kNameCtrlQ[] = "daemon_ctrl_mq";
constexpr static char kSQPrefix[] = "sendq-";
constexpr static char kRQPrefix[] = "recvq-";

namespace utils {
const std::string get_sq_name(std::string qpname, bool create);
const std::string get_rq_name(std::string qpname, bool create);
const std::string get_ackq_name(std::string qpname, uint64_t id);
} // namespace utils

class QSingle {
public:
  using MsgQueue = boost::interprocess::message_queue;
  // QSingle() = default;
  QSingle(std::string name, bool create, uint32_t qdepth = kClientQDepth,
          uint32_t msgsize = kMaxMsgSize)
      : _qdepth(qdepth), _msgsize(msgsize), _name(name) {
    init(create);
  }

  inline int send(const void *buffer, size_t buffer_size);
  inline int recv(void *buffer, size_t buffer_size);
  inline int try_recv(void *buffer, size_t buffer_size);
  inline int timed_recv(void *buffer, size_t buffer_size, int timeout);

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
        uint32_t msgsize = kMaxMsgSize);
  QPair(std::shared_ptr<QSingle> sq, std::shared_ptr<QSingle> rq)
      : _sq(sq), _rq(rq) {}

  inline int send(const void *buffer, size_t buffer_size);
  inline int recv(void *buffer, size_t buffer_size);
  inline int try_recv(void *buffer, size_t buffer_size);
  inline int timed_recv(void *buffer, size_t buffer_size, int timeout);

  inline QSingle &SendQ() const noexcept { return *_sq; }
  inline QSingle &RecvQ() const noexcept { return *_rq; }

  /* destroy() will remove the shm file so it should be called only once
   * manually by the owner of the QP, usually in its destructor. */
  inline void destroy();

private:
  std::shared_ptr<QSingle> _sq;
  std::shared_ptr<QSingle> _rq;
};


} // namespace midas

#include "impl/qpair.ipp"