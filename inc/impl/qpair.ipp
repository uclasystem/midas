#pragma once
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/date_time/posix_time/ptime.hpp>

#include "logging.hpp"

namespace midas {

namespace utils {
inline const std::string get_sq_name(std::string qpname, bool create) {
  return create ? kSQPrefix + qpname : kRQPrefix + qpname;
}

inline const std::string get_rq_name(std::string qpname, bool create) {
  return get_sq_name(qpname, !create);
}

inline const std::string get_ackq_name(std::string qpname, uint64_t id) {
  return kSQPrefix + qpname + "-" + std::to_string(id);
}
} // namespace utils

inline int QSingle::send(const void *buffer, size_t buffer_size) {
  try {
    _q->send(buffer, buffer_size, /* prio = */ 0);
  } catch (boost::interprocess::interprocess_exception &e) {
    MIDAS_LOG(kError) << e.what();
    return -1;
  }
  return 0;
}

inline int QSingle::recv(void *buffer, size_t buffer_size) {
  try {
    unsigned priority;
    size_t recvd_size;
    _q->receive(buffer, buffer_size, recvd_size, priority);
    if (recvd_size != buffer_size) {
      MIDAS_LOG(kError) << "Q " << _name << " recv error: " << recvd_size
                        << "!=" << buffer_size;
      return -1;
    }
  } catch (boost::interprocess::interprocess_exception &e) {
    MIDAS_LOG(kError) << e.what();
    return -1;
  }
  return 0;
}

inline int QSingle::try_recv(void *buffer, size_t buffer_size) {
  try {
    unsigned priority;
    size_t recvd_size;
    if (!_q->try_receive(buffer, buffer_size, recvd_size, priority))
      return -1;
    if (recvd_size != buffer_size) {
      MIDAS_LOG(kError) << "Q " << _name << " recv error: " << recvd_size
                        << "!=" << buffer_size;
      return -1;
    }
  } catch (boost::interprocess::interprocess_exception &e) {
    MIDAS_LOG(kError) << e.what();
    return -1;
  }
  return 0;
}

inline int QSingle::timed_recv(void *buffer, size_t buffer_size, int timeout) {
  try {
    using namespace boost::posix_time;
    using namespace boost::gregorian;

    unsigned priority;
    size_t recvd_size;
    if (!_q->timed_receive(
            buffer, buffer_size, recvd_size, priority,
            ptime(second_clock::local_time() + seconds(timeout))))
      return -1;
    if (recvd_size != buffer_size) {
      MIDAS_LOG(kError) << "Q " << _name << " recv error: " << recvd_size
                        << "!=" << buffer_size;
      return -1;
    }
  } catch (boost::interprocess::interprocess_exception &e) {
    MIDAS_LOG(kError) << e.what();
    return -1;
  }
  return 0;
}

inline void QSingle::init(bool create) {
  try {
    if (create)
      _q = std::make_shared<MsgQueue>(boost::interprocess::create_only,
                                      _name.c_str(), _qdepth, _msgsize);
    else
      _q = std::make_shared<MsgQueue>(boost::interprocess::open_only,
                                      _name.c_str());
  } catch (boost::interprocess::interprocess_exception &e) {
    std::cerr << e.what() << std::endl;
  }
}

inline QPair::QPair(std::string qpname, bool create, uint32_t qdepth,
                    uint32_t msgsize)
    : _sq(std::make_shared<QSingle>(utils::get_sq_name(qpname, create), create,
                                    qdepth, msgsize)),
      _rq(std::make_shared<QSingle>(utils::get_rq_name(qpname, create), create,
                                    qdepth, msgsize)) {}

inline void QSingle::destroy() {
  try {
    MsgQueue::remove(_name.c_str());
  } catch (boost::interprocess::interprocess_exception &e) {
    MIDAS_LOG(kError) << e.what();
  }
}

inline void QPair::destroy() {
  _sq->destroy();
  _rq->destroy();
}

inline int QPair::send(const void *buffer, size_t buffer_size) {
  return _sq->send(buffer, buffer_size);
}

inline int QPair::recv(void *buffer, size_t buffer_size) {
  return _rq->recv(buffer, buffer_size);
}

inline int QPair::try_recv(void *buffer, size_t buffer_size) {
  return _rq->try_recv(buffer, buffer_size);
}

inline int QPair::timed_recv(void *buffer, size_t buffer_size, int timeout) {
  return _rq->timed_recv(buffer, buffer_size, timeout);
}
} // namespace midas