#pragma once

#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <memory>

#include "utils.hpp"
#include "shm_types.hpp"

namespace midas {

using SharedMemObj = boost::interprocess::shared_memory_object;
using MsgQueue = boost::interprocess::message_queue;
using MappedRegion = boost::interprocess::mapped_region;

class RegionListReader {
public:
  RegionListReader(uint64_t uuid);
  ~RegionListReader() = default;

  const RegionList *get();

private:
  uint64_t uuid_;
  std::shared_ptr<MappedRegion> shm_;
};

class RegionListWriter {
public:
  RegionListWriter(uint64_t uuid);
  ~RegionListWriter();

  RegionList *get();

private:
  uint64_t uuid_;
  std::shared_ptr<SharedMemObj> shm_file_;
  std::shared_ptr<MappedRegion> shm_;
};

} // namespace midas

#include "impl/shm_utils.ipp"