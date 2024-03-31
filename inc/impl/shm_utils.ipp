#pragma once

namespace midas {

inline RegionListReader::RegionListReader(uint64_t uuid) : uuid_(uuid) {
  const auto shm_name = utils::get_region_list_shm_name(uuid_);
  const auto rwmode = boost::interprocess::read_only;
  auto shm_obj =
      SharedMemObj(boost::interprocess::open_only, shm_name.c_str(), rwmode);
  int64_t shm_size;
  shm_obj.get_size(shm_size);
  shm_ = std::make_shared<MappedRegion>(shm_obj, rwmode, 0, shm_size);
}

inline const RegionList *RegionListReader::get() {
  return reinterpret_cast<RegionList *>(shm_->get_address());
}

inline RegionListWriter::RegionListWriter(uint64_t uuid) : uuid_(uuid) {
  using SharedMemObj = boost::interprocess::shared_memory_object;
  uint64_t shm_size = sizeof(uint64_t) * kMaxRegionNum;
  const std::string shm_name = utils::get_region_list_shm_name(uuid_);
  const auto rwmode = boost::interprocess::read_write;
  boost::interprocess::permissions perms;
  perms.set_unrestricted();

  shm_file_ = std::make_shared<SharedMemObj>(boost::interprocess::create_only,
                                             shm_name.c_str(), rwmode, perms);
  shm_file_->truncate(shm_size);
  int64_t actual_size;
  shm_file_->get_size(actual_size);
  shm_ = std::make_shared<MappedRegion>(*shm_file_, rwmode, 0, actual_size);
  std::memset(shm_->get_address(), 0, actual_size);
}

inline RegionListWriter::~RegionListWriter() {
  SharedMemObj::remove(utils::get_region_list_shm_name(uuid_).c_str());
}

inline RegionList *RegionListWriter::get() {
  return reinterpret_cast<RegionList *>(shm_->get_address());
}

} // namespace midas