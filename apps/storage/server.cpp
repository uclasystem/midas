#include <chrono>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "server.hpp"
// [Midas]
#include "perf.hpp"
#include "zipf.hpp"

namespace storage {
Server::Server()
    : disk_file_(kDiskPath, std::ios::in | std::ios::out | std::ios::binary) {
  if (!disk_file_.is_open()) {
    std::cout << "Error opening file!" << std::endl;
    exit(-1);
  } else {
    disk_file_.seekg(0, std::ios::end);
    std::streampos size = disk_file_.tellg();
    disk_file_.seekg(0);
    std::cout << "File size: " << size << " bytes, " << size / kPageSize
              << " pages" << std::endl;
  }
  auto cmanager = midas::CacheManager::global_cache_manager();
  assert(cmanager->create_pool("page-cache"));
  pool_ = cmanager->get_pool("page-cache");
  pool_->update_limit(5ll * 1024 * 1024 * 1024);
  page_cache_ = std::make_unique<SyncArray<Page>>(pool_, kNumPages);

  for (int i = 0; i < kNumThds; i++) {
    std::random_device rd;
    gens[i] = std::make_unique<std::mt19937>(rd());
    zipf_dist[i] = std::make_unique<midas::zipf_table_distribution<>>(
        kNumPages, kSkewness);
    op_dist[i] = std::make_unique<std::uniform_real_distribution<>>(0.0, 1.0);
  }
}

Server::~Server() { disk_file_.close(); }

bool Server::construct(size_t page_idx) {
  Page page;
  {
    std::unique_lock<std::mutex> ul(disk_mtx_);
    disk_file_.seekg(page_idx * kPageSize, std::ios::beg);
    disk_file_.read(page, sizeof(Page));
    int num_read = disk_file_.gcount();
    assert(num_read == sizeof(Page));
  }
  bool succ = page_cache_->set(page_idx, page);
  return true;
}

bool Server::read(size_t page_idx) {
  if (page_idx >= kNumPages)
    return false;
  auto cached = page_cache_->get(page_idx);
  if (!cached) {
    auto stt = midas::Time::get_cycles_stt();
    construct(page_idx);
    auto end = midas::Time::get_cycles_end();
    pool_->record_miss_penalty(end - stt, sizeof(Page));
  }
  return true;
}

bool Server::write(size_t page_idx) {
  Page page;

  if (page_idx >= kNumPages)
    return false;
  else {
    {
      std::unique_lock<std::mutex> ul(disk_mtx_);
      // disk_file_.seekg(0); // reset the pointer
      disk_file_.seekg(page_idx * kPageSize, std::ios::beg);
      disk_file_.write(page, sizeof(Page));
    }
    bool succ = page_cache_->set(page_idx, page);
    return true;
  }
}

void Server::warmup() {
  for (int64_t i = 0; i < kNumPages; i++) {
    read(i);
  }
  std::cout << "Done warmup!" << std::endl;
}

std::unique_ptr<midas::PerfRequest> Server::gen_req(int tid) {
  auto req = std::make_unique<PgReq>();
  req->tid = tid;
  req->op =
      (*op_dist[tid])(*gens[tid]) < kReadRatio ? PgReq::READ : PgReq::WRITE;
  req->pg_idx = (*zipf_dist[tid])(*gens[tid]);

  return req;
}

bool Server::serve_req(int tid, const midas::PerfRequest *req_) {
  auto *req = dynamic_cast<const PgReq *>(req_);
  if (req->op == PgReq::READ)
    read(req->pg_idx);
  else if (req->op == PgReq::WRITE)
    write(req->pg_idx);
  else
    std::cerr << "Unknown OP code: " << req->op << std::endl;
  return true;
}
} // namespace storage

int run() {
  storage::Server server;
  server.warmup();
  for (int i = 0; i < 100; i++) {
    midas::Perf perf(server);
    auto target_kops = 800;
    auto duration_us = 1500ull * midas::to_us;
    auto warmup_us = 10ull * midas::to_us;
    auto miss_ddl = 10ull * midas::to_us;
    perf.run(storage::kNumThds, target_kops, duration_us, warmup_us, miss_ddl);
    std::cout << "Real Tput: " << perf.get_real_kops() << " Kops" << std::endl;
    std::cout << "P99 Latency: " << perf.get_nth_lat(99) << " us" << std::endl;
  }

  return 0;
}

int main(int argc, char *argv[]) {
  run();
  return 0;
}
