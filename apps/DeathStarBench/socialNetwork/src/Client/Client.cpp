#include <chrono>
#include <cstdint>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <signal.h>
#include <string>
#include <thread>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>

#include <boost/program_options.hpp>

#include "../../gen-cpp/ComposePostService.h"
#include "../../gen-cpp/HomeTimelineService.h"
#include "../../gen-cpp/SocialGraphService.h"
#include "../../gen-cpp/UserService.h"
#include "../../gen-cpp/UserTimelineService.h"
#include "../../gen-cpp/social_network_types.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../utils.h"
#include "../utils_thrift.h"
#include "zipf.hpp"

// [midas]
#include "perf.hpp"

using apache::thrift::protocol::TBinaryProtocolFactory;
using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::transport::TServerSocket;
using namespace social_network;

// constexpr static uint32_t kNumUsers = 962;
constexpr static char kCharSet[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
constexpr static uint32_t kTextLen = 64;
constexpr static uint32_t kUrlLen = 64;
constexpr static uint32_t kMaxNumMentionsPerText = 2;
constexpr static uint32_t kMaxNumUrlsPerText = 2;
constexpr static uint32_t kMaxNumMediasPerText = 2;
constexpr static uint32_t kNumThd = 4;
constexpr static uint32_t kPerThdWorkload = 100000;
constexpr static bool kSkewed = true;
constexpr static float kSkewness = 0.99;

constexpr static uint32_t kEvalThd = 20;
// constexpr static uint32_t kEpoch = 10;

constexpr static char kDatasetPath[] = "/datasets/social-graph";
constexpr static char kDatasetName[] = "socfb-Penn94";
// constexpr static char kDatasetName[] = "socfb-Reed98";

uint64_t CPU_FREQ = 0;

static inline uint64_t rdtscp() {
  uint32_t a, d, c;
  asm volatile("rdtscp" : "=a"(a), "=d"(d), "=c"(c));
  return ((uint64_t)a) | (((uint64_t)d) << 32);
}

bool init_timer() {
  auto stt_time = std::chrono::high_resolution_clock::now();
  uint64_t stt_cycles = rdtscp();
  volatile int64_t sum = 0;
  for (int i = 0; i < 10000000; i++) {
    sum += i;
  }
  uint64_t end_cycles = rdtscp();
  auto end_time = std::chrono::high_resolution_clock::now();
  uint64_t dur_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - stt_time)
          .count();
  CPU_FREQ = (end_cycles - stt_cycles) * 1000 / dur_ns;
  std::cout << "Timer initialized, CPU Freq: " << CPU_FREQ << "MHz" << std::endl;
  return true;
}

inline uint64_t microtime() { return rdtscp() / CPU_FREQ; }

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

template <typename Service> class WrappedClientPool {
public:
  void init(std::string service_name) {
    if (load_config_file("/config/service-config.json", &config_json) != 0) {
      exit(EXIT_FAILURE);
    }

    std::cout << service_name << std::endl;

    int port = config_json[service_name]["port"];
    std::string addr = config_json[service_name]["addr"];
    // int conns = config_json[service_name]["connections"];
    int conns = kEvalThd;
    int timeout = config_json[service_name]["timeout_ms"];
    int keepalive = config_json[service_name]["keepalive_ms"];

    std::cout << port << " " << addr << " " << timeout << " done" << std::endl;

    _client_pool.reset(new ClientPool<Service>(service_name.append("-client"),
                                               addr, port, 0, conns, timeout,
                                               keepalive, config_json));
  }

  ClientPool<Service> *get() { return _client_pool.get(); }

private:
  json config_json;
  std::shared_ptr<ClientPool<Service>> _client_pool;
};

struct SocialNetState {
  WrappedClientPool<ThriftClient<SocialGraphServiceClient>> social_graph_client;
  WrappedClientPool<ThriftClient<UserServiceClient>> user_service_client;
  WrappedClientPool<ThriftClient<ComposePostServiceClient>> compose_post_client;
  WrappedClientPool<ThriftClient<HomeTimelineServiceClient>>
      home_timeline_client;
  WrappedClientPool<ThriftClient<UserTimelineServiceClient>>
      user_timeline_client;

  struct SocialGraph {
    int64_t numUsers;
    std::vector<std::pair<int64_t, int64_t>> edges;
  } graph;

  std::random_device rd;
  std::unique_ptr<std::mt19937> gen;
  std::unique_ptr<std::uniform_int_distribution<>> uniform_1_100;
  std::unique_ptr<std::uniform_int_distribution<>> uniform_1_numusers;
  std::unique_ptr<zipf_table_distribution<>> zipf_0_numusers_1;
  std::unique_ptr<std::uniform_int_distribution<>> uniform_0_charsetsize;
  std::unique_ptr<std::uniform_int_distribution<>> uniform_0_maxnummentions;
  std::unique_ptr<std::uniform_int_distribution<>> uniform_0_maxnumurls;
  std::unique_ptr<std::uniform_int_distribution<>> uniform_0_maxnummedias;
  std::unique_ptr<std::uniform_int_distribution<int64_t>> uniform_0_maxint64;

  std::unique_ptr<zipf_table_distribution<>> eval_zipf_users[kEvalThd];
  std::unique_ptr<std::uniform_int_distribution<>> eval_uniform_users[kEvalThd];
  std::unique_ptr<std::mt19937> eval_gens[kEvalThd];

  int64_t getNumUsers() { return graph.numUsers; }

  int loadGraph() {
    std::string fullPath = std::string(kDatasetPath) + "/" + kDatasetName +
                           "/" + kDatasetName + ".nodes";
    std::ifstream nodeFile(fullPath);
    if (!nodeFile.is_open()) {
      std::cerr << "Could not open file " << fullPath << std::endl;
      return -1;
    }
    nodeFile >> graph.numUsers;
    nodeFile.close();

    fullPath = std::string(kDatasetPath) + "/" + kDatasetName + "/" +
               kDatasetName + ".edges";
    std::ifstream edgeFile(fullPath);
    if (!edgeFile.is_open()) {
      std::cerr << "Could not open file " << fullPath << std::endl;
      return -1;
    }
    int64_t src, dst;
    while (edgeFile >> src >> dst) {
      // NOTE: for Socfb-Penn94, the node id starts from 1.
      graph.edges.push_back(std::pair<int64_t, int64_t>(src - 1, dst - 1));
    }
    std::cout << graph.numUsers << " " << graph.edges.size() << std::endl;
    return 0;
  }

  int init() {
    this->user_service_client.init("user-service");
    this->compose_post_client.init("compose-post-service");
    this->home_timeline_client.init("home-timeline-service");
    this->user_timeline_client.init("user-timeline-service");
    this->social_graph_client.init("social-graph-service");

    loadGraph();

    this->gen.reset(new std::mt19937((this->rd)()));
    this->uniform_1_100.reset(new std::uniform_int_distribution<>(1, 100));
    this->uniform_1_numusers.reset(
        new std::uniform_int_distribution<>(1, getNumUsers()));
    this->zipf_0_numusers_1.reset(
        new zipf_table_distribution<>(getNumUsers(), kSkewness));
    this->uniform_0_charsetsize.reset(
        new std::uniform_int_distribution<>(0, sizeof(kCharSet) - 2));
    this->uniform_0_maxnummentions.reset(
        new std::uniform_int_distribution<>(0, kMaxNumMentionsPerText));
    this->uniform_0_maxnumurls.reset(
        new std::uniform_int_distribution<>(0, kMaxNumUrlsPerText));
    this->uniform_0_maxnummedias.reset(
        new std::uniform_int_distribution<>(0, kMaxNumMediasPerText));
    this->uniform_0_maxint64.reset(new std::uniform_int_distribution<int64_t>(
        0, std::numeric_limits<int64_t>::max()));

    for (int i = 0; i < kEvalThd; i++) {
      this->eval_zipf_users[i].reset(
          new zipf_table_distribution<>(getNumUsers(), kSkewness));
      this->eval_uniform_users[i].reset(
          new std::uniform_int_distribution<>(1, getNumUsers()));
      this->eval_gens[i].reset(new std::mt19937((this->rd)()));
    }
    return 0;
  }
} state;

std::string random_string(uint32_t len, const SocialNetState &state) {
  std::string str = "";
  for (uint32_t i = 0; i < kTextLen; i++) {
    auto idx = (*state.uniform_0_charsetsize)(*state.gen);
    str += kCharSet[idx];
  }
  return str;
}

int64_t random_int64() {
  return std::chrono::high_resolution_clock::now().time_since_epoch().count();
}

int RegisterUser(int64_t user_id) {
  std::map<std::string, std::string> carrier;
  int64_t req_id = random_int64();
  std::string firstname = "first_" + std::to_string(user_id);
  std::string lastname = "last_" + std::to_string(user_id);
  std::string username = "user_" + std::to_string(user_id);
  std::string password = "password_" + std::to_string(user_id);

  auto clientPool = state.user_service_client.get();
  auto clientWrapper = clientPool->Pop();
  if (!clientWrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to user-service";
    throw se;
  }
  auto client = clientWrapper->GetClient();
  try {
    client->RegisterUserWithId(req_id, firstname, lastname, username, password,
                               user_id, carrier);
  } catch (...) {
    std::cout << "Failed to register user " << user_id << " from user-service"
              << std::endl;
    state.user_service_client.get()->Remove(clientWrapper);
    throw;
  }
  state.user_service_client.get()->Keepalive(clientWrapper);
  return 0;
}

int Follow(int64_t src, int64_t dst) {
  std::map<std::string, std::string> carrier;
  int64_t req_id = random_int64();

  auto clientPool = state.social_graph_client.get();
  auto clientWrapper = clientPool->Pop();
  if (!clientWrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to social-graph-service";
    throw se;
  }
  auto client = clientWrapper->GetClient();
  try {
    client->Follow(req_id, src, dst, carrier);
    client->Follow(req_id, dst, src, carrier);
  } catch (...) {
    std::cout << "Failed to follow in social-graph-service:" << src << "<->"
              << dst << std::endl;
    state.social_graph_client.get()->Remove(clientWrapper);
    throw;
  }
  state.social_graph_client.get()->Keepalive(clientWrapper);

  return 0;
}

int ComposePost(int64_t user_id) {
  std::map<std::string, std::string> carrier;

  int64_t req_id = random_int64();
  // int64_t user_id = (*state.uniform_1_numusers)(*state.gen);
  std::string username = std::string("username_") + std::to_string(user_id);
  std::string text = random_string(kTextLen, state);
  int num_user_mentions = (*state.uniform_0_maxnummentions)(*state.gen);
  for (uint32_t i = 0; i < num_user_mentions; i++) {
    auto mentioned_id = (*state.uniform_1_numusers)(*state.gen);
    text += " @username_" + std::to_string(mentioned_id);
  }
  auto num_urls = (*state.uniform_0_maxnumurls)(*state.gen);
  for (uint32_t i = 0; i < num_urls; i++) {
    text += " http://" + random_string(kUrlLen, state);
  }
  int num_medias = (*state.uniform_0_maxnummedias)(*state.gen);

  std::vector<int64_t> media_ids;
  std::vector<std::string> media_types;
  for (uint32_t i = 0; i < num_medias; i++) {
    media_ids.emplace_back((*state.uniform_0_maxint64)(*state.gen));
    media_types.push_back("png");
  }
  social_network::PostType::type post_type = social_network::PostType::POST;

  auto clientPool = state.compose_post_client.get();
  auto clientWrapper = clientPool->Pop();
  if (!clientWrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to compose-post-service";
    throw se;
  }
  auto client = clientWrapper->GetClient();
  try {
    client->ComposePost(req_id, username, user_id, text, media_ids, media_types,
                        post_type, carrier);
  } catch (...) {
    std::cout << "Failed to compose post from compose-post-service"
              << std::endl;
    state.compose_post_client.get()->Remove(clientWrapper);
    throw;
  }
  state.compose_post_client.get()->Keepalive(clientWrapper);
  return 0;
}

int ReadUserTimeline(int64_t req_id, int64_t user_id = -1) {
  std::map<std::string, std::string> carrier;
  if (user_id == -1)
    user_id = kSkewed ? ((*state.zipf_0_numusers_1)(*state.gen) + 1)
                      : (*state.uniform_1_numusers)(*state.gen);
  int64_t start = 0;
  int64_t stop = 20;
  // int64_t start = 0;
  // int64_t stop = (*state.uniform_1_100)(*state.gen);
  // int64_t start = (*state.uniform_1_100)(*state.gen) % 20;
  // int64_t stop = start + 1;
  std::vector<social_network::Post> ret;

  auto clientPool = state.user_timeline_client.get();
  auto clientWrapper = clientPool->Pop();
  if (!clientWrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to user-timeline-service";
    throw se;
  }
  auto client = clientWrapper->GetClient();
  try {
    client->ReadUserTimeline(ret, req_id, user_id, start, stop, carrier);
  } catch (...) {
    std::cout << "Failed to read posts from user-timeline-service" << std::endl;
    state.user_timeline_client.get()->Remove(clientWrapper);
    throw;
  }
  state.user_timeline_client.get()->Keepalive(clientWrapper);
  return 0;
}

int ReadHomeTimeline(int64_t req_id, int64_t user_id = -1) {
  std::map<std::string, std::string> carrier;
  if (user_id == -1)
    user_id = kSkewed ? ((*state.zipf_0_numusers_1)(*state.gen) + 1)
                      : (*state.uniform_1_numusers)(*state.gen);
  int64_t start = 0;
  int64_t stop = 10;
  // int64_t start = 0;
  // int64_t stop = (*state.uniform_1_100)(*state.gen);
  // int64_t start = (*state.uniform_1_100)(*state.gen) % 10;
  // int64_t stop = start + 1;
  std::vector<social_network::Post> ret;

  auto clientPool = state.home_timeline_client.get();
  auto clientWrapper = clientPool->Pop();
  if (!clientWrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to home-timeline-service";
    throw se;
  }
  auto client = clientWrapper->GetClient();
  try {
    client->ReadHomeTimeline(ret, req_id, user_id, start, stop, carrier);
  } catch (...) {
    std::cout << "Failed to read posts from home-timeline-service" << std::endl;
    state.home_timeline_client.get()->Remove(clientWrapper);
    throw;
  }
  // state.home_timeline_client.get()->Push(clientWrapper);
  state.home_timeline_client.get()->Keepalive(clientWrapper);
  return 0;
}

int reg_users() {
  const int BATCH_SIZE = 100;
  auto stt = std::chrono::high_resolution_clock::now();
  std::atomic<int64_t> numFinished{0};

  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNumThd; tid++) {
    thds.push_back(std::thread([&numFinished, tid = tid]() {
      const int64_t chunkSize = (state.getNumUsers() + kNumThd - 1) / kNumThd;
      const int64_t stt = chunkSize * tid;
      const int64_t end = std::min(stt + chunkSize, state.getNumUsers());

      std::vector<std::future<int>> futures;
      for (int uid = stt; uid < end; uid++) {
        futures.push_back(std::async(
            std::launch::async,
            [&](int64_t uid) {
              RegisterUser(uid);
              return 0;
            },
            uid));
        if (futures.size() >= BATCH_SIZE) {
          for (auto &future : futures)
            future.get();
          futures.clear();
          if (numFinished.fetch_add(BATCH_SIZE) % 10000 == 0)
            std::cout << "Register " << numFinished.load() << " users..."
                      << std::endl;
        }
      }

      for (auto &future : futures)
        future.get();
    }));
  }

  for (auto &thd : thds)
    thd.join();

  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "Reg users duration: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end - stt)
                   .count()
            << "ms" << std::endl;
  return 0;
}

int add_followers() {
  const int BATCH_SIZE = 100;

  auto stt = std::chrono::high_resolution_clock::now();
  std::atomic<int64_t> numFinished{0};
  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNumThd; tid++) {
    thds.push_back(std::thread([&, tid = tid]() {
      std::vector<std::future<int>> futures;
      const int64_t numEdges = state.graph.edges.size();
      const int64_t chunkSize = (numEdges + kNumThd - 1) / kNumThd;
      const int64_t stt = chunkSize * tid;
      const int64_t end = std::min(stt + chunkSize, numEdges);
      for (int64_t i = stt; i < end; i++) {
        auto &edge = state.graph.edges[i];
        futures.push_back(std::async(
            std::launch::async,
            [](int64_t src, int64_t dst) {
              Follow(src, dst);
              return 0;
            },
            edge.first, edge.second));

        if (futures.size() >= BATCH_SIZE) {
          for (auto &future : futures)
            future.get();
          futures.clear();
          if (numFinished.fetch_add(BATCH_SIZE) % 10000 == 0) {
            std::cout << "Finished " << numFinished.load() << std::endl;
          }
        }
      }

      std::cout << tid << " " << chunkSize << " " << stt << "," << end
                << std::endl;

      for (auto &future : futures) {
        future.get();
      }
    }));
  }

  for (auto &thd : thds)
    thd.join();

  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "Add followers duration: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end - stt)
                   .count()
            << "ms" << std::endl;

  return 0;
}

int compose_posts() {
  const int BATCH_SIZE = 100;
  auto stt = std::chrono::high_resolution_clock::now();
  std::atomic<int64_t> numFinished{0};
  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNumThd; tid++) {
    thds.push_back(std::thread([&numFinished, tid = tid]() {
      std::vector<std::future<int>> futures;
      int64_t chunkSize = (state.getNumUsers() + kNumThd - 1) / kNumThd;
      int64_t stt = chunkSize * tid;
      int64_t end = std::min(stt + chunkSize, state.getNumUsers());
      for (int64_t i = stt; i < end; i++) {
        futures.push_back(std::async(std::launch::async, [i = i]() {
          for (int j = 0; j < 20; j++)
            ComposePost(i);
          return 0;
        }));

        if (futures.size() % BATCH_SIZE == 0) {
          for (auto &future : futures)
            future.get();
          futures.clear();
          if (numFinished.fetch_add(BATCH_SIZE) % 10000 == 0) {
            std::cout << "Finished users: " << numFinished.load() << std::endl;
          }
        }
      }
      for (auto &future : futures)
        future.get();
    }));
  }

  for (auto &thd : thds) {
    thd.join();
  }

  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "Warm up posts duration: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end - stt)
                   .count()
            << "ms" << std::endl;

  return 0;
}

int read_posts() {
  const int BATCH_SIZE = 100;
  auto stt = std::chrono::high_resolution_clock::now();
  std::cout << "Start reading posts (user timelines)..." << std::endl;
  std::atomic<int64_t> numFinished{0};
  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNumThd; tid++) {
    thds.push_back(std::thread([&numFinished, tid = tid]() {
      std::vector<std::future<int>> futures;
      int64_t chunkSize = (state.getNumUsers() + kNumThd - 1) / kNumThd;
      int64_t stt = chunkSize * tid;
      int64_t end = std::min(stt + chunkSize, state.getNumUsers());
      for (int64_t i = stt; i < end; i++) {
        futures.push_back(std::async(std::launch::async, [i = i]() {
          ReadUserTimeline(random_int64(), i);
          return 0;
        }));

        if (futures.size() % BATCH_SIZE == 0) {
          for (auto &future : futures)
            future.get();
          futures.clear();
          if (numFinished.fetch_add(BATCH_SIZE) % 10000 == 0) {
            std::cout << "Finished users: " << numFinished.load() << std::endl;
          }
        }
      }
      for (auto &future : futures)
        future.get();
    }));
  }

  for (auto &thd : thds) {
    thd.join();
  }

  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "Read posts (user timelines) duration: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end - stt)
                   .count()
            << "ms" << std::endl;

  return 0;
}

enum RType {
  tUserTimeline,
  tHomeTimeline,
};
struct SNReq : public midas::PerfRequest {
  int64_t req_id;
  int64_t user_id;
  RType type;

};

class SNAdapter : public midas::PerfAdapter {
  std::unique_ptr<midas::PerfRequest> gen_req(int tid) override;
  bool serve_req(int tid, const midas::PerfRequest *req) override;
};

std::unique_ptr<midas::PerfRequest> SNAdapter::gen_req(int tid) {
  int64_t req_id = (((u_int64_t)random_int64() << 8) | (tid));
  int64_t user_id =
      kSkewed ? ((*state.eval_zipf_users[tid])(*state.eval_gens[tid]) + 1)
              : (*state.eval_uniform_users[tid])(*state.eval_gens[tid]);
  auto req = std::make_unique<SNReq>();
  req->req_id = req_id;
  req->user_id = user_id;
  req->type = tUserTimeline;
  return req;
}

bool SNAdapter::serve_req(int tid, const midas::PerfRequest *req_) {
  auto *req = dynamic_cast<const SNReq *>(req_);
  auto req_id = req->req_id;
  auto user_id = req->user_id;
  if (req->type == tUserTimeline)
    ReadUserTimeline(req_id, user_id);
  else if (req->type == tHomeTimeline)
    ReadHomeTimeline(req_id, user_id);
  else
    std::cerr << "Unknown OP code: " << req->type;
  return true;
}

int main(int argc, char *argv[]) {
  state.init();

  bool init = false;
  bool warmup = false;
  if (argc > 1) {
    if (strncmp(argv[1], "init", 4) == 0)
      init = true;
    if (strncmp(argv[1], "warmup", 6) == 0)
      warmup = true;
  }
  if (init) {
    reg_users();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    add_followers();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    compose_posts();
    return 0;
  }

  if (warmup)
    read_posts();

  SNAdapter adapter;
  midas::Perf perf(adapter);
  auto target_kops = 20;
  auto miss_ddl = 10ull * midas::to_us;

  auto warmup_us = 100ull * midas::to_us;
  perf.run(kEvalThd, target_kops, 0, warmup_us, miss_ddl);
  perf.reset();

  init_timer();
  auto stt_ts = microtime();

  for (int i = 0; i < 10000; i++) {
    auto duration_us = 5ull * midas::to_us;
    perf.reset();
    perf.run(kEvalThd, target_kops, duration_us, 0, miss_ddl);
    auto curr_ts = (microtime() - stt_ts) / midas::to_us; // to s

    auto real_kops = perf.get_real_kops();
    auto p99_lat = perf.get_nth_lat(99);
    std::cout << "Real Tput: " << real_kops << " Kops" << std::endl;
    std::cout << "P99 Latency: " << p99_lat << " us" << std::endl;
    std::cout << "raw: " << curr_ts << "\t" << p99_lat << "\t" << real_kops
              << std::endl;
  }
  return 0;
}
