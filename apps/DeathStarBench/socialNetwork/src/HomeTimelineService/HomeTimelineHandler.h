#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_HOMETIMELINESERVICE_HOMETIMELINEHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_HOMETIMELINESERVICE_HOMETIMELINEHANDLER_H_

#include <sw/redis++/redis++.h>

#include <future>
#include <iostream>
#include <string>

#include "../../gen-cpp/HomeTimelineService.h"
#include "../../gen-cpp/PostStorageService.h"
#include "../../gen-cpp/SocialGraphService.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"
#include "../PostUtils.h"

// [Midas]
#include "cache_manager.hpp"
#include "sync_kv.hpp"
#include "time.hpp"
constexpr static int kNumBuckets = 1 << 20;

using namespace sw::redis;
namespace social_network {
class HomeTimelineHandler : public HomeTimelineServiceIf {
 public:
  HomeTimelineHandler(Redis *,
                      ClientPool<ThriftClient<PostStorageServiceClient>> *,
                      ClientPool<ThriftClient<SocialGraphServiceClient>> *, uint64_t pool_size);
  HomeTimelineHandler(RedisCluster *,
                      ClientPool<ThriftClient<PostStorageServiceClient>> *,
                      ClientPool<ThriftClient<SocialGraphServiceClient>> *,
                      uint64_t pool_size);
  ~HomeTimelineHandler() override = default;

  void ReadHomeTimeline(std::vector<Post> &, int64_t, int64_t, int, int,
                        const std::map<std::string, std::string> &) override;

  void WriteHomeTimeline(int64_t, int64_t, int64_t, int64_t,
                         const std::vector<int64_t> &,
                         const std::map<std::string, std::string> &) override;

 private:
  Redis *_redis_client_pool;
  RedisCluster *_redis_cluster_client_pool;
  ClientPool<ThriftClient<PostStorageServiceClient>> *_post_client_pool;
  ClientPool<ThriftClient<SocialGraphServiceClient>> *_social_graph_client_pool;
  // [Midas]
  midas::CachePool *_pool;
  std::unique_ptr<midas::SyncKV<kNumBuckets>> _post_cache;
};

HomeTimelineHandler::HomeTimelineHandler(
    Redis *redis_pool,
    ClientPool<ThriftClient<PostStorageServiceClient>> *post_client_pool,
    ClientPool<ThriftClient<SocialGraphServiceClient>>
        *social_graph_client_pool, uint64_t pool_size) {
  _redis_client_pool = redis_pool;
  _redis_cluster_client_pool = nullptr;
  _post_client_pool = post_client_pool;
  _social_graph_client_pool = social_graph_client_pool;

  // [Midas]
  auto _cmanager = midas::CacheManager::global_cache_manager();
  if (!_cmanager->create_pool("posts") ||
      (_pool = _cmanager->get_pool("posts")) == nullptr) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MIDAS_ERROR;
    se.message = "Failed to create midas cache pool!";
    throw se;
  };
  _pool->update_limit(pool_size);
  _post_cache = std::make_unique<midas::SyncKV<kNumBuckets>>(_pool);
}

HomeTimelineHandler::HomeTimelineHandler(
    RedisCluster *redis_pool,
    ClientPool<ThriftClient<PostStorageServiceClient>> *post_client_pool,
    ClientPool<ThriftClient<SocialGraphServiceClient>>
        *social_graph_client_pool, uint64_t pool_size) {
  _redis_client_pool = nullptr;
  _redis_cluster_client_pool = redis_pool;
  _post_client_pool = post_client_pool;
  _social_graph_client_pool = social_graph_client_pool;

  // [Midas]
  auto _cmanager = midas::CacheManager::global_cache_manager();
  if (!_cmanager->create_pool("posts") ||
      (_pool = _cmanager->get_pool("posts")) == nullptr) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MIDAS_ERROR;
    se.message = "Failed to create midas cache pool!";
    throw se;
  };
  _pool->update_limit(pool_size);
  _post_cache = std::make_unique<midas::SyncKV<kNumBuckets>>(_pool);
}

void HomeTimelineHandler::WriteHomeTimeline(
    int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
    const std::vector<int64_t> &user_mentions_id,
    const std::map<std::string, std::string> &carrier) {
  // // Initialize a span
  // TextMapReader reader(carrier);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // auto span = opentracing::Tracer::Global()->StartSpan(
  //     "write_home_timeline_server", {opentracing::ChildOf(parent_span->get())});

  // // Find followers of the user
  // auto followers_span = opentracing::Tracer::Global()->StartSpan(
  //     "get_followers_client", {opentracing::ChildOf(&span->context())});
  std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // opentracing::Tracer::Global()->Inject(followers_span->context(), writer);

  auto social_graph_client_wrapper = _social_graph_client_pool->Pop();
  if (!social_graph_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to social-graph-service";
    throw se;
  }
  auto social_graph_client = social_graph_client_wrapper->GetClient();
  std::vector<int64_t> followers_id;
  try {
    social_graph_client->GetFollowers(followers_id, req_id, user_id,
                                      writer_text_map);
  } catch (...) {
    LOG(error) << "Failed to get followers from social-network-service";
    _social_graph_client_pool->Remove(social_graph_client_wrapper);
    throw;
  }
  _social_graph_client_pool->Keepalive(social_graph_client_wrapper);
  // followers_span->Finish();

  std::set<int64_t> followers_id_set(followers_id.begin(), followers_id.end());
  followers_id_set.insert(user_mentions_id.begin(), user_mentions_id.end());

  // Update Redis ZSet
  // Zset key: follower_id, Zset value: post_id_str, Zset score: timestamp_str
  // auto redis_span = opentracing::Tracer::Global()->StartSpan(
  //     "write_home_timeline_redis_update_client",
  //     {opentracing::ChildOf(&span->context())});
  std::string post_id_str = std::to_string(post_id);

  {
    if (_redis_client_pool) {
      auto pipe = _redis_client_pool->pipeline(false);
      for (auto &follower_id : followers_id_set) {
        pipe.zadd(std::to_string(follower_id), post_id_str, timestamp,
                  UpdateType::NOT_EXIST);
      }
      try {
        auto replies = pipe.exec();
      } catch (const Error &err) {
        LOG(error) << err.what();
        throw err;
      }
    } else {
      // Create multi-pipeline that match with shards pool
      std::map<std::shared_ptr<ConnectionPool>, std::shared_ptr<Pipeline>> pipe_map;
      auto *shards_pool = _redis_cluster_client_pool->get_shards_pool();

      for (auto &follower_id : followers_id_set) {
        auto conn = shards_pool->fetch(std::to_string(follower_id));
        auto pipe = pipe_map.find(conn);
        if(pipe == pipe_map.end()) {//Not found, create new pipeline and insert
          auto new_pipe = std::make_shared<Pipeline>(_redis_cluster_client_pool->pipeline(std::to_string(follower_id), false));
          pipe_map.insert(make_pair(conn, new_pipe));
          auto *_pipe = new_pipe.get();
          _pipe->zadd(std::to_string(follower_id), post_id_str, timestamp,
                  UpdateType::NOT_EXIST);
        }else{//Found, use exist pipeline
          std::pair<std::shared_ptr<ConnectionPool>, std::shared_ptr<Pipeline>> found = *pipe;
          auto *_pipe = found.second.get();
          _pipe->zadd(std::to_string(follower_id), post_id_str, timestamp,
                  UpdateType::NOT_EXIST);
        }
      }
      // LOG(info) <<"followers_id_set items:" << followers_id_set.size()<<"; pipeline items:" << pipe_map.size();
      try {
        for(auto const &it : pipe_map) {
          auto _pipe = it.second.get();
          _pipe->exec();
        }

      } catch (const Error &err) {
        LOG(error) << err.what();
        throw err;
      }
    }
  }
  // redis_span->Finish();
}

void HomeTimelineHandler::ReadHomeTimeline(
    std::vector<Post> &_return, int64_t req_id, int64_t user_id, int start_idx,
    int stop_idx, const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  // TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // auto span = opentracing::Tracer::Global()->StartSpan(
  //     "read_home_timeline_server", {opentracing::ChildOf(parent_span->get())});
  // opentracing::Tracer::Global()->Inject(span->context(), writer);

  if (stop_idx <= start_idx || start_idx < 0) {
    return;
  }

  // auto redis_span = opentracing::Tracer::Global()->StartSpan(
  //     "read_home_timeline_redis_find_client",
  //     {opentracing::ChildOf(&span->context())});

  std::vector<std::string> post_ids_str;
  try {
    if (_redis_client_pool) {
      _redis_client_pool->zrevrange(std::to_string(user_id), start_idx,
                                    stop_idx - 1,
                                    std::back_inserter(post_ids_str));
    } else {
      _redis_cluster_client_pool->zrevrange(std::to_string(user_id), start_idx,
                                            stop_idx - 1,
                                            std::back_inserter(post_ids_str));
    }
  } catch (const Error &err) {
    LOG(error) << err.what();
    throw err;
  }
  // redis_span->Finish();

  std::vector<int64_t> post_ids;
  for (auto &post_id_str : post_ids_str) {
    post_ids.emplace_back(std::stoul(post_id_str));
  }
  std::map<int64_t, Post> return_map;
  std::set<int64_t> post_ids_not_cached(post_ids.begin(), post_ids.end());
  if (post_ids_not_cached.size() != post_ids.size()) {
    LOG(error)<< "Post_ids are duplicated";
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message = "Post_ids are duplicated";
    throw se;
  }
  midas::kv_types::BatchPlug plug;
  _post_cache->batch_stt(plug);
  for (auto &post_id : post_ids) {
    size_t post_len = 0;
    char *post_store = reinterpret_cast<char *>(
        _post_cache->bget_single(&post_id, sizeof(post_id), &post_len, plug));
    if (post_store) {
      Post new_post;
      json post_json =
          json::parse(std::string(post_store, post_store + post_len));
      json_utils::JsonToPost(post_json, new_post);
      return_map.insert(std::make_pair(new_post.post_id, new_post));
      // filter out
      post_ids_not_cached.erase(new_post.post_id);
      free(post_store);
    }
  }
  _post_cache->batch_end(plug);

  if (!post_ids_not_cached.empty()) {
    auto missed_cycles_stt = midas::Time::get_cycles_stt();
    auto post_client_wrapper = _post_client_pool->Pop();
    if (!post_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to post-storage-service";
      throw se;
    }
    auto post_client = post_client_wrapper->GetClient();
    std::vector<Post> _return_posts;
    try {
      std::vector<int64_t> _post_ids(post_ids_not_cached.begin(),
                                      post_ids_not_cached.end());
      post_client->ReadPosts(_return_posts, req_id, _post_ids, writer_text_map);
    } catch (...) {
      _post_client_pool->Remove(post_client_wrapper);
      LOG(error) << "Failed to read posts from post-storage-service";
      throw;
    }
    _post_client_pool->Keepalive(post_client_wrapper);

    try { // merge
      size_t missed_bytes = 0;
      for (auto &post : _return_posts) {
        return_map.insert(std::make_pair(post.post_id, post));

        json post_json;
        json_utils::PostToJson(post, post_json);
        std::string post_json_str = post_json.dump();
        int64_t post_id = post.post_id;
        _post_cache->set(&post_id, sizeof(post_id), post_json_str.c_str(),
                         post_json_str.length());
        missed_bytes += post_json_str.length();
      }
      auto missed_cycles_end = midas::Time::get_cycles_end();
      _pool->record_miss_penalty(missed_cycles_end - missed_cycles_stt,
                                 missed_bytes);
    } catch (...) {
      LOG(error) << "Failed to get post from post-storage-service";
      throw;
    }
  }
  if (return_map.size() < post_ids.size())
    LOG(error) << "Failed to get all posts!";
  std::vector<Post> return_vec;
  for (auto &post_id : post_ids) {
    return_vec.emplace_back(return_map[post_id]);
  }
  _return = return_vec;
  // span->Finish();
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_SRC_HOMETIMELINESERVICE_HOMETIMELINEHANDLER_H_
