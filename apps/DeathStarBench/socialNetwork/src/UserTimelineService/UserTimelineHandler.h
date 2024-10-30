#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_USERTIMELINESERVICE_USERTIMELINEHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_USERTIMELINESERVICE_USERTIMELINEHANDLER_H_

#include <bson/bson.h>
#include <mongoc.h>
#include <sw/redis++/redis++.h>

#include <future>
#include <iostream>
#include <string>

#include "../../gen-cpp/PostStorageService.h"
#include "../../gen-cpp/UserTimelineService.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"
#include "../PostUtils.h"

// [Midas]
#include "cache_manager.hpp"
#include "resource_manager.hpp"
#include "sync_kv.hpp"
#include "time.hpp"
constexpr static int kNumBuckets = 1 << 20;

using namespace sw::redis;

namespace social_network {

class UserTimelineHandler : public UserTimelineServiceIf {
 public:
  UserTimelineHandler(Redis *, mongoc_client_pool_t *,
                      ClientPool<ThriftClient<PostStorageServiceClient>> *,
                      uint64_t);
  UserTimelineHandler(RedisCluster *, mongoc_client_pool_t *,
                      ClientPool<ThriftClient<PostStorageServiceClient>> *,
                      uint64_t);
  ~UserTimelineHandler() override = default;

  void WriteUserTimeline(
      int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
      const std::map<std::string, std::string> &carrier) override;

  void ReadUserTimeline(std::vector<Post> &, int64_t, int64_t, int, int,
                        const std::map<std::string, std::string> &) override;

private:
  Redis *_redis_client_pool;
  RedisCluster *_redis_cluster_client_pool;
  mongoc_client_pool_t *_mongodb_client_pool;
  ClientPool<ThriftClient<PostStorageServiceClient>> *_post_client_pool;
  // [Midas]
  midas::CachePool *_pool;
  std::shared_ptr<midas::SyncKV<kNumBuckets>> _post_cache;
};

UserTimelineHandler::UserTimelineHandler(
    Redis *redis_pool, mongoc_client_pool_t *mongodb_pool,
    ClientPool<ThriftClient<PostStorageServiceClient>> *post_client_pool,
    uint64_t pool_size) {
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
  _pool->set_weight(2);
  _pool->set_lat_critical(true);
  _post_cache = std::make_shared<midas::SyncKV<kNumBuckets>>(_pool);

  _redis_client_pool = redis_pool;
  _redis_cluster_client_pool = nullptr;
  _mongodb_client_pool = mongodb_pool;
  _post_client_pool = post_client_pool;
}

UserTimelineHandler::UserTimelineHandler(
    RedisCluster *redis_pool, mongoc_client_pool_t *mongodb_pool,
    ClientPool<ThriftClient<PostStorageServiceClient>> *post_client_pool,
    uint64_t pool_size) {
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
  _post_cache = std::make_shared<midas::SyncKV<kNumBuckets>>(_pool);

  _redis_cluster_client_pool = redis_pool;
  _redis_client_pool = nullptr;
  _mongodb_client_pool = mongodb_pool;
  _post_client_pool = post_client_pool;
}

void UserTimelineHandler::WriteUserTimeline(
    int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
    const std::map<std::string, std::string> &carrier) {
  // // Initialize a span
  // TextMapReader reader(carrier);
  // std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // auto span = opentracing::Tracer::Global()->StartSpan(
  //     "write_user_timeline_server", {opentracing::ChildOf(parent_span->get())});
  // opentracing::Tracer::Global()->Inject(span->context(), writer);

  mongoc_client_t *mongodb_client =
      mongoc_client_pool_pop(_mongodb_client_pool);
  if (!mongodb_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to pop a client from MongoDB pool";
    throw se;
  }
  auto collection = mongoc_client_get_collection(
      mongodb_client, "user-timeline", "user-timeline");
  if (!collection) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to create collection user-timeline from MongoDB";
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }
  bson_t *query = bson_new();

  BSON_APPEND_INT64(query, "user_id", user_id);
  bson_t *update =
      BCON_NEW("$push", "{", "posts", "{", "$each", "[", "{", "post_id",
               BCON_INT64(post_id), "timestamp", BCON_INT64(timestamp), "}",
               "]", "$position", BCON_INT32(0), "}", "}");
  bson_error_t error;
  bson_t reply;
  // auto update_span = opentracing::Tracer::Global()->StartSpan(
  //     "write_user_timeline_mongo_insert_client",
  //     {opentracing::ChildOf(&span->context())});
  bool updated = mongoc_collection_find_and_modify(collection, query, nullptr,
                                                   update, nullptr, false, true,
                                                   true, &reply, &error);
  // update_span->Finish();

  if (!updated) {
    // update the newly inserted document (upsert: false)
    updated = mongoc_collection_find_and_modify(collection, query, nullptr,
                                                update, nullptr, false, false,
                                                true, &reply, &error);
    if (!updated) {
      LOG(error) << "Failed to update user-timeline for user " << user_id
                 << " to MongoDB: " << error.message;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = error.message;
      bson_destroy(update);
      bson_destroy(query);
      bson_destroy(&reply);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }
  }

  bson_destroy(update);
  bson_destroy(&reply);
  bson_destroy(query);
  mongoc_collection_destroy(collection);
  mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

  // Update user's timeline in redis
  // auto redis_span = opentracing::Tracer::Global()->StartSpan(
  //     "write_user_timeline_redis_update_client",
  //     {opentracing::ChildOf(&span->context())});
  try {
    if (_redis_client_pool)
      _redis_client_pool->zadd(std::to_string(user_id), std::to_string(post_id),
                              timestamp, UpdateType::NOT_EXIST);
    else
      _redis_cluster_client_pool->zadd(std::to_string(user_id), std::to_string(post_id),
                              timestamp, UpdateType::NOT_EXIST);

  } catch (const Error &err) {
    LOG(error) << err.what();
    throw err;
  }
  // redis_span->Finish();
  // span->Finish();
}

void UserTimelineHandler::ReadUserTimeline(
    std::vector<Post> &_return, int64_t req_id, int64_t user_id, int start,
    int stop, const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "read_user_timeline_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  if (stop <= start || start < 0) {
    return;
  }

  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "read_user_timeline_redis_find_client",
      {opentracing::ChildOf(&span->context())});

  std::vector<std::string> post_ids_str;
  try {
    if (_redis_client_pool)
      _redis_client_pool->zrevrange(std::to_string(user_id), start, stop - 1,
                                  std::back_inserter(post_ids_str));
    else
      _redis_cluster_client_pool->zrevrange(std::to_string(user_id), start, stop - 1,
                                  std::back_inserter(post_ids_str));
  } catch (const Error &err) {
    LOG(error) << err.what();
    throw err;
  }
  redis_span->Finish();

  std::vector<int64_t> post_ids;
  for (auto &post_id_str : post_ids_str) {
    post_ids.emplace_back(std::stoul(post_id_str));
  }

  // find in mongodb
  int mongo_start = start + post_ids.size();
  std::unordered_map<std::string, double> redis_update_map;
  if (mongo_start < stop) {
    // Instead find post_ids from mongodb
    mongoc_client_t *mongodb_client =
        mongoc_client_pool_pop(_mongodb_client_pool);
    if (!mongodb_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to pop a client from MongoDB pool";
      throw se;
    }
    auto collection = mongoc_client_get_collection(
        mongodb_client, "user-timeline", "user-timeline");
    if (!collection) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to create collection user-timeline from MongoDB";
      throw se;
    }

    bson_t *query = BCON_NEW("user_id", BCON_INT64(user_id));
    bson_t *opts = BCON_NEW("projection", "{", "posts", "{", "$slice", "[",
                            BCON_INT32(0), BCON_INT32(stop), "]", "}", "}");

    auto find_span = opentracing::Tracer::Global()->StartSpan(
        "user_timeline_mongo_find_client",
        {opentracing::ChildOf(&span->context())});
    mongoc_cursor_t *cursor =
        mongoc_collection_find_with_opts(collection, query, opts, nullptr);
    find_span->Finish();
    const bson_t *doc;
    bool found = mongoc_cursor_next(cursor, &doc);
    if (found) {
      bson_iter_t iter_0;
      bson_iter_t iter_1;
      bson_iter_t post_id_child;
      bson_iter_t timestamp_child;
      int idx = 0;
      bson_iter_init(&iter_0, doc);
      bson_iter_init(&iter_1, doc);
      while (bson_iter_find_descendant(
                 &iter_0, ("posts." + std::to_string(idx) + ".post_id").c_str(),
                 &post_id_child) &&
             BSON_ITER_HOLDS_INT64(&post_id_child) &&
             bson_iter_find_descendant(
                 &iter_1,
                 ("posts." + std::to_string(idx) + ".timestamp").c_str(),
                 &timestamp_child) &&
             BSON_ITER_HOLDS_INT64(&timestamp_child)) {
        auto curr_post_id = bson_iter_int64(&post_id_child);
        auto curr_timestamp = bson_iter_int64(&timestamp_child);
        if (idx >= mongo_start) {
          //In mixed workload condition, post may composed between redis and mongo read
          //mongodb index will shift and duplicate post_id occurs
          if ( std::find(post_ids.begin(), post_ids.end(), curr_post_id) == post_ids.end() ) {
            post_ids.emplace_back(curr_post_id);
          }
        }
        redis_update_map.insert(std::make_pair(std::to_string(curr_post_id),
                                               (double)curr_timestamp));
        bson_iter_init(&iter_0, doc);
        bson_iter_init(&iter_1, doc);
        idx++;
      }
    }
    bson_destroy(opts);
    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
  }

  // midas query cache
  std::set<int64_t> post_ids_not_cached(post_ids.begin(), post_ids.end());
  if (post_ids_not_cached.size() != post_ids.size()) {
    LOG(error)<< "Post_ids are duplicated";
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message = "Post_ids are duplicated";
    throw se;
  }
  std::map<int64_t, Post> return_map;
  midas::kv_types::BatchPlug plug;
  _post_cache->batch_stt(plug);
  for (auto &post_id : post_ids) {
    size_t post_len = 0;
    char *post_store = reinterpret_cast<char *>(
        _post_cache->bget_single(&post_id, sizeof(post_id), &post_len, plug));
    if (post_store) {
      Post new_post;
      json post_json = json::parse(
          std::string(post_store, post_store + post_len));
      json_utils::JsonToPost(post_json, new_post);
      return_map.insert(std::make_pair(new_post.post_id, new_post));
      // filter out
      post_ids_not_cached.erase(new_post.post_id);
      free(post_store);
    }
  }
  _post_cache->batch_end(plug);

  auto missed_cycles_stt = midas::Time::get_cycles_stt();
  std::future<std::vector<Post>> post_future;
  if (!post_ids_not_cached.empty()) {
    post_future =
        std::async(std::launch::async, [&]() {
          auto post_client_wrapper = _post_client_pool->Pop();
          if (!post_client_wrapper) {
            ServiceException se;
            se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
            se.message = "Failed to connect to post-storage-service";
            throw se;
          }
          std::vector<Post> _return_posts;
          auto post_client = post_client_wrapper->GetClient();
          try {
            std::vector<int64_t> post_ids_(post_ids_not_cached.begin(),
                                           post_ids_not_cached.end());
            post_client->ReadPosts(_return_posts, req_id, post_ids_,
                                   writer_text_map);
          } catch (...) {
            _post_client_pool->Remove(post_client_wrapper);
            LOG(error) << "Failed to read posts from post-storage-service";
            throw;
          }
          _post_client_pool->Keepalive(post_client_wrapper);
          return _return_posts;
        });
  }

  if (redis_update_map.size() > 0) {
    auto redis_update_span = opentracing::Tracer::Global()->StartSpan(
        "user_timeline_redis_update_client",
        {opentracing::ChildOf(&span->context())});
    try {
      if (_redis_client_pool)
        _redis_client_pool->zadd(std::to_string(user_id),
                               redis_update_map.begin(),
                               redis_update_map.end());
      else
        _redis_cluster_client_pool->zadd(std::to_string(user_id),
                               redis_update_map.begin(),
                               redis_update_map.end());

    } catch (const Error &err) {
      LOG(error) << err.what();
      throw err;
    }
    redis_update_span->Finish();
  }

  if (!post_ids_not_cached.empty()) {
    try {
      // merge
      auto posts_not_cached = post_future.get();
      size_t missed_bytes = 0;
      for (auto &post : posts_not_cached) {
        return_map.insert(std::make_pair(post.post_id, post));

        json post_json;
        json_utils::PostToJson(post, post_json);
        std::string post_json_str = post_json.dump();
        int64_t post_id = post.post_id;
        if (!_post_cache->set(&post_id, sizeof(post_id), post_json_str.c_str(),
                              post_json_str.length())) {
          // LOG(error) << "Failed to set post " << post.post_id << " into Midas";
        }
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
  span->Finish();
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_SRC_USERTIMELINESERVICE_USERTIMELINEHANDLER_H_
