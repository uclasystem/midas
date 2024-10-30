#ifndef SOCIAL_NETWORK_MICROSERVICES_POSTSTORAGEHANDLER_H
#define SOCIAL_NETWORK_MICROSERVICES_POSTSTORAGEHANDLER_H

#include <bson/bson.h>
#include <mongoc.h>

#include <future>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>

#include "../../gen-cpp/PostStorageService.h"
#include "../logger.h"
#include "../tracing.h"
#include "../PostUtils.h"

// [Midas]
#include "cache_manager.hpp"
#include "sync_kv.hpp"
#include "time.hpp"
constexpr static uint64_t kNumBuckets = 1 << 20;

namespace social_network {
using json = nlohmann::json;

class PostStorageHandler : public PostStorageServiceIf {
 public:
  PostStorageHandler(mongoc_client_pool_t *, uint64_t);
  ~PostStorageHandler() override = default;

  void StorePost(int64_t req_id, const Post &post,
                 const std::map<std::string, std::string> &carrier) override;

  void ReadPost(Post &_return, int64_t req_id, int64_t post_id,
                const std::map<std::string, std::string> &carrier) override;

  void ReadPosts(std::vector<Post> &_return, int64_t req_id,
                 const std::vector<int64_t> &post_ids,
                 const std::map<std::string, std::string> &carrier) override;

private:
  midas::CachePool *_pool;
  std::shared_ptr<midas::SyncKV<kNumBuckets>> _post_cache;
  mongoc_client_pool_t *_mongodb_client_pool;
};

PostStorageHandler::PostStorageHandler(
    mongoc_client_pool_t *mongodb_client_pool, uint64_t pool_size) {
  _mongodb_client_pool = mongodb_client_pool;

  // [Midas]
  auto cmanager = midas::CacheManager::global_cache_manager();
  if (!cmanager->create_pool("posts") ||
      (_pool = cmanager->get_pool("posts")) == nullptr) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MIDAS_ERROR;
    se.message = "Failed to create midas cache pool";
    throw se;
  }
  _pool->update_limit(pool_size);
  _post_cache = std::make_shared<midas::SyncKV<kNumBuckets>>(_pool);
}

void PostStorageHandler::StorePost(
    int64_t req_id, const social_network::Post &post,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  // TextMapReader reader(carrier);
  // std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // auto span = opentracing::Tracer::Global()->StartSpan(
  //     "store_post_server", {opentracing::ChildOf(parent_span->get())});
  // opentracing::Tracer::Global()->Inject(span->context(), writer);

  mongoc_client_t *mongodb_client =
      mongoc_client_pool_pop(_mongodb_client_pool);
  if (!mongodb_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to pop a client from MongoDB pool";
    throw se;
  }

  auto collection =
      mongoc_client_get_collection(mongodb_client, "post", "post");
  if (!collection) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to create collection user from DB user";
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }

  bson_t *new_doc = bson_new();
  BSON_APPEND_INT64(new_doc, "post_id", post.post_id);
  BSON_APPEND_INT64(new_doc, "timestamp", post.timestamp);
  BSON_APPEND_UTF8(new_doc, "text", post.text.c_str());
  BSON_APPEND_INT64(new_doc, "req_id", post.req_id);
  BSON_APPEND_INT32(new_doc, "post_type", post.post_type);

  bson_t creator_doc;
  BSON_APPEND_DOCUMENT_BEGIN(new_doc, "creator", &creator_doc);
  BSON_APPEND_INT64(&creator_doc, "user_id", post.creator.user_id);
  BSON_APPEND_UTF8(&creator_doc, "username", post.creator.username.c_str());
  bson_append_document_end(new_doc, &creator_doc);

  const char *key;
  int idx = 0;
  char buf[16];

  bson_t url_list;
  BSON_APPEND_ARRAY_BEGIN(new_doc, "urls", &url_list);
  for (auto &url : post.urls) {
    bson_uint32_to_string(idx, &key, buf, sizeof buf);
    bson_t url_doc;
    BSON_APPEND_DOCUMENT_BEGIN(&url_list, key, &url_doc);
    BSON_APPEND_UTF8(&url_doc, "shortened_url", url.shortened_url.c_str());
    BSON_APPEND_UTF8(&url_doc, "expanded_url", url.expanded_url.c_str());
    bson_append_document_end(&url_list, &url_doc);
    idx++;
  }
  bson_append_array_end(new_doc, &url_list);

  bson_t user_mention_list;
  idx = 0;
  BSON_APPEND_ARRAY_BEGIN(new_doc, "user_mentions", &user_mention_list);
  for (auto &user_mention : post.user_mentions) {
    bson_uint32_to_string(idx, &key, buf, sizeof buf);
    bson_t user_mention_doc;
    BSON_APPEND_DOCUMENT_BEGIN(&user_mention_list, key, &user_mention_doc);
    BSON_APPEND_INT64(&user_mention_doc, "user_id", user_mention.user_id);
    BSON_APPEND_UTF8(&user_mention_doc, "username",
                     user_mention.username.c_str());
    bson_append_document_end(&user_mention_list, &user_mention_doc);
    idx++;
  }
  bson_append_array_end(new_doc, &user_mention_list);

  bson_t media_list;
  idx = 0;
  BSON_APPEND_ARRAY_BEGIN(new_doc, "media", &media_list);
  for (auto &media : post.media) {
    bson_uint32_to_string(idx, &key, buf, sizeof buf);
    bson_t media_doc;
    BSON_APPEND_DOCUMENT_BEGIN(&media_list, key, &media_doc);
    BSON_APPEND_INT64(&media_doc, "media_id", media.media_id);
    BSON_APPEND_UTF8(&media_doc, "media_type", media.media_type.c_str());
    bson_append_document_end(&media_list, &media_doc);
    idx++;
  }
  bson_append_array_end(new_doc, &media_list);

  bson_error_t error;
  // auto insert_span = opentracing::Tracer::Global()->StartSpan(
  //     "post_storage_mongo_insert_client",
  //     {opentracing::ChildOf(&span->context())});
  bool inserted = mongoc_collection_insert_one(collection, new_doc, nullptr,
                                               nullptr, &error);
  // insert_span->Finish();

  if (!inserted) {
    LOG(error) << "Error: Failed to insert post to MongoDB: " << error.message;
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = error.message;
    bson_destroy(new_doc);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }

  bson_destroy(new_doc);
  mongoc_collection_destroy(collection);
  mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

  // span->Finish();
}

void PostStorageHandler::ReadPost(
    Post &_return, int64_t req_id, int64_t post_id,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  // TextMapReader reader(carrier);
  // std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // auto span = opentracing::Tracer::Global()->StartSpan(
  //     "read_post_server", {opentracing::ChildOf(parent_span->get())});
  // opentracing::Tracer::Global()->Inject(span->context(), writer);

  size_t post_len = 0;
  char *post_store = reinterpret_cast<char *>(
      _post_cache->get(&post_id, sizeof(post_id), &post_len));
  if (post_store) { // cached in midas
    // LOG(debug) << "Get post " << post_id << " cache hit from Midas";
    Post new_post;
    json post_json = json::parse(
        std::string(post_store, post_store + post_len));
    json_utils::JsonToPost(post_json, _return);
    free(post_store);
  } else { // If not cached in midas
    auto missed_cycles_stt = midas::Time::get_cycles_stt();
    mongoc_client_t *mongodb_client =
        mongoc_client_pool_pop(_mongodb_client_pool);
    if (!mongodb_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to pop a client from MongoDB pool";
      throw se;
    }

    auto collection =
        mongoc_client_get_collection(mongodb_client, "post", "post");
    if (!collection) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to create collection user from DB user";
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }

    bson_t *query = bson_new();
    BSON_APPEND_INT64(query, "post_id", post_id);
    // auto find_span = opentracing::Tracer::Global()->StartSpan(
    //     "post_storage_mongo_find_client",
    //     {opentracing::ChildOf(&span->context())});
    mongoc_cursor_t *cursor =
        mongoc_collection_find_with_opts(collection, query, nullptr, nullptr);
    const bson_t *doc;
    bool found = mongoc_cursor_next(cursor, &doc);
    // find_span->Finish();
    if (!found) {
      bson_error_t error;
      if (mongoc_cursor_error(cursor, &error)) {
        LOG(warning) << error.message;
        bson_destroy(query);
        mongoc_cursor_destroy(cursor);
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
        ServiceException se;
        se.errorCode = ErrorCode::SE_MONGODB_ERROR;
        se.message = error.message;
        throw se;
      } else {
        LOG(warning) << "Post_id: " << post_id << " doesn't exist in MongoDB";
        bson_destroy(query);
        mongoc_cursor_destroy(cursor);
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
        ServiceException se;
        se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
        se.message =
            "Post_id: " + std::to_string(post_id) + " doesn't exist in MongoDB";
        throw se;
      }
    } else {
      LOG(debug) << "Post_id: " << post_id << " found in MongoDB";
      auto post_json_char = bson_as_json(doc, nullptr);
      json post_json = json::parse(post_json_char);
      json_utils::JsonToPost(post_json, _return);
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

      // upload post to midas
      if (!_post_cache->set(&post_id, sizeof(post_id), post_json_char,
                            std::strlen(post_json_char))) {
        // LOG(debug) << "Failed to set post " << post_json_char << " to midas";
      }
      // set_span->Finish();
      bson_free(post_json_char);
      auto missed_cycles_end = midas::Time::get_cycles_end();
      _pool->record_miss_penalty(missed_cycles_end - missed_cycles_stt,
                                 std::strlen(post_json_char));
    }
  }

  // span->Finish();
}
void PostStorageHandler::ReadPosts(
    std::vector<Post> &_return, int64_t req_id,
    const std::vector<int64_t> &post_ids,
    const std::map<std::string, std::string> &carrier) {
  // // Initialize a span
  // TextMapReader reader(carrier);
  // std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // auto span = opentracing::Tracer::Global()->StartSpan(
  //     "post_storage_read_posts_server",
  //     {opentracing::ChildOf(parent_span->get())});
  // opentracing::Tracer::Global()->Inject(span->context(), writer);

  if (post_ids.empty()) {
    return;
  }

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
    size_t return_value_length = 0;
    char *return_value = reinterpret_cast<char *>(_post_cache->bget_single(
        &post_id, sizeof(post_id), &return_value_length, plug));
    if (return_value) {
      Post new_post;
      json post_json = json::parse(
          std::string(return_value, return_value + return_value_length));
      json_utils::JsonToPost(post_json, new_post);
      return_map.insert(std::make_pair(new_post.post_id, new_post));
      post_ids_not_cached.erase(new_post.post_id);
      free(return_value);
    }
  }
  _post_cache->batch_end(plug);

  std::map<int64_t, std::string> post_json_map;

  // [Midas] cache miss path

  // Find the rest in MongoDB
  if (!post_ids_not_cached.empty()) {
    auto missed_cycles_stt = midas::Time::get_cycles_stt();
    mongoc_client_t *mongodb_client =
        mongoc_client_pool_pop(_mongodb_client_pool);
    if (!mongodb_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to pop a client from MongoDB pool";
      throw se;
    }
    auto collection =
        mongoc_client_get_collection(mongodb_client, "post", "post");
    if (!collection) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to create collection user from DB user";
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }
    bson_t *query = bson_new();
    bson_t query_child;
    bson_t query_post_id_list;
    const char *key;
    int idx = 0;
    char buf[16];

    BSON_APPEND_DOCUMENT_BEGIN(query, "post_id", &query_child);
    BSON_APPEND_ARRAY_BEGIN(&query_child, "$in", &query_post_id_list);
    for (auto &item : post_ids_not_cached) {
      bson_uint32_to_string(idx, &key, buf, sizeof buf);
      BSON_APPEND_INT64(&query_post_id_list, key, item);
      idx++;
    }
    bson_append_array_end(&query_child, &query_post_id_list);
    bson_append_document_end(query, &query_child);
    mongoc_cursor_t *cursor =
        mongoc_collection_find_with_opts(collection, query, nullptr, nullptr);
    const bson_t *doc;

    // auto find_span = opentracing::Tracer::Global()->StartSpan(
    //     "mongo_find_client", {opentracing::ChildOf(&span->context())});
    while (true) {
      bool found = mongoc_cursor_next(cursor, &doc);
      if (!found) {
        break;
      }
      Post new_post;
      char *post_json_char = bson_as_json(doc, nullptr);
      json post_json = json::parse(post_json_char);
      json_utils::JsonToPost(post_json, new_post);
      post_json_map.insert({new_post.post_id, std::string(post_json_char)});
      return_map.insert({new_post.post_id, new_post});
      bson_free(post_json_char);
    }
    // find_span->Finish();
    bson_error_t error;
    if (mongoc_cursor_error(cursor, &error)) {
      LOG(warning) << error.message;
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = error.message;
      throw se;
    }
    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

    // upload posts to midas cache
    size_t missed_bytes = 0;
    for (auto &it : post_json_map) {
      if (!_post_cache->set(&it.first, sizeof(it.first), it.second.c_str(),
                            it.second.length())) {
        // LOG(debug) << "Failed to set post " << it.first << " to midas";
      }
      missed_bytes += it.second.length();
    }
    auto missed_cycles_end = midas::Time::get_cycles_end();
    _pool->record_miss_penalty(missed_cycles_end - missed_cycles_stt,
                               missed_bytes);
  }

  if (return_map.size() != post_ids.size()) {
    LOG(error) << "Return set incomplete";
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message = "Return set incomplete";
    throw se;
  }

  for (auto &post_id : post_ids) {
    _return.emplace_back(return_map[post_id]);
  }
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_POSTSTORAGEHANDLER_H
