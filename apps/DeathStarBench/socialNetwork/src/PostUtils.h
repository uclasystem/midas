#ifndef SOCIAL_NETWORK_MICROSERVICES_POSTUTILS_H
#define SOCIAL_NETWORK_MICROSERVICES_POSTUTILS_H

#include <nlohmann/json.hpp>

#include "../gen-cpp/PostStorageService.h"
#include "logger.h"

namespace social_network {
namespace json_utils {
using json = nlohmann::json;
inline void JsonToPost(json &post_json, Post &post) {
  post.req_id = post_json["req_id"];
  post.timestamp = post_json["timestamp"];
  post.post_id = post_json["post_id"];
  post.creator.user_id = post_json["creator"]["user_id"];
  post.creator.username = post_json["creator"]["username"];
  post.post_type = post_json["post_type"];
  post.text = post_json["text"];
  for (auto &item : post_json["media"]) {
    Media media;
    media.media_id = item["media_id"];
    media.media_type = item["media_type"];
    post.media.emplace_back(media);
  }
  for (auto &item : post_json["user_mentions"]) {
    UserMention user_mention;
    user_mention.username = item["username"];
    user_mention.user_id = item["user_id"];
    post.user_mentions.emplace_back(user_mention);
  }
  for (auto &item : post_json["urls"]) {
    Url url;
    url.shortened_url = item["shortened_url"];
    url.expanded_url = item["expanded_url"];
    post.urls.emplace_back(url);
  }
}

inline void PostToJson(Post &post, json &post_json) {
  post_json["req_id"] = post.req_id;
  post_json["timestamp"] = post.timestamp;
  post_json["post_id"] = post.post_id;
  post_json["creator"]["user_id"] = post.creator.user_id;
  post_json["creator"]["username"] = post.creator.username;
  post_json["post_type"] = post.post_type;
  post_json["text"] = post.text;
  for (auto &media : post.media) {
    json media_json = {{"media_id", media.media_id},
                       {"media_type", media.media_type}};
    post_json["media"].emplace_back(media_json);
  }
  for (auto &user_mention : post.user_mentions) {
    json user_mention_json = {{"username", user_mention.username},
                              {"user_id", user_mention.user_id}};
    post_json["user_mentions"].emplace_back(user_mention_json);
  }
  for (auto &url : post.urls) {
    json url_json = {{"shortened_url", url.shortened_url},
                     {"expanded_url", url.expanded_url}};
    post_json["urls"].emplace_back(url_json);
  }
}
} // namespace json_utils
} // namespace social_network

#endif // SOCIAL_NETWORK_MICROSERVICES_POSTUTILS_H