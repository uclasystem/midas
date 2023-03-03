#include <algorithm>
#include <cassert>
#include <iostream>
#include <list>
#include <random>
#include <string>
#include <unordered_map>

#include "../inc/zipf.hpp"

using namespace std;

constexpr int64_t kNumEles = 64ll * 1024;
float kSkewness = 0.9;
float kCacheRatio = 0.8;
constexpr int64_t kTolls = 320ll * 1024;

class LRU {
public:
  LRU(int cap) : capacity(cap) {}
  bool access(unsigned long id) {
    bool ret = false;
    auto found = map.find(id);
    if (found != map.cend()) {
      list.erase(found->second);
      ret = true;
    }
    list.emplace_front(id);
    map[id] = list.begin();
    while (list.size() > capacity) {
      auto tail = list.back();
      map.erase(tail);
      list.pop_back();
    }
    return ret;
  }
private:
  int capacity;
  std::list<unsigned long> list;
  std::unordered_map<unsigned long, std::list<unsigned long>::iterator> map;
};

class LRU_Deprecated {
public:
  LRU_Deprecated(int capacity_) : capacity(capacity_) {
    assert(capacity > 1);
  }

  bool find(unsigned long id) {
    return map.find(id) != map.cend();
  }
  bool access(unsigned long id) {
    bool ret = false;
    auto found = map.find(id);
    if (found != map.cend()) {
      auto node = found->second;
      assert(node);
      if (node == list->prev)
        list->prev = node->prev;
      node->remove();
      delete node;
      size--;
      ret = true;
    }
    auto node = new Node(id);
    map[id] = node;
    node->prev = node;
    node->next = list;
    if (list) {
      auto tail = list->prev;
      assert(tail);
      node->prev = tail;
      list->prev = node;
    }
    list = node;
    // std::cout << list->id << " " << list->prev->id << " "
    //           << (list->next ? list->next->id : -1) << std::endl;

    size++;
    if (size >= capacity) {
      assert(list);
      while (size >= capacity) {
        auto tail = list->prev;
        assert(tail);
        assert(tail != list);
        auto prev_tail = tail->prev;
        map.erase(tail->id);
        tail->remove();
        delete tail;
        size--;
        list->prev = prev_tail;
      }
    }
    return ret;
  }
private:
  struct Node {
    Node(unsigned long id_) : id(id_) {}
    unsigned long id;
    struct Node *prev = nullptr;
    struct Node *next = nullptr;

    void insert() {

    }
    void remove() {
      if (prev)
        prev->next = next;
      if (next)
        next->prev = prev;
    }
  };
  int size = 0;
  int capacity;
  Node *list = nullptr;
  std::unordered_map<unsigned long, Node *> map;
};

int main(int argc, char *argv[]) {
  if (argc <= 2) {
    std::cout << "Usage: ./lru.bin <skewness> <cache_ratio>" << std::endl;
    exit(-1);
  } else {
    kSkewness = std::stof(argv[1]);
    std::cout << "skewness: " << kSkewness << std::endl;
    kCacheRatio = std::stof(argv[2]);
    std::cout << "cache ratio: " << kCacheRatio << std::endl;
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  midas::zipf_table_distribution<> dist_zipf(kNumEles, kSkewness);

  std::vector<int> arr;
  for (int i = 0; i < kTolls; i++) {
    auto r = dist_zipf(gen) - 1;
    arr.push_back(r);
  }

  LRU lru(kNumEles * kCacheRatio);
  for (int i = 0; i < kNumEles; i++) {
    lru.access(i);
  }

  int nr_hits = 0;

  for (auto id : arr) {
    nr_hits += lru.access(id);
  }

  std::cout << "LRU replacement hit ratio: "
            << static_cast<float>(nr_hits) / kTolls << std::endl;

  return 0;
}
