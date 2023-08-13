CXXFLAGS += -std=c++1z -O2
CXXFLAGS += -march=native # required for avx-enhanced rmemcpy
CXXFLAGS +=  -fPIC
# CXXFLAGS += -Wall -Wextra
# CXXFLAGS += -g -O0
CXX = g++
LDXX = g++

INC += -Iinc

override LDFLAGS += -lrt -lpthread
# For stacktrace logging
override LDFLAGS += -rdynamic
override LDFLAGS += -ldl

lib_src = $(wildcard src/*.cpp)
lib_src := $(filter-out $(wildcard src/*main.cpp),$(lib_src))
lib_obj = $(lib_src:.cpp=.o)

test_src = $(wildcard test/test_*.cpp)
# test_src := $(filter-out $(wildcard test/boost*.cpp),$(test_src))
test_target = $(test_src:.cpp=)

daemon_src = $(wildcard daemon/*.cpp)
daemon_obj = $(daemon_src:.cpp=.o)

src = $(lib_src) $(test_src) $(daemon_src)
obj = $(src:.cpp=.o)
dep = $(obj:.o=.d)

daemon_main_src = $(daemon_src)
daemon_main_obj = $(daemon_obj)
test_resource_manager_src = test/test_resource_manager.cpp
test_resource_manager_obj = $(test_resource_manager_src:.cpp=.o)
test_object_src = test/test_object.cpp
test_object_obj = $(test_object_src:.cpp=.o)
test_slab_src = test/test_slab.cpp
test_slab_obj = $(test_slab_src:.cpp=.o)
test_sync_hashmap_src = test/test_sync_hashmap.cpp
test_sync_hashmap_obj = $(test_sync_hashmap_src:.cpp=.o)
test_sync_list_src = test/test_sync_list.cpp
test_sync_list_obj = $(test_sync_list_src:.cpp=.o)
test_hashmap_clear_src = test/test_hashmap_clear.cpp
test_hashmap_clear_obj = $(test_hashmap_clear_src:.cpp=.o)
test_log_src = test/test_log.cpp
test_log_obj = $(test_log_src:.cpp=.o)
test_large_alloc_src = test/test_large_alloc.cpp
test_large_alloc_obj = $(test_large_alloc_src:.cpp=.o)
test_parallel_evacuator_src = test/test_parallel_evacuator.cpp
test_parallel_evacuator_obj = $(test_parallel_evacuator_src:.cpp=.o)
test_concurrent_evacuator_src = test/test_concurrent_evacuator.cpp
test_concurrent_evacuator_obj = $(test_concurrent_evacuator_src:.cpp=.o)
test_concurrent_evacuator2_src = test/test_concurrent_evacuator2.cpp
test_concurrent_evacuator2_obj = $(test_concurrent_evacuator2_src:.cpp=.o)
test_concurrent_evacuator3_src = test/test_concurrent_evacuator3.cpp
test_concurrent_evacuator3_obj = $(test_concurrent_evacuator3_src:.cpp=.o)
test_skewed_hashmap_src = test/test_skewed_hashmap.cpp
test_skewed_hashmap_obj = $(test_skewed_hashmap_src:.cpp=.o)
test_sighandler_src = test/test_sighandler.cpp
test_sighandler_obj = $(test_sighandler_src:.cpp=.o)
test_memcpy_src = test/test_memcpy.cpp
test_memcpy_obj = $(test_memcpy_src:.cpp=.o)
test_cache_manager_src = test/test_cache_manager.cpp
test_cache_manager_obj = $(test_cache_manager_src:.cpp=.o)
test_victim_cache_src = test/test_victim_cache.cpp
test_victim_cache_obj = $(test_victim_cache_src:.cpp=.o)
test_sync_kv_src = test/test_sync_kv.cpp
test_sync_kv_obj = $(test_sync_kv_src:.cpp=.o)
test_ordered_set_src = test/test_ordered_set.cpp
test_ordered_set_obj = $(test_ordered_set_src:.cpp=.o)
test_batched_kv_src = test/test_batched_kv.cpp
test_batched_kv_obj = $(test_batched_kv_src:.cpp=.o)
test_fs_shim_src = test/test_fs_shim.cpp
test_fs_shim_obj = $(test_fs_shim_src:.cpp=.o)
test_softptr_read_cost_src = test/test_softptr_read_cost.cpp
test_softptr_read_cost_obj = $(test_softptr_read_cost_src:.cpp=.o)
test_softptr_write_cost_src = test/test_softptr_write_cost.cpp
test_softptr_write_cost_obj = $(test_softptr_write_cost_src:.cpp=.o)

test_feat_extractor_src = test/test_feat_extractor.cpp
test_feat_extractor_obj = $(test_feat_extractor_src:.cpp=.o)
test_feat_extractor_kv_src = test/test_feat_extractor_kv.cpp
test_feat_extractor_kv_obj = $(test_feat_extractor_kv_src:.cpp=.o)

.PHONY: all bin lib daemon clean

all: bin lib daemon

lib: lib/libmidas++.a

bin: bin/test_resource_manager bin/test_object bin/test_parallel_evacuator \
	bin/test_log bin/test_large_alloc \
	bin/test_sync_hashmap bin/test_hashmap_clear bin/test_sync_list \
	bin/test_cache_manager bin/test_victim_cache \
	bin/test_sync_kv bin/test_ordered_set bin/test_batched_kv \
	bin/test_skewed_hashmap \
	bin/test_fs_shim \
	bin/test_sighandler \
	bin/test_memcpy \
	bin/test_softptr_read_cost bin/test_softptr_write_cost

# bin/test_feat_extractor bin/test_feat_extractor_kv
# bin/test_concurrent_evacuator bin/test_concurrent_evacuator2 bin/test_concurrent_evacuator3

daemon: bin/daemon_main

bin/daemon_main: $(daemon_main_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_resource_manager: $(test_resource_manager_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_object: $(test_object_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_slab: $(test_slab_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_sync_hashmap: $(test_sync_hashmap_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_sync_list: $(test_sync_list_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_hashmap_clear: $(test_hashmap_clear_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_log: $(test_log_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_large_alloc: $(test_large_alloc_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_parallel_evacuator: $(test_parallel_evacuator_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_concurrent_evacuator: $(test_concurrent_evacuator_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_concurrent_evacuator2: $(test_concurrent_evacuator2_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_concurrent_evacuator3: $(test_concurrent_evacuator3_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_skewed_hashmap: $(test_skewed_hashmap_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_sighandler: $(test_sighandler_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_memcpy: $(test_memcpy_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_cache_manager: $(test_cache_manager_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_victim_cache: $(test_victim_cache_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_sync_kv: $(test_sync_kv_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_ordered_set: $(test_ordered_set_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_batched_kv: $(test_batched_kv_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_fs_shim: $(test_fs_shim_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_softptr_read_cost: $(test_softptr_read_cost_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_softptr_write_cost: $(test_softptr_write_cost_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

lib/libmidas++.a: $(lib_obj)
	mkdir -p lib
	$(AR) rcs $@ $^

# bin/test_feat_extractor: $(test_feat_extractor_obj) $(lib_obj)
# 	$(LDXX) -o $@ $^ -lcrypto $(LDFLAGS)

# bin/test_feat_extractor_kv: $(test_feat_extractor_kv_obj) $(lib_obj)
# 	$(LDXX) -o $@ $^ -lcrypto $(LDFLAGS)

%.o: %.cpp Makefile
	$(CXX) $(CXXFLAGS) $(INC) -MMD -MP -c $< -o $@

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

clean:
	$(RM) $(dep) $(obj) bin/*
