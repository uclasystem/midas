# CXXFLAGS += -std=gnu++20
CXX = g++
LDXX = g++

INC += -Iinc
INC += -I/home/yifan/tools/boost_1_79_0

override LDFLAGS += -lrt -lpthread

lib_src = $(wildcard src/*.cpp)
lib_src := $(filter-out $(wildcard src/*main.cpp),$(lib_src))
lib_obj = $(lib_src:.cpp=.o)

src = $(lib_src)
obj = $(src:.cpp=.o)
dep = $(obj:.o=.d)

daemon_main_src = src/daemon_main.cpp
daemon_main_obj = $(daemon_main_src:.cpp=.o)
test_resource_manager_src = test/test_resource_manager.cpp
test_resource_manager_obj = $(test_resource_manager_src:.cpp=.o)
test_slab_src = test/test_slab.cpp
test_slab_obj = $(test_slab_src:.cpp=.o)
test_sync_hashmap_src = test/test_sync_hashmap.cpp
test_sync_hashmap_obj = $(test_sync_hashmap_src:.cpp=.o)

all: bin/daemon_main bin/test_resource_manager bin/test_slab bin/test_sync_hashmap

%.d: %.cpp
	$(CXX) $(CXXFLAGS) $(INC) $< -MM -MT $(@:.d=.o) >$@
%.o: %.cpp
	$(CXX) $(CXXFLAGS)  $(INC) -c $< -o $@

bin/daemon_main: $(daemon_main_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_resource_manager: $(test_resource_manager_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_slab: $(test_slab_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

bin/test_sync_hashmap: $(test_sync_hashmap_obj) $(lib_obj)
	$(LDXX) -o $@ $^ $(LDFLAGS)

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

.PHONY: clean
clean:
	rm -f $(dep) src/*.o test/*.o bin/*
