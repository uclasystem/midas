all: daemon runtime

CXX = g++

INC += -Iinc
INC += -I/home/yifan/tools/boost_1_79_0

# CXXFLAGS += -std=gnu++20

daemon: src/daemon.cpp
	$(CXX) $(CXXFLAGS) $(INC) -o $@ $^ -lrt -pthread

runtime: src/runtime.cpp
	$(CXX) $(CXXFLAGS) $(INC) -o $@ $^ -lrt -pthread
