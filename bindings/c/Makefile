CXXFLAGS += -std=c++1z -O2
CXXFLAGS += -march=native # required for avx-enhanced rmemcpy
CXXFLAGS += -fPIC
# CXXFLAGS += -Wall -Wextra
# CXXFLAGS += -g -O0
CXX = g++
LDXX = g++

CFLAGS += -O2 -fPIC
# CFLAGS += -march=native
CC = gcc
LD = gcc

INC += -I../../inc -I./include
INC += -I/mnt/ssd/yifan/tools/boost_1_79_0

# override LDFLAGS += -static -static-libstdc++ -static-libgcc
override LDFLAGS += -lrt -pthread
# For stacktrace logging
override LDFLAGS += -ldl

root_dir = ../..
lib_src = $(wildcard $(root_dir)/src/*.cpp)
lib_src := $(filter-out $(wildcard src/*main.cpp),$(lib_src))
lib_obj = $(lib_src:.cpp=.o)

bindings_src = $(wildcard *.cpp)
bindings_obj = $(bindings_src:.cpp=.o)

test_src = test.c
test_obj = $(test_src:.c=.o)

src = $(bindings_src) $(test_src)
obj = $(bindings_obj) $(test_obj)
dep = $(obj:.o=.d)

.PHONY: all clean install

target = lib/libmidas.a \
	 test

# libmidas.so \

all: $(target)

test: $(test_obj) lib/libmidas.a
	$(LD) -o $@ $^ $(LDFLAGS) -lstdc++

lib/libmidas.a: $(bindings_obj) $(lib_obj)
	mkdir -p lib
	$(AR) rcs $@ $^

libmidas.so: $(bindings_obj) $(lib_obj)
	$(LD) -o $@ $^ -shared $(LDFLAGS)

%.o: %.cpp Makefile
	$(CXX) $(CXXFLAGS) $(INC) -MMD -MP -c $< -o $@

%.o: %.c Makefile
	$(CC) $(CFLAGS) $(INC) -MMD -MP -c $< -o $@

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

clean:
	$(RM) $(dep) $(obj) $(target)
