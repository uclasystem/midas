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

INC += -I../../inc -I./
INC += -I/mnt/ssd/yifan/tools/boost_1_79_0

override LDFLAGS += -lrt -lpthread
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

all: libmidas.a \
	libmidas.so \
	test

test: $(test_obj) libmidas.a
	$(LD) -o $@ $^ $(LDFLAGS) -lstdc++

libmidas.a: $(bindings_obj) $(lib_obj)
	$(AR) rcs libmidas.a $^

libmidas.so: $(bindings_obj) $(lib_obj)
	$(LD) -o $@ $^ -shared $(LDFLAGS)

%.o: %.cpp Makefile
	$(CXX) $(CXXFLAGS) $(INC) -MMD -MP -c $< -o $@

%.o: %.c Makefile
	$(CC) $(CFLAGS) $(INC) -MMD -MP -c $< -o $@

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

CP = cp
inc_src = $(wildcard *.h)
install:
	mkdir -p include
	$(CP) $(inc_src) include/
	mkdir -p lib
	$(CP) libmidas.a lib/

clean:
	$(RM) $(dep) $(obj) test
