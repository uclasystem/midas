CXXFLAGS += -std=c++1z -O2
CXXFLAGS += -march=native # required for avx-enhanced rmemcpy
# CXXFLAGS += -Wall -Wextra
# CXXFLAGS += -g -O0
CXX = g++
LDXX = g++

CFLAGS += -O2
# CFLAGS += -march=native
CC = gcc
LD = gcc

INC += -I../../inc -I./
INC += -I/mnt/ssd/yifan/tools/boost_1_79_0

override LDFLAGS += -lrt -lpthread
# For stacktrace logging
override LDFLAGS += -ldl

lib_src = $(wildcard *.cpp)
lib_obj = $(lib_src:.cpp=.o)

test_src = test.c
test_obj = $(test_src:.c=.o)

src = $(lib_src) $(test_src)
obj = $(lib_obj) $(test_obj)
dep = $(obj:.o=.d)

.PHONY: all clean

all: libmidas.a \
	test

test: $(test_obj) libmidas.a
	$(LD) -o $@ $^ ../../libmidas++.a $(LDFLAGS) -lstdc++

libmidas.a: $(lib_obj)
	$(AR) rcs $@ $^

%.o: %.cpp Makefile
	$(CXX) $(CXXFLAGS) $(INC) -MMD -MP -c $< -o $@

%.o: %.c Makefile
	$(CC) $(CFLAGS) $(INC) -MMD -MP -c $< -o $@

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

clean:
	$(RM) $(dep) $(obj) test
