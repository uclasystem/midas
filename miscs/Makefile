CXXFLAGS += -std=c++1z -O2 -g
# CXXFLAGS += -Wall -Wextra
# CXXFLAGS += -O0
CXX = /mnt/ssd/yifan/tools/gcc10/bin/g++
LDXX = /mnt/ssd/yifan/tools/gcc10/bin/g++

INC += -Iinc

src = $(*.cpp)
obj = $(src:.cpp=.o)
target = $(src:.cpp=.bin)
dep = $(obj:.o=.d)

.PHONY: all clean

all: missrate.bin fault_handle

%.bin: %.o
	$(LDXX) -o $@ $^ $(LDFLAGS)

%.o: %.cpp Makefile
	$(CXX) $(CXXFLAGS) $(INC) -MMD -MP -c $< -o $@

fault_handle: fault_handler.cpp
	$(CXX) $(CXXFLAGS) $(INC) $^ -o $@ -ldl

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

clean:
	$(RM) $(dep) $(obj) *.bin fault_handle
