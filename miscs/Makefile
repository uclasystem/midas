CXXFLAGS += -std=c++1z -O2
# CXXFLAGS += -Wall -Wextra
# CXXFLAGS += -g -O0
CXX = /mnt/ssd/yifan/tools/gcc10/bin/g++
LDXX = /mnt/ssd/yifan/tools/gcc10/bin/g++

INC += -Iinc

src = $(*.cpp)
obj = $(src:.cpp=.o)
target = $(src:.cpp=.bin)
dep = $(obj:.o=.d)

.PHONY: all clean

all: missrate.bin

%.bin: %.o
	$(LDXX) -o $@ $^ $(LDFLAGS)

%.o: %.cpp Makefile
	$(CXX) $(CXXFLAGS) $(INC) -MMD -MP -c $< -o $@

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

clean:
	$(RM) $(dep) $(obj) *.bin
