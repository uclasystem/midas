CXX = ccache g++
LDXX = ccache g++

# CXX = g++
# LDXX = g++
CXXFLAGS += -std=c++1z -O2
# CXXFLAGS += -g -O0

INC += -I../../inc
libmidas_lib := ../../lib/libmidas++.a -lrt -ldl -lpthread
LIBS += $(libmidas_lib)

# For stacktrace logging
LIBS += -rdynamic -ggdb -no-pie -fno-pie
LIBS += -ldl

BUILD_DIR = build

SRC = $(wildcard *.cpp)
DEP = $(SRC:%.cpp=$(BUILD_DIR)/%.d)
TARGET = $(SRC:%.cpp=$(BUILD_DIR)/%)

# all: softptr_read_small softptr_read_large softptr_write_small softptr_write_large \
# 	 unique_ptr_read_small unique_ptr_read_large unique_ptr_write_small unique_ptr_write_large

all: $(TARGET)

$(BUILD_DIR)/% : %.cpp Makefile
	@mkdir -p $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(INC) -MMD -MP $< $(LIBS) -o $@

ifneq ($(MAKECMDGOALS),clean)
-include $(DEP)
endif

.PHONY : clean
clean :
	-rm -rf $(TARGET)