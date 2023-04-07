CXX = ccache g++
LDXX = ccache g++
CXXFLAGS += -std=c++1z -O2
CXXFLAGS += -march=native # required for avx-enhanced rmemcpy

BUILD_DIR = bin

# INC += -Ithird_party/json/build/
# INC += -I/usr/include/mysql-cppconn-8/
# LIBS += -lmysqlcppconn8

INC += -I../../inc
libmidas_lib := ../../lib/libmidas++.a -lrt -ldl
LIBS += $(libmidas_lib)

INC += -Iinc
LIBS += -lcrypto -pthread

SRC = $(wildcard src/*.cpp)

APP_BIN = hdsearch
APP_SRC = $(SRC)
APP_OBJ = $(APP_SRC:%.cpp=$(BUILD_DIR)/%.o)
APP_DEP = $(APP_OBJ:.o=.d)

all: $(APP_BIN)

$(APP_BIN) : $(BUILD_DIR)/$(APP_BIN)

$(BUILD_DIR)/$(APP_BIN) : $(APP_OBJ)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LIBS)

$(BUILD_DIR)/%.o : %.cpp Makefile
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $(INC) -MMD -MP -c $< -o $@

ifneq ($(MAKECMDGOALS),clean)
-include $(APP_DEP)
endif

.PHONY : clean
clean :
	-rm -rf $(BUILD_DIR)/$(APP_BIN) $(APP_OBJ) $(APP_DEP)