CXX = g++
LDXX = g++
CXXFLAGS += -std=c++1z -O2
CXXFLAGS += -march=native # required for avx-enhanced rmemcpy

BUILD_DIR = build

INC += -Ithird_party/json/build/
# INC += -I/usr/include/mysql-cppconn-8/
# LIBS += -lmysqlcppconn8

INC += -I../../inc
libmidas_lib := ../../lib/libmidas++.a -lrt -ldl
LIBS += $(libmidas_lib)

INC += -Iinc
LIBS += -pthread

SRC = $(wildcard src/*.cpp)

RH_APP_BIN = rh_app
RH_APP_SRC = $(SRC) $(wildcard rh_server/*.cpp)
RH_APP_OBJ = $(RH_APP_SRC:%.cpp=$(BUILD_DIR)/%.o)
RH_APP_DEP = $(RH_APP_OBJ:.o=.d)

all: $(RH_APP_BIN)

$(RH_APP_BIN) : $(BUILD_DIR)/$(RH_APP_BIN)

$(BUILD_DIR)/$(RH_APP_BIN) : $(RH_APP_OBJ)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LIBS)

$(BUILD_DIR)/%.o : %.cpp Makefile
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $(INC) -MMD -MP -c $< -o $@

ifneq ($(MAKECMDGOALS),clean)
-include $(RH_APP_DEP)
endif

.PHONY : clean
clean :
	-rm -rf $(BUILD_DIR)/$(RH_APP_BIN) $(RH_APP_OBJ) $(RH_APP_DEP)