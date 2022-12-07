CXX = g++
LDXX = g++
CXXFLAGS += -std=c++1z -O2
CXXFLAGS += -march=native # required for avx-enhanced rmemcpy

BUILD_DIR = build

INC += -Iinc
LIBS += -lredis++ -lhiredis -lcrypto -pthread

APP_BIN = img2vec
APP_SRC = $(wildcard src/*.cpp)
APP_OBJ = $(APP_SRC:%.cpp=$(BUILD_DIR)/%.o)
APP_DEP = $(APP_OBJ:.o=.d)

TOOL_BIN = gen_feats
TOOL_SRC = $(wildcard tools/*.cpp)
TOOL_OBJ = $(TOOL_SRC:%.cpp=$(BUILD_DIR)/%.o)
TOOL_DEP = $(TOOL_OBJ:.o=.d)

all: $(APP_BIN) $(TOOL_BIN)

$(APP_BIN) : $(BUILD_DIR)/$(APP_BIN)
$(TOOL_BIN) : $(BUILD_DIR)/$(TOOL_BIN)

$(BUILD_DIR)/$(APP_BIN) : $(APP_OBJ)
	mkdir -p $(@D)
	$(CXX) $(CXX_FLAGS) $^ -o $@ $(LIBS)

$(BUILD_DIR)/$(TOOL_BIN) : $(TOOL_OBJ)
	mkdir -p $(@D)
	$(CXX) $(CXX_FLAGS) $^ -o $@ $(LIBS)

$(BUILD_DIR)/%.o : %.cpp Makefile
	mkdir -p $(@D)
	$(CXX) $(CXX_FLAGS) $(INC) -MMD -MP -c $< -o $@

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

.PHONY : clean
clean :
	-rm -rf $(BUILD_DIR)/$(BIN) $(OBJ) $(DEP)