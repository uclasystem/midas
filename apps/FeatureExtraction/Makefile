CXX = g++
LDXX = g++
CXXFLAGS += -std=c++1z -O2
CXXFLAGS += -march=native # required for avx-enhanced rmemcpy

INC += -Iinc
LIBS += -lredis++ -lhiredis -lcrypto -pthread

# libmidas_lib = $(wildcard ../../src/*.o)
# libmidas_lib := $(filter-out $(wildcard ../../src/daemon_main.o),$(libmidas_lib))

APP_SRC = $(wildcard src/*.cpp)
APP_OBJ = $(APP_SRC:.cpp=.o)
TOOL_SRC = $(wildcard tools/*.cpp)
TOOL_OBJ = $(TOOL_SRC:.cpp=.o)

all: img2vec gen_feats

img2vec: $(APP_SRC)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(libmidas_lib) $(INC) $(LIBS)

gen_feats: $(TOOL_SRC)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(libmidas_lib) $(INC) $(LIBS)

clean:
	rm -rf img2vec gen_feats $(APP_OBJ) $(TOOL_OBJ)
