CXX = g++

INC = -Iinc
LIBS = -lredis++ -lhiredis -lcrypto -pthread


SRC_FILES=src/main.cpp
TOOL_FILES=tools/gen_feats.cpp

all: img2vec gen_feats

img2vec: $(SRC_FILES)
	g++ $^ -o $@ $(INC) $(LIBS)

gen_feats: $(TOOL_FILES)
	g++ $^ -o $@

clean:
	rm -rf img2vec
