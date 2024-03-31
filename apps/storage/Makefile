CXX = g++
LDXX = g++
CXXFLAGS += -std=c++1z -O2
CXXFLAGS += -march=native # required for avx-enhanced rmemcpy
# CXXFLAGS += -g -O0
CXXFLAGS += -rdynamic

INC += -I../../inc
libmidas_lib := ../../lib/libmidas++.a -lrt -ldl
LIBS += $(libmidas_lib)

INC += -Iinc
LIBS += -pthread

APP_BIN = storage_server
APP_SRC = server.cpp
APP_OBJ = $(APP_SRC:%.cpp=%.o)
APP_DEP = $(APP_OBJ:.o=.d)

all: $(APP_BIN)

$(APP_BIN) : $(APP_OBJ)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LIBS)

%.o : %.cpp Makefile
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $(INC) -MMD -MP -c $< -o $@

ifneq ($(MAKECMDGOALS),clean)
-include $(APP_DEP)
endif

.PHONY : clean
clean :
	-rm -rf $(APP_BIN) $(APP_OBJ) $(APP_DEP)