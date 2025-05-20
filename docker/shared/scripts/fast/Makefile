CXX        = g++
LANGFLAGS  = -std=c++23
OPTFLAGS   = -Ofast -march=core-avx2
WARNFLAGS  = -pedantic -Wall -Wextra -Weffc++
CXXFLAGS   = $(OPTFLAGS) $(LANGFLAGS) $(WARNFLAGS) -fPIC
LDFLAGS    = -shared
TARGET     = ./lib/libfast.so
SRC        = ./src/fast.cpp

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

.PHONY: clean
clean:
	$(RM) $(TARGET) *.o *~
