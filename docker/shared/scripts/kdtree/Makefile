CXX       = g++
CXXFLAGS  = -std=c++23 -Ofast -march=core-avx2 -pedantic -Wall -Wextra -Weffc++ -fPIC
LDFLAGS   = -shared

TARGET    = ./lib/libfast.so
SRC       = ./src/fastlib.cpp        

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

.PHONY: clean
clean:
	$(RM) $(TARGET) *.o *~
