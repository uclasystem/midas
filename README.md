# Midas
Cache as a service

## Dependencies
* gcc (>= 11)
* g++ (>= 11)
* Boost (>= 1.79.0)

## Compile
```bash
# C++ static library and test cases
make -j

# C library and bindings
cd bindings/c
make -j
make install # this will install the lib into bindings/c/lib
```
