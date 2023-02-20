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
## Run
To run any application or test, we first need to create a memory configuration file under `config/mem.config`. For now we echo #(bytes) into the file to simulate a memory pressure:

```bash
touch config/mem.config
echo $((1024*1024*1024)) > config/mem.config # 1GB
```

Then we start the daemon, which coordinates the memory usage of applications. The daemon uses boost shared memory files to communiate with applications, so we enlarge the open file limit of current shell, then start the daemon:

```bash
ulimit -n 1024000
./bin/deamon_main
# It should output something like:
# [kInfo](daemon/daemon_types.cpp:251, in serve()): Daemon starts listening...
```

Finally, we can run a test case. Here is an example:

```bash
./bin/test_feat_extractor 0.5 # run test_feat_extractor with 0.5 cache ratio
```

We should be able to see connection/disconnection logs from the daemon side. A sample output of the test program looks like:

```txt
Perf done. Duration: 35853 ms, Throughput: 6.694 Kops
Cache hit ratio = 210803/240000 = 0.878346
Test passed!
```