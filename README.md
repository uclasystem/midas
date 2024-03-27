# Midas

Midas is a memory management system that enables applications to efficiently and safely harvest idle memory to store their soft state (e.g., caches, memoization results, etc.). It frees the developers from the burden of manually managing applications' soft state and stores the soft state the is most beneficial to each application, thereby improving both application performance and memory utilization.

Currently, Midas supports C and C++ applications on Linux. Please refer to the [NSDI'24 paper](https://www.usenix.org/conference/nsdi24/presentation/qiao) for technical details about Midas.


## Getting Started

### System Requirements
Midas' code base assumes an x86-64 CPU architecture and the system as well as applications are running atop Linux. Midas has been extensively tested on Ubuntu 18.04 and 20.04, but it should support other Linux distributions as well.

### Dependencies

Midas is developed in C++ and requires compilers that support C++ 17 (`std=c++1z`). It has the following dependencies:

* gcc (>= 11)
* g++ (>= 11)
* Boost (>= 1.79.0)

### Compile Midas
We have provided a push-button script to build Midas:
```bash
./scripts/build.sh
```
The script will build the Midas library, the Midas coordinator (daemon), and all unit tests. The compile time is usually less than one minute.

Users can also build Midas manually:
```bash
# Midas C++ static library, Midas coordinator (daemon), and unit tests.
make -j

# Midas C library and bindings
cd bindings/c
make -j
make install # this will install the lib into bindings/c/lib
```

### Compile Ported Applications
We have ported several applications to Midas under `apps`. We also offered the `apps/build_all.sh` script for users to automatically build them.

```bash
cd apps/
./build_all.sh
# The compiled executables are under each application's directory, respectively.
```

### Run
Before running any application, one needs to start the Midas coordinator on the host machine:
```bash
./scripts/run_daemon.sh
# Example terminal outputs:
# [kInfo]: Daemon starts listening...
```
By default, the Midas coordinator can harvest all available memory on the server. This offers the most utility to applications and it is safe because Midas only allocates memory on demand and can promptly scale down its memory usage upon memory pressure. However, users can set a hard limit on the maximum memory capacity that Midas can harvest:
```bash
./scripts/set_memory_limit.sh <memory limit in MB>
```

We are free to run applications now. Here we take the storage server as an example.
```bash
cd apps/storage
./storage_server
```

## Repo Structure

```txt
Github Repo Root
├── apps        # Ported applications.
├── bin         # Compiled unit tests.
├── bindings    # Bindings for the other languages. Currently only C is supported.
├── config      # Configuration files for the Midas coordinator(daemon).
├── daemon      # Midas coordinator that runs as a daemon process.
├── exp         # Scripts and instructions to reproduce results in the paper.
├── inc         # Midas header files.
├── lib         # Compiled Midas library.
├── LICENSE     # Midas is open-sourced under the Apache-2.0 License.
├── Makefile    # For building the Midas library and unit tests.
├── scripts     # Scripts to faciliate developing and using Midas.
├── src         # Midas source code.
└── test        # Unit tests for individual Midas components.
```

## Contact
Please contact [Yifan Qiao](yifanqiao@g.ucla.edu) for any questions.