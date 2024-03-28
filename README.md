# Midas
![Status](https://img.shields.io/badge/Version-Experimental-green.svg)
[![Build](https://github.com/ivanium/cachebank/actions/workflows/build.yml/badge.svg)](https://github.com/ivanium/cachebank/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


Midas is a memory management system that enables applications to efficiently and safely harvest idle memory to store their soft state (e.g., caches, memoization results, etc.). It frees the developers from the burden of manually managing applications' soft state and stores the soft state that is most beneficial to each application, thereby improving both application performance and memory utilization.

Midas currently supports C and C++ applications on Linux, with several real-world applications we have ported serving as practical references to demonstrate its simplicity and effectiveness.
For technical details about Midas, please refer to the [NSDI'24 paper](https://www.usenix.org/conference/nsdi24/presentation/qiao).


## Getting Started

### System Requirements
Midas' code base assumes an x86-64 CPU architecture and the system is designed to run on Linux. Midas has been extensively tested on Ubuntu 18.04, 20.04, and 22.04, but it should also be compatible with other Linux distributions.

### Dependencies

Midas is developed in C++ and requires compilers with C++ 17 support (`std=c++1z`). To deploy Midas, the following dependencies are required:

* gcc (>= 11)
* g++ (>= 11)
* Boost (>= 1.79.0)

### Compile Midas
We have provided a push-button script to build Midas:
```bash
./scripts/build.sh
```
The script will build the Midas library, the Midas coordinator (daemon), and all associated unit tests. This compilation typically completes within less than one minute.

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
We have also ported several applications to Midas under the `apps` directory. We have also offered the `apps/build_all.sh` script for automated compilation.

```bash
cd apps/
./build_all.sh
# The compiled executables are under each application's directory, respectively.
```

### Run
Before running any application, one needs to start the Midas coordinator on the host machine. The coordinator is responsible for managing the memory allocation and coordinate all running applications.

```bash
./scripts/run_daemon.sh
# Example terminal outputs:
# [kInfo]: Daemon starts listening...
```
By default, the Midas coordinator is configured to utilize all available memory on the server. This is recommended because it maximized the utility for applications. However, for users who wish to manage system resources more conservatively, there is an option to set a hard limit on the maximum memory capacity that Midas can harvest:
```bash
./scripts/set_memory_limit.sh <memory limit in MB>
```

With the Midas coordinator running, users can launch applications and take advantage of soft state. Below is an example of how to start a storage server application ported to work with Midas:
```bash
cd apps/storage
./setup_disk.sh  # Setup the disk to be served.
./storage_server
```
Users can run more applications following a similar process.

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
├── koord       # Midas coordinator kernel module.
├── lib         # Compiled Midas library.
├── LICENSE     # Midas is open-sourced under the Apache-2.0 License.
├── Makefile    # For building the Midas library and unit tests.
├── scripts     # Scripts to faciliate developing and using Midas.
├── src         # Midas source code.
└── test        # Unit tests for individual Midas components.
```

## Contact
Please contact [Yifan Qiao](mailto:yifanqiao@g.ucla.edu) for any questions.