# SocialNet with Midas Support

This is the SocialNet microservice in the [DeathStarBench](https://github.com/delimitrou/DeathStarBench) suite. We port it to use soft memory with Midas based on commit [`1c3f3b6`](https://github.com/delimitrou/DeathStarBench/commit/1c3f3b63643110e40ee6160e945aeb39fee5eb7b).

Intersted users can find the original README [here](orig_README.md).

## Pre-requirements

* Docker
* Docker-compose
* Python 3.5+ (with asyncio and aiohttp)
* libssl-dev (apt-get install libssl-dev)
* libz-dev (apt-get install libz-dev)
* Midas (https://github.com/uclasystem/midas)

## Setup and Compile

We have provided a set of scripts under `run/` to faciliate the setup process.

1. We first need to compile the base docker image:

```bash
# Under SocialNet root dir (this dir):
./build.sh image  # This may take about 5 minutes to finish
```

This should create a new docker image locally named `socialnet_buildbase`:

```bash
docker image ls
## Sample outputs:
# REPOSITORY                   TAG       IMAGE ID       CREATED         SIZE
# socialnet_buildbase          latest    6dd159810afb   2 minutes ago   1.3GB
# ...
```

2. We then can compile SocialNet. NOTE that we must have Midas compiled and installed before this step.

To compile Midas:

```bash
./build.sh socialnet
./run/cp_services.sh
```

Now all compiled binaries are under `services/` dir.

## Run
1. First, follow the same instructions to start Midas daemon.

2. Launch all SocialNet microservices:

```bash
./run/up.sh
```

3. After all services are started, we can use the client to init/populate all involved databases with our datasets:
NOTE: since databases are persistent (under `dbs/`), we only need to init them once at the first time.

```bash
./client.sh init  # This is a long process that may take ~20-30 minutes to finish.
## Sample output looks like:
# docker exec -it socialnetwork_sn-client_1 /services/Client init
# user-service
# 9090 user-service 10000 done
# compose-post-service
# 9090 compose-post-service 10000 done
# home-timeline-service
# 9090 home-timeline-service 10000 done
# user-timeline-service
# 9090 user-timeline-service 10000 done
# social-graph-service
# 9090 social-graph-service 10000 done
# 465017 835423
# Register 100 users...
# Register 10100 users...
# Register 20100 users...
# ...
```

NOTE: only the first time needs to run this command. Databases are stored under `dbs/` after intialization. Users can delete the `dbs/` directory to reset the database state.

4. We then can run real workloads with the client:

```bash
./run/client warmup
```

Here adding the warmup option will run an additionally phase before the real load to warm up services. It is recommended to add this option to get more stable results.
Sample outputs may look like:

```txt
docker exec -it socialnetwork_sn-client_1 /services/Client warmup
user-service
9090 user-service 100000 done
compose-post-service
9090 compose-post-service 100000 done
home-timeline-service
9090 home-timeline-service 100000 done
user-timeline-service
9090 user-timeline-service 100000 done
social-graph-service
9090 social-graph-service 100000 done
41536 1362220
Start reading posts (user timelines)...
Finished users: 100
Finished users: 10100
Finished users: 20100
Finished users: 30100
Finished users: 40100
Read posts (user timelines) duration: 4560ms
[Info] Start generating requests...
[Info] Finish generating warmup requests...
[Info] Finish generating perf requests...
[Info] Tput: 400.584 Kops
Timer initialized, CPU Freq: 2095MHz
[Info] Start generating requests...
[Info] Finish generating warmup requests...
[Info] Finish generating perf requests...
[Info] Tput: 19.868 Kops
[Info] Tput: 0.002 Kops
Real Tput: 19.8912 Kops
P99 Latency: 622 us
raw: 10 622     19.891
...
```
