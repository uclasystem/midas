#pragma once

#include "configs.h"
#include <iostream>
#include <string>
#include <mutex>

extern "C" {
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <arpa/inet.h>

#define MAXDATASIZE 1024 // max number of bytes we can get at once

// get sockaddr, IPv4 or IPv6:
inline void *get_in_addr(struct sockaddr *sa) {
  if (sa->sa_family == AF_INET) {
    return &(((struct sockaddr_in *)sa)->sin_addr);
  }

  return &(((struct sockaddr_in6 *)sa)->sin6_addr);
}

inline int connect_socket(const char *host, const char *port) {
  int sockfd = -1;
  int numbytes;
  struct addrinfo hints, *servinfo, *p;
  int rv;
  char s[INET6_ADDRSTRLEN];

  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if ((rv = getaddrinfo(host, port, &hints, &servinfo)) != 0) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
    return -1;
  }

  // loop through all the results and connect to the first we can
  for (p = servinfo; p != NULL; p = p->ai_next) {
    if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
      perror("client: socket");
      continue;
    }

    if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
      close(sockfd);
      perror("client: connect");
      continue;
    }

    break;
  }

  if (p == NULL) {
    fprintf(stderr, "client: failed to connect\n");
    return -2;
  }

  inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr), s,
            sizeof s);
  // printf("client: connecting to %s\n", s);

  freeaddrinfo(servinfo); // all done with this structure
  return sockfd;
}

inline int close_socket(int sockfd) {
  close(sockfd);
  return 0;
}

inline int send_socket(int sockfd, const char *data, size_t len) {
  int numbytes;
  char buf[MAXDATASIZE];
  if ((numbytes = send(sockfd, data, len, 0)) == -1) {
    perror("send");
    exit(-1);
  }
  return numbytes;
}

inline int recv_socket(int sockfd) {
  int numbytes;
  char buf[MAXDATASIZE];
  if ((numbytes = recv(sockfd, buf, MAXDATASIZE - 1, 0)) == -1) {
    perror("recv");
    exit(1);
  }

  buf[numbytes] = '\0';

  return 0;
}

static int socket_fd;
inline int initInfClient(const char *host, const char *port) {
  socket_fd = connect_socket(host, port);
  return socket_fd;
}

inline int send_recv_socket(const char *data, size_t len) {
  int numbytes;
  char buf[MAXDATASIZE];
  if ((numbytes = send(socket_fd, data, len, 0)) == -1) {
    perror("send");
    exit(-1);
  }

  if ((numbytes = recv(socket_fd, buf, MAXDATASIZE - 1, 0)) == -1) {
    perror("recv");
    exit(1);
  }

  buf[numbytes] = '\0';
  return numbytes;
}
}

class Socket {
public:
  Socket(const std::string &host = "localhost",
         const std::string &port = "10080")
      : socketfd(0), _host(host), _port(port) {
    init();
  }
  ~Socket() { close(); }

  void send(const std::string &msg) {
    std::unique_lock<std::mutex> lk(mtx);
    send_socket(socketfd, msg.c_str(), msg.length());
  }
  const std::string recv() {
    std::unique_lock<std::mutex> lk(mtx);
    recv_socket(socketfd);
    return "";
  }

  const std::string send_recv(const std::string &msg) {
    std::unique_lock<std::mutex> lk(mtx);
    send_socket(socketfd, msg.c_str(), msg.length());
    recv_socket(socketfd);
    return "";
  }

private:
  void init() {
    std::unique_lock<std::mutex> lk(mtx);
    if (kSimulate)
      socketfd = 1;
    if (socketfd)
      return;
    socketfd = connect_socket(_host.c_str(), _port.c_str());
    std::cout << "socketfd: " << socketfd << std::endl;
    if (socketfd <= 0) {
      std::cerr << "Init inference client failed! " << socketfd << std::endl;
      exit(-1);
    }
  }

  void close() {
    std::unique_lock<std::mutex> lk(mtx);
    close_socket(socketfd);
  }

  std::mutex mtx;
  int socketfd;
  const std::string _host;
  const std::string _port;
};