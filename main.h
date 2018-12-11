#ifndef MAIN_H
#define MAIN_H

#include "config.h"
#include "buf.h"
#include "ev.h"
#include "array.h"
#include "struct.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <pthread.h>

#define REDIS_RUN_ID_SIZE 40

typedef struct{
	int port;
	char * ip;
	char * auth;
}redis_conf;

#ifndef CONTEX_T
typedef struct server_contex_s server_contex;
#define CONTEX_T
#endif

typedef struct{
	array * servers_from;
    array * servers_to;
    int logLevel;
    char * logfile;
    int lisentPort;

	char * prefix;
	char * removePre;
	char * filter;
	char * have;

	server_contex * contex;
}pipe_server;




#ifndef EVENT_T
typedef struct eventLoop eventLoop;
typedef struct event event;
#define EVENT_T
#endif

struct server_contex_s{
	pipe_server * sc;
	int pid;
	pthread_mutex_t mutex;
	int from_fd; //与from server的连接
	int to_fd; //与from server的连接
	int rdbfd;   
	char rdbfile[100];
	char aoffile[100];
	int aoffd;

	char * key;
	rvalue * value;
	long processed;
	int bucknum;
	buf_t * bufout;
	buf_t * bufoutLast;
	
	eventLoop *loop;
	event *from;
	event *to;
	long transfer_size;
	long transfer_read;
	int usemark;
	int version;
	int close;

	long key_length;
	long value_length;
	int type;
	int buffed;
	time_t expiretime;
	long long expiretimeM;

	int inputMode;
	int step;
	char * replicationBuf;
	int replicationBufSize;
	char * replicationBufPos;
	char * replicationBufPosPre;
	char * replicationBufLast;
	int lineSize;
};

void * transferFromServer(void *arg);
void * outPutLoop(void *arg);
void init_pool();
void logRaw(const char * function, int line, int level, const char * fmt, ...);

FILE *logfp;
int logLeve;
enum {
	LOG_DEBUG = 0,
	LOG_NOTICE,
	LOG_WARNING,
	LOG_ERROR
};

#define Log(level,format, ...)  logRaw(__FUNCTION__, __LINE__, level, format, ## __VA_ARGS__)
void initConf();
#endif