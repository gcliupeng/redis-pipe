#ifndef EV_H
#define EV_H

#include "main.h"
#include <sys/epoll.h>

struct list;

#ifndef LIST_T 
typedef struct list list;
#define LIST_T
#endif

#ifndef CONTEX_T
   typedef struct server_contex_s server_contex;
#define CONTEX_T
#endif

enum 
{
	EVENT_READ = 0,
	EVENT_WRITE,
	EVENT_TIMEOUT,
	EVENT_CHECK,
};
typedef void (*callBack) (void *); 

struct event{
	int type; // 读、写、timeout
	long timeout; // 过期时间，毫秒
	callBack rcall; //回调函数
	callBack wcall; //回调函数
	callBack tcall; //回调函数
	int fd;
	int ractive;
	int wactive;
	server_contex * contex;
};



struct eventLoop{
	int efd;
	struct epoll_event list[500];
	list * timeoutList;
};
#ifndef EVENT_T
typedef struct event event;
typedef struct eventLoop eventLoop;
#define EVENT_T
#endif

eventLoop * eventLoopCreate();
int addEvent(eventLoop* loop,event *ev, int type);
void delEvent(eventLoop* loop,event *ev, int type);
void eventCycle(eventLoop* loop);
#endif