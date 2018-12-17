#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include "ev.h"
#include "list.h"
#include "main.h"

eventLoop * eventLoopCreate(){
	int efd = epoll_create(500);
	if(efd < 0){
		Log(LOG_ERROR, "epoll_create error");
		return NULL;
	}
	eventLoop * ev =malloc(sizeof(*ev));
	if(!ev){
		Log(LOG_ERROR,"malloc error");
		return NULL;
	}
	list * newl = newList();
	ev->timeoutList = newl;
	if(!ev->timeoutList){
		free(ev);
		Log(LOG_ERROR,"malloc error");
		return NULL;
	}
	ev->efd = efd;
	return ev;
}

int addEvent(eventLoop* loop,event *ev, int type){
	int op;
	//printf("type %d\n",type );
	if((type == EVENT_READ && ev->ractive) || (type == EVENT_WRITE && ev->wactive)){
		return 1;
	}
	//printf("come here addEvent\n");
	struct epoll_event e;
	long long timeout ;
	switch(type){
		case EVENT_READ:
				if(ev->wactive){
					op = EPOLL_CTL_MOD;
					e.events = EPOLLIN | EPOLLOUT;
				}else{
					op = EPOLL_CTL_ADD;
					e.events = EPOLLIN;
				}
				e.data.ptr = ev; 
				ev->ractive = 1;
				return epoll_ctl(loop->efd, op, ev->fd, &e);
				break;
		case EVENT_WRITE:
				if(ev->ractive){
					op = EPOLL_CTL_MOD;
					e.events = EPOLLIN | EPOLLOUT;
				}else{
					op = EPOLL_CTL_ADD;
					e.events = EPOLLOUT;
				}
				e.data.ptr = ev; 
				ev->wactive = 1;
				//Log(LOG_DEBUG,"come here %d",ev->fd); 
				return epoll_ctl(loop->efd, op, ev->fd, &e);
				//Log(LOG_DEBUG,"after epoll");
				// return 1;
				break;
		case EVENT_TIMEOUT:
				timeout = ev->timeout;
				struct timeval   tv;
				gettimeofday(&tv, NULL);
				timeout = timeout+tv.tv_sec*1000+tv.tv_usec/1000;
				//printf("insert timeout %lld\n", timeout);
				return listInsert(loop->timeoutList,timeout,ev);
				break;
	}
	return 0;
}

void delEvent(eventLoop* loop,event *ev, int type){
	int op ;
	event * other;
	server_contex * contex = ev->contex;
	struct epoll_event e;
	if((type == EVENT_READ && !ev->ractive) || (type == EVENT_WRITE && !ev->wactive)){
		return ;
	}
	if(type == EVENT_READ){
		if(ev->wactive){
			op = EPOLL_CTL_MOD;
			e.events = EPOLLOUT;
		}else{
			op = EPOLL_CTL_DEL;
			e.events = EPOLLIN;
		}
		ev->ractive = 0;
	}else{
		if(ev->ractive){
			op = EPOLL_CTL_MOD;
			e.events = EPOLLIN;
		}else{
			op = EPOLL_CTL_DEL;
			e.events = EPOLLOUT;
		}
		ev->wactive = 0;
	}
	e.data.ptr = ev; 
	epoll_ctl(loop->efd, op, ev->fd, &e);
}

void eventCycle(eventLoop* loop){
	int n,i;
	int fd;
	event * ev;
	long long minTimeout ,now;
	while(1){
		if(listLength(loop->timeoutList) == 0){
			minTimeout = -1;
		}else{
			struct timeval   tv;
			gettimeofday(&tv, NULL);
			minTimeout = loop->timeoutList->head->timeout;
			//printf("timeoout %lld\n",minTimeout );
			now = tv.tv_sec*1000+tv.tv_usec/1000;
			minTimeout = minTimeout - now;
			if(minTimeout < 0){
				minTimeout = 0;
			} 
		}
		//printf("%lld\n",minTimeout );
		//printf("now %lld\n", now);
		n = epoll_wait(loop->efd,loop->list,500,minTimeout);
		if(n < 0){
			Log(LOG_ERROR, "epoll_wait return %d",n);
			continue ;
		}
		// Log(LOG_NOTICE,"[notice] epoll_wait return %d",n);
		for (i = 0; i < n; ++i)
		{
			struct epoll_event e = loop->list[i];
			ev = (event *)e.data.ptr;

			if(e.events & EPOLLIN){
				// Log(LOG_NOTICE,"[notice] fd %d , read ready ~",ev->fd);
				ev->rcall(ev);
			}
			if(e.events & EPOLLOUT){
				// Log(LOG_NOTICE,"[notice] fd %d , write ready ~",ev->fd);
				ev->wcall(ev);
			}
		}
		//timeout
		struct timeval   tv;
		gettimeofday(&tv, NULL);
		now = tv.tv_sec*1000+tv.tv_usec/1000;
		expireTimeout(loop->timeoutList,now);
	}
}