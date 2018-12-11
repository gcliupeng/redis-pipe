#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <malloc.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <errno.h>
#include <string.h>

#include "loop.h"
#include "network.h"
#include "config.h"
#include "main.h"
#include "ev.h"
#include "rdb_process.h"
#include "aof_process.h"

extern pipe_server server;

pthread_cond_t sync_cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t sync_mutex = PTHREAD_MUTEX_INITIALIZER;

int aof_alive;
int stopAofSave = 0;

void masterLoop(){

}

void workerLoop(){

	//  for (int i = 0; i < array_n(server.servers_from); ++i){
	// 	redis_conf * sc = array_get(server.servers_from, i);
	// 	printf("%d", (int)getpid());
	// 	printf("the server name is %s ",sc->ip );
	// 	printf("the port is %d ",sc->port);
	// 	printf("the auth is %s ",sc->auth );

	// 	sc = array_get(server.servers_to, i);
	// 	//printf("%x\n",sc );
	// 	printf("the server name is %s ",sc->ip );
	// 	printf("the port is %d ",sc->port);
	// 	printf("the auth is %s\n ",sc->auth );
	// 	/* code */
	// }
	// sleep(100);
	// exit(0);
	int re;
	//建立server_contex
	server_contex * contex = malloc(sizeof(*contex));
	if(!contex){
		Log(LOG_ERROR,"malloc error");
		exit(1);
	}
	server.contex = contex;
	contex->sc = &server;
	//contex->transfer_size = -1;
	pthread_mutex_init(&contex->mutex,NULL);
	//初始化EPOLL
	eventLoop * loop = eventLoopCreate();
	if(!loop){
		Log(LOG_ERROR, "eventLoopCreate error");
		exit(1);
	}
	contex->loop = loop;
	//建立跟from redis 的连接，并认证
	contex->from_fd= connectFrom();
	if(!(contex->from_fd)){
		// printf("%d\n",contex->from_fd );
		exit(1);
	}
	//建立跟to redis的连接，并认证
	contex->to_fd = connectTo();
	if(!(contex->to_fd)){
		exit(1);
	}
	//全量同步dupmRdb，可能需要考虑重试
	if(!dumpRdbFile()){
		exit(1);
	}

	//解析rdb，同时saveAof
	aof_alive = 1;
	pthread_t saveAofthread;
	pthread_create(&saveAofthread,NULL,saveAofThread,contex);
	parseRdbThread(contex);
	//sleep(10);
	stopAofSave = 1;

	pthread_mutex_lock(&sync_mutex);
	while(aof_alive == 1){
		pthread_cond_wait(&sync_cond,&sync_mutex);
	}
	pthread_mutex_unlock(&sync_mutex);
	//处理aof文件
	contex->replicationBufSize = 1024*1024;
	contex->replicationBuf = malloc(1024*1024*sizeof(char));
	contex->replicationBufLast = contex->replicationBufPos = contex->replicationBuf;
	contex->bucknum = -1;
	contex->lineSize = -1;
	contex->inputMode = -1;
	contex->step = 0;
	contex->key = NULL;
	contex->processed = 0;
	Log(LOG_NOTICE, "begin process the aof file from server , the file is %s",contex->aoffile);
	replicationAofFile(contex);
	Log(LOG_NOTICE, "process the aof file done, the file is %s, processed %d",contex->aoffile,contex->processed);
	unlink(contex->aoffile);
	
	// //检查是否server已经把连接关闭
	// struct tcp_info info; 
 //  	int len=sizeof(info); 
 //  	getsockopt(th->fd, IPPROTO_TCP, TCP_INFO, &info, (socklen_t *)&len); 
 //  	if(info.tcpi_state != TCP_ESTABLISHED){
 //  		Log(LOG_ERROR, "socket closed %s:%d",sc->pname,sc->port);
 //  		//todo 
 //  	}
 //  	Log(LOG_NOTICE, "begin process the server output buf ");
	nonBlock(contex->from_fd);
	event * from =malloc(sizeof(*from));
	bzero(from,sizeof(*from));
	if(!from){
		Log(LOG_ERROR, "create event error");
		exit(1);
	}
	from->type = EVENT_READ;
	from->fd = contex->from_fd;
	from->rcall = replicationAofBuf;
	from->contex = contex;
	contex->from = from;
	addEvent(contex->loop,from,EVENT_READ);

	event * to =malloc(sizeof(*to));
	if(!to){
		Log(LOG_ERROR, "create event error");
		exit(1);
	}
	bzero(to,sizeof(*to));
	to->type = EVENT_WRITE;
	to->fd = contex->to_fd;
	to->wcall = sendData;
	to->contex = contex;
	contex->to = to;
	addEvent(contex->loop,to,EVENT_WRITE);

	//定时检查

	// th->processed = 0;
	// r->type = EVENT_READ;
	// r->fd = th->fd;
	// r->rcall = replicationWithServer;
	// r->contex = th;
	/*
	th->replicationBufSize = 1024*1024;
	th->replicationBuf = malloc(1024*1024*sizeof(char));
	th->replicationBufLast = th->replicationBufPos = th->replicationBuf;
	th->bucknum = -1;
	th->lineSize = -1;
	th->inputMode = -1;
	th->step = 0;
	*/
	//addEvent(th->loop,r,EVENT_READ);




	//事件循环
	eventCycle(contex->loop);
}

int connectFrom(){
	redis_conf *redis_c = array_get(server.servers_from, 0);
	int fd = connetToServer(redis_c->port,redis_c->ip);
	if(fd <= 0){
		Log(LOG_ERROR, "can't connetToServer %s:%d",redis_c->ip,redis_c->port);
		return 0;
	}

	//auth
	if(strlen(redis_c->auth)>0){
		char auth[100];
		int n;
		n = sprintf(auth,"*2\r\n$4\r\nauth\r\n$%d\r\n%s\r\n",strlen(redis_c->auth),redis_c->auth);
		auth[n] = '\0';
		if(!sendToServer(fd,auth,strlen(auth))){
			Log(LOG_ERROR,"can't send auth:%s to server %s:%d",redis_c->auth, redis_c->ip,redis_c->port);
			return 0;
		}
		//read +OK\r\n
		if(readBytes(fd,auth,5)==0){
			Log(LOG_ERROR,"can't read auth:%s response, server %s:%d",redis_c->auth, redis_c->ip,redis_c->port);
			return 0;
		}
		if(strncmp(auth,"+OK\r\n",5)!=0){
			Log(LOG_ERROR,"auth failed, auth:%s response:%s, server %s:%d",redis_c->auth,auth, redis_c->ip,redis_c->port);
			return 0;
		}
	}
	Log(LOG_NOTICE, "connect to from redis ok %s:%d",redis_c->ip,redis_c->port);
	return fd;
}

int connectTo(){
	redis_conf *redis_c = array_get(server.servers_to, 0);
	int fd = connetToServer(redis_c->port,redis_c->ip);
	if(fd <= 0){
		Log(LOG_ERROR, "can't connetToServer %s:%d",redis_c->ip,redis_c->port);
		return 0;
	}

	//auth
	if(strlen(redis_c->auth)>0){
		char auth[100];
		int n;
		n = sprintf(auth,"*2\r\n$4\r\nauth\r\n$%d\r\n%s\r\n",strlen(redis_c->auth),redis_c->auth);
		auth[n] = '\0';
		if(!sendToServer(fd,auth,strlen(auth))){
			Log(LOG_ERROR,"can't send auth:%s to server %s:%d",redis_c->auth, redis_c->ip,redis_c->port);
			return 0;
		}
		//read +OK\r\n
		if(readBytes(fd,auth,5)==0){
			Log(LOG_ERROR,"can't read auth:%s response, server %s:%d",redis_c->auth, redis_c->ip,redis_c->port);
			return 0;
		}
		if(strncmp(auth,"+OK\r\n",5)!=0){
			Log(LOG_ERROR,"auth failed, auth:%s response:%s, server %s:%d",redis_c->auth,auth, redis_c->ip,redis_c->port);
			return 0;
		}

	}
	Log(LOG_NOTICE, "connect to to redis ok %s:%d",redis_c->ip,redis_c->port);
	return fd;
}

int dumpRdbFile(){
	server_contex * contex = server.contex;
	redis_conf *redis_c = array_get(server.servers_from, 0);
	if(!sendSync(contex)){
		Log(LOG_ERROR,"can't send sync to server %s:%p",redis_c->ip,redis_c->port);
		return 0;
	}
	Log(LOG_NOTICE, "send sync to server %s:%d ok",redis_c->ip,redis_c->port);
	
	if(!parseSize(contex)){
		//printf("parse size error \n");
		Log(LOG_ERROR, "parse size error from server %s:%d",redis_c->ip,redis_c->port);
		return 0;
	}

	Log(LOG_NOTICE, "parse size from server %s:%d ok, the size is %llu",redis_c->ip,redis_c->port ,contex->transfer_size);

	if(!saveRdb(contex)){
		Log(LOG_ERROR, "save the rdb error %s:%d",redis_c->ip,redis_c->port);
		return 0;
	}

}