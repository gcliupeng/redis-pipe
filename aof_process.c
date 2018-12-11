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

#include "network.h"
#include "config.h"
#include "main.h"
#include "ev.h"
#include "rdb_process.h"

extern pipe_server server;
extern int stopAofSave;
extern pthread_cond_t sync_cond;
extern pthread_mutex_t sync_mutex ;
extern int aof_alive;

int processMulti(server_contex * th){
	int num = 0;
	long long ll;
	char *p;
	if(th->bucknum == -1){
		p = strstr(th->replicationBufPos,"\r\n");
		
		if(!p){
			//not enough data
			return -1;
		}
		string2ll(th->replicationBufPos+1,p-(th->replicationBufPos+1),&ll);
		th->bucknum = ll;
		//printf("%d\n", th->bucknum);
		th->replicationBufPos = p+2;
	}

	while(th->bucknum){

		if(th->lineSize == -1){
			p = strstr(th->replicationBufPos,"\r\n");
			if(!p){
				return -1;
			}
			string2ll(th->replicationBufPos+1,p-(th->replicationBufPos+1),&ll);
			th->replicationBufPos = p+2;
			th->lineSize = ll;
			//printf("%d\n",th->lineSize );
			th->step ++;
		}
			
			//p = strstr(th->replicationBufPos,"\r\n");
		if(th->replicationBufLast - th->replicationBufPos < th->lineSize+2){
			return -1;
		}
		if(th->step ==1){
			//printf("%s\n",th->replicationBufPos );
			if(strncmp(th->replicationBufPos,"PING",4)==0 || strncmp(th->replicationBufPos,"ping",4) ==0){
				th->type = REDIS_CMD_PING;
			}
			if(strncmp(th->replicationBufPos,"SELECT",6)==0 || strncmp(th->replicationBufPos,"select",6)==0){
				th->type = REDIS_CMD_SELECTDB;
			}
			/*
			if(strncmp(th->replicationBufPos,"flushall",8)==0 || strncmp(th->replicationBufPos,"FLUSHALL",8)==0){
				th->type = REDIS_CMD_SELECTDB;
			}*/
		}
		if(th->step == 2){
			th->key_length = th->lineSize;
			th->key = th->replicationBufPos;

		}
		th->replicationBufPos = th->replicationBufPos+th->lineSize+2;
		th->lineSize =-1;
		th->bucknum --;
	}
		return 1;
}

int processSingle(server_contex * th){
	char * p = strstr(th->replicationBuf,"\r\n");
	if(!p){
		//not enough
		return -1;
	}
	th->replicationBufPos = p+2;
	return 1;
}
void resetState(server_contex * th){
	th->replicationBufPosPre = th->replicationBufPos;
	th->bucknum = -1;
	th->lineSize = -1;
	th->inputMode = -1;
	th->type = -1;
	th->step = 0;
	th->key = NULL;
}
void moveBuf(server_contex * th){
	int n1,n2,nk;
	n1 = th->replicationBufLast-th->replicationBufPosPre;
	n2 = th->replicationBufPos-th->replicationBufPosPre;
	if(th->key !=NULL){
		nk = th->key - th->replicationBufPosPre;
	}
	memmove(th->replicationBuf,th->replicationBufPosPre,n1);
	th->replicationBufLast =th->replicationBuf +n1;
	th->replicationBufPos = th->replicationBuf + n2;
	th->replicationBufPosPre = th->replicationBuf;
	if(th->key != NULL){
		th->key = th->replicationBuf + nk;
	}
	memset(th->replicationBufLast,0,th->replicationBufSize-n1);
}

void processBuf(server_contex * th,int directSend){
	int n;
	static char t[1000];
	while(th->replicationBufPos != th->replicationBufLast){
			if(th->inputMode == -1){
				if(th->replicationBufPos[0] == '*'){
					th->inputMode = 1;//multi line
				}else{ 
					th->inputMode = 0; //single line
				}
			}

			if(th->inputMode == 1){
				n = processMulti(th);
			}else{
				//single mode 实现有问题，但2.8版本已经不用single mode，后续版本待验证
				n = processSingle(th);
			}
			if(n == -1){
				//not enough data
				break;
			}
			// parse ok
			redis_conf * from = array_get(th->sc->servers_from, 0);;
			if(th->processed %1000 ==0){
					Log(LOG_NOTICE, "processed %lld key  , from %s:%d",th->processed,from->ip,from->port);	
			}
			
			if(th->type == REDIS_CMD_PING || th->type == REDIS_CMD_SELECTDB){
				//printf("is ping or select\n");
				resetState(th);
				continue ;
			}
			
			//是否需要过滤
    		if(strlen(server.filter)>0 ){
        		if(strncmp(th->key,server.filter,strlen(server.filter)) !=0){
            		resetState(th);
            		th->processed++;
            		continue;
        		}
    		}

    		//是否需要过滤
    		if(strlen(server.have)>0){
    			char * pos = strstr(th->key,server.have);
        		if(!pos || (pos-th->key) > (th->key_length - strlen(server.have))){
        			th->processed ++;
            		resetState(th);
            		continue;
        		}
    		}

			if(th->key == NULL){
				Log(LOG_NOTICE,"%p",th->key);
				Log(LOG_NOTICE,"%s",th->replicationBufPosPre);
			}

			if(strlen(server.prefix) >0 || strlen(server.removePre)>0 ){
				memset(t,0,1000);
				memcpy(t,server.prefix,strlen(server.prefix));
				memcpy(t+strlen(server.prefix),th->key+strlen(server.removePre),th->key_length-strlen(server.removePre));
			}else{
				memset(t,0,1000);
				memcpy(t,th->key,th->key_length);
			}
			
			redis_conf * to = array_get(th->sc->servers_to, 0);;
			
			//send to new
			buf_t * output = getBuf(th->replicationBufPos - th->replicationBufPosPre+10);
			if(!output){
				//printf("getBuf error\n");
				Log(LOG_ERROR,"get buf error");
				exit(1);
			}
			
			if(strlen(server.prefix) >0 || strlen(server.removePre) > 0){
				char * beforKey = th->key - lengcontexSize(th->key_length) -2;
				long length,nn=0;

				length = beforKey-th->replicationBufPosPre;
				memcpy(output->start+nn, th->replicationBufPosPre, length);
				nn += length;
				
				length = sprintf(output->start+nn,"%d\r\n",th->key_length+strlen(server.prefix)-strlen(server.removePre));
				nn += length;

				length = strlen(server.prefix);
				memcpy(output->start+nn,server.prefix,strlen(server.prefix));
				nn += length;

				length = th->replicationBufPos - th->key - strlen(server.removePre);
				memcpy(output->start +nn ,th->key+strlen(server.removePre),length);
				nn += length;
				output->position = output->start + nn;
			}else{
				memcpy(output->start, th->replicationBufPosPre, th->replicationBufPos-th->replicationBufPosPre);
				output->position = output->start+(th->replicationBufPos - th->replicationBufPosPre);
			}
			output->last = output->position; 
    		output->position = output->start;
    		// printf("%s\n",output->start );
			if(directSend){
				if(sendToServer(th->to_fd,output->start, bufLength(output)) != bufLength(output)){
        			Log(LOG_ERROR,"send command to server error %s:%d,",to->ip,to->port);
        			resetState(th);
        			return ;
    			}
			}else{
				appendToOutBuf(th, output);
			}
			resetState(th);
			th->processed++;
		}
}

void replicationAofFile(server_contex *th){
	int fd ;
	fd = open(th->aoffile,O_RDWR|O_CREAT,0644);
    if(fd <0){
    	Log(LOG_ERROR,"can't open the aof file , %s ,errno %d",th->aoffile,errno);
        return;
   	}
	int n , i;
	int left;
	int extra;
	
	while(1){
		left = th->replicationBufSize-(th->replicationBufLast - th->replicationBuf);
		if(left == 0){
			if(th->replicationBufPosPre< th->replicationBuf + th->replicationBufSize/2){
				int used = th->replicationBufPos - th->replicationBuf;
				int used2 = th->replicationBufPosPre - th->replicationBuf;
				int nk;
				if(th->key != NULL){
					nk = th->key - th->replicationBuf;
				}
				th->replicationBuf = realloc(th->replicationBuf,th->replicationBufSize*2);
				th->replicationBufPos = th->replicationBuf +used;
				th->replicationBufPosPre = th->replicationBuf +used2;
				th->replicationBufLast = th->replicationBuf+th->replicationBufSize;
				if(th->key != NULL){
					th->key = th->replicationBuf + nk;
				}
				left = th->replicationBufSize;
				th->replicationBufSize *=2;
			}else{
				moveBuf(th);
				left = th->replicationBufSize-(th->replicationBufLast - th->replicationBuf);
			}
		}
		n = read(fd, th->replicationBufLast ,left);
		if(n <= 0){
			return;
		}
		th->offset += n;
	 	th->replicationBufLast+=n;
	 	processBuf(th,1);
	}
}

void replicationAofBuf(void * data){
	event *ev = data;
	server_contex * th = ev->contex;
	int fd ;
	fd = th->from_fd;
    if(fd <0){
    	Log(LOG_ERROR,"the from redis not connected ");
        return;
   	}
	int n , i;
	int left;
	int extra;
	
	while(1){
		left = th->replicationBufSize-(th->replicationBufLast - th->replicationBuf);
		if(left == 0){
			if(th->replicationBufPosPre< th->replicationBuf + th->replicationBufSize/2){
				int used = th->replicationBufPos - th->replicationBuf;
				int used2 = th->replicationBufPosPre - th->replicationBuf;
				int nk;
				if(th->key != NULL){
					nk = th->key - th->replicationBuf;
				}
				th->replicationBuf = realloc(th->replicationBuf,th->replicationBufSize*2);
				th->replicationBufPos = th->replicationBuf +used;
				th->replicationBufPosPre = th->replicationBuf +used2;
				th->replicationBufLast = th->replicationBuf+th->replicationBufSize;
				if(th->key != NULL){
					th->key = th->replicationBuf + nk;
				}
				left = th->replicationBufSize;
				th->replicationBufSize *=2;
			}else{
				moveBuf(th);
				left = th->replicationBufSize-(th->replicationBufLast - th->replicationBuf);
			}
		}
		n = read(fd, th->replicationBufLast ,left);
		if(n <= 0){
			return;
		}
		th->offset += n;
	 	th->replicationBufLast+=n;
	 	processBuf(th,0);
	}
}

void * saveAofThread(void *data){
	server_contex * contex = data;
	redis_conf *redis_c = array_get(contex->sc->servers_from, 0);
	memset(contex->aoffile,0,100);
    sprintf(contex->aoffile,"aof-%s-%d.aof",redis_c->ip,redis_c->port);
	Log(LOG_NOTICE, "create the aof file  from server %s:%d , the file is %s",redis_c->ip,redis_c->port ,contex->aoffile);
	
	int filefd = open(contex->aoffile,O_RDWR|O_CREAT|O_TRUNC,0644);
    if(filefd <0){
    	Log(LOG_ERROR,"can't open the aof file , %s ,errno %d",contex->aoffile,errno);
        return NULL;
   	}

   	int loop =0;
   	long sum = 0;
	while(!stopAofSave){
		char buf[1024*100];
    	int n;
    	n = read(contex->from_fd,buf,1024*100);
        if(n>0){
            write(filefd,buf,n);
            sum+=n;
            if(loop %1000 ==0){
            	Log(LOG_NOTICE, "write into aof file %s , %lld byte ",contex->aoffile, sum);
            }
            loop++;
        }
    }
    close(filefd);
    aof_alive = 0;
    Log(LOG_NOTICE, "write the aof file %s done ,total  %lld byte ",contex->aoffile, sum);
    pthread_mutex_lock(&sync_mutex);
    pthread_cond_broadcast(&sync_cond);
    pthread_mutex_unlock(&sync_mutex);
}

void  sendData(void * data){
	event *ev = data;
	server_contex * contex = ev->contex;;
	int fd = ev->fd;
	int n;
	//get one buf
	buf_t * buf = contex->bufout;
	if(!buf){
		delEvent(contex->loop ,ev, EVENT_WRITE);
		return;
	} 
	contex->bufout = buf->next;

	//send it
	if(buf->last-buf->position ==0){
		freeBuf(buf);
		return;
	}
	// printf("%s\n",buf->position );
	n = write(fd, buf->position, buf->last-buf->position);
	if(n == buf->last - buf->position){
		freeBuf(buf);
	}else{
		buf->position += n;
		// put again
		buf->next = contex->bufout;
		contex->bufout = buf;
	}
}

// void reconnect(server_contex * th){
// 	server_conf * sc = th->sc;
// 	th->fd = connetToServer(sc->port,sc->pname);
// 	if(th->fd <= 0){
// 		Log(LOG_ERROR, "can't connetToServer %s:%d",sc->pname,sc->port);
// 		//exit(1);
// 		return;
// 	}

// 	//auth
// 	if(strlen(server.new_config->auth)>0){
// 		char auth[100];
// 		int n;
// 		n = sprintf(auth,"*2\r\n$4\r\nauth\r\n$%d\r\n%s\r\n",strlen(server.new_config->auth),server.new_config->auth);
// 		auth[n] = '\0';
// 		if(!sendToServer(th->fd,auth,strlen(auth))){
// 			Log(LOG_ERROR,"can't send auth:%s to server %s:%p",server.new_config->auth, sc->pname,sc->port);
// 			//exit(1);
// 			return;
// 		}
// 		//read +OK\r\n
// 		if(readBytes(th->fd,auth,5)==0){
// 			Log(LOG_ERROR,"can't read auth:%s response, server %s:%p",server.new_config->auth, sc->pname,sc->port);
// 			//exit(1);
// 			return;
// 		}
// 	}

// 	event * w =malloc(sizeof(*w));
// 	if(!w){
// 		Log(LOG_ERROR, "create event error");
// 		exit(1);
// 	}
// 	th->write = w;

// 	event * r =malloc(sizeof(*r));
// 	if(!r){
// 		Log(LOG_ERROR, "create event error");
// 		exit(1);
// 	}
// 	th->read = r;
// 	w->type = EVENT_WRITE;
// 	w->fd = th->fd;
// 	w->wcall = sendData;
// 	w->contex = th;
// 	addEvent(th->loop,w,EVENT_WRITE);
	
// }


// void checkConnect(void * data){
// 	event *ev = data;
// 	server_contex * th = ev->contex;
// 	int fd  = th->fd;
// 	// char tmp[10];
// 	// int n = read(fd,tmp,10);
// 	// if(n ==0){
// 	// 	Log(LOG_NOTICE, "closed");
// 	// }
// 	struct tcp_info info; 
//   	int len=sizeof(info); 
//   	getsockopt(th->fd, IPPROTO_TCP, TCP_INFO, &info, (socklen_t *)&len); 
//   	if(info.tcpi_state != TCP_ESTABLISHED){
//   		Log(LOG_ERROR, "socket closed %s:%d",th->sc->pname,th->sc->port);
//   		//todo 
//   		reconnect(th);
//   	}

// 	Log(LOG_DEBUG, "checkConnect ");
// 	ev->type  = EVENT_TIMEOUT;
// 	ev->tcall = checkConnect;
// 	ev->timeout = 1000;
// 	addEvent(th->loop,ev,EVENT_TIMEOUT);
// }
