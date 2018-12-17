#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include "main.h"
#include "loop.h"
#include <signal.h>  

pipe_server server;
FILE * logfp;
int logLevel;

void logRaw(const char * function, int line, int level, const char * fmt, ...){
	if(level<logLevel){												
		return;														
	}																
	char msg[500];													
	int n;																	
	time_t cur;																		
	struct tm cur_tm = {0};										
	time(&cur);														
    localtime_r(&cur, &cur_tm);												
	switch(level){												
		case LOG_DEBUG:											
			n = sprintf(msg,"[DEBUG]");	
			break;						
		case LOG_NOTICE:										
			n = sprintf(msg,"[NOTICE]");
			break;						
		case LOG_WARNING:										
			n = sprintf(msg,"[WORNING]");
			break;						
		case LOG_ERROR:											
			n = sprintf(msg,"[ERROR]");
			break;									
	}																			
	n+=sprintf(msg+n,"%4d/%02d/%02d %02d:%02d:%02d  ",cur_tm.tm_year + 1900,
                     cur_tm.tm_mon + 1,							
                     cur_tm.tm_mday,							
                     cur_tm.tm_hour,							
                     cur_tm.tm_min,								
                     cur_tm.tm_sec);							
	va_list ap;															
    va_start(ap, fmt);											
    vsnprintf(msg+n, sizeof(msg)-n, fmt, ap);					
    va_end(ap);													
    fprintf(logfp,"%s Functon: %s Line: %d\n",msg,function,line);									\
    fflush(logfp);												
}																
												
void initLog(const char *log,int level){
	if(log == NULL){
		logfp = stdout;
	}else{
		logfp = fopen(log,"a");
		if(!logfp){
			logfp = stdout;
		}
	}
	logLevel = level;
}

void spawWorkers(){

	int count = array_n(server.servers_from);
	while(count){
		int r = fork();
		if(r<0){
			Log(LOG_ERROR,"fork error, errno:%d",errno);
		}
		//child
		if(r ==0){
			array * servers_from = array_create(1,sizeof(redis_conf));
			redis_conf *sct = array_push(servers_from);
			redis_conf *sctt = array_get(server.servers_from, count-1);
			*sct = *sctt;
			server.servers_from = servers_from;

			array * servers_to = array_create(1,sizeof(redis_conf));
			sct = array_push(servers_to);
			sctt = array_get(server.servers_to, count-1);
			*sct = *sctt;
			server.servers_to = servers_to;
			break;
		}else{
			//father
			count--;
			//建立 server_from 到 pid的映射
			if(!count){
				exit(0);
				//masterLoop();
			}
		}
	}
	workerLoop();
}

// void func(){
// 	Log(LOG_ERROR, "server close the connection error");
// }

int main(int argc, char const *argv[])
{
	/* code */
	logfp = stdout;
	logLevel = LOG_DEBUG;
	server.logfile = NULL;
	server.logLevel = LOG_NOTICE;
	const char * configFile = "config.yml";
	
	server.prefix = "";
	server.removePre = "";
	server.filter = "";
	server.have = "";
	server.servers_from = array_create(10,sizeof(redis_conf));
	server.servers_to = array_create(10,sizeof(redis_conf));

	int i;
	for (i = 1; i < argc; ++i){
		if(!strcmp(argv[i],"-c")){
			configFile = argv[++i];
		}
	}
	if(loadConfig(configFile)){
		exit(1);
	}
	initLog(server.logfile,server.logLevel);
	init_pool();
	Log(LOG_NOTICE, "config file parse ok ");
	
	/*dump
	*/
	//printf("filter is %s\n",server.filter);
	//printf("prefix is %s\n",server.prefix);
	// printf("the hash type is %d\n",server.old_config->hashType);
	// printf("the dist type is %d\n",server.old_config->distType);
	// printf("the auth is %s\n",server.old_config->auth);
	// for (int i = 0; i < array_n(server.servers_from); ++i)
	// {
	// 	redis_conf * sc = array_get(server.servers_from, i);
	// 	printf("%x\n",sc );
	// 	printf("the server name is %s\n",sc->ip );
	// 	printf("the port is %d\n",sc->port);
	// 	printf("the auth is %s\n",sc->auth );

	// 	sc = array_get(server.servers_to, i);
	// 	printf("%x\n",sc );
	// 	printf("the server name is %s\n",sc->ip );
	// 	printf("the port is %d\n",sc->port);
	// 	printf("the auth is %s\n",sc->auth );
	// 	/* code */
	// }
	//exit(0);
	// oldMap = createDict(20);
	// newMap = createDict(20);
	struct sigaction act;  
 	act.sa_handler = SIG_IGN;  
 	sigemptyset(&act.sa_mask);  
 	sigaction(SIGPIPE, &act, 0);
	//workerLoop();
	spawWorkers();

	// //wait signal
	// struct sigaction act;  
 //    act.sa_handler = func;  
 //   	sigemptyset(&act.sa_mask); 
 //  	sigaction(SIGPIPE, &act, 0);

	// sigset_t		set;
	// sigemptyset(&set);
	// sigaddset(&set,SIGPIPE);
 //    sigsuspend(&set);

	return 0;
}