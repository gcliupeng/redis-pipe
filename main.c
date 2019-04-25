#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include "main.h"
#include "loop.h"
#include <signal.h>  
#include <execinfo.h>
#include <sys/wait.h> 

pipe_server server;
FILE * logfp;
int logLevel;
array * array_pid;

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

	// printf("%d\n",array_n(server.servers_from ));
	// exit(0);
	int count = array_n(server.servers_from);
	array_pid = array_create(count,sizeof(int));
	int i = 0;
	while(i++ < array_n(server.servers_from)){
		int r = fork();
		if(r<0){
			Log(LOG_ERROR,"fork error, errno:%d",errno);
		}
		//child
		if(r ==0){
			array * servers_from = array_create(1,sizeof(redis_conf));
			redis_conf *sct = array_push(servers_from);
			redis_conf *sctt = array_get(server.servers_from, i-1);
			*sct = *sctt;
			server.servers_from = servers_from;

			array * servers_to = array_create(1,sizeof(redis_conf));
			sct = array_push(servers_to);
			sctt = array_get(server.servers_to, i-1);
			*sct = *sctt;
			server.servers_to = servers_to;
			break;
		}else{
			//father
			int * pid = array_push(array_pid);
			*pid = r; 
			count--;
			if(!count){
				int deadN = 0;
				//fetch child exit signal
				while(deadN < array_n(server.servers_from)){
					int exit_status;
					int exitpid = wait(&exit_status);
					if(exitpid == -1){
						continue;
					}
					int j = 0;
					for (; j < array_n(array_pid); ++j){
						int *childid = array_get(array_pid,j);
						if (*childid == exitpid){
							*childid = -1;
						}
					}
					deadN++;
					Log(LOG_ERROR,"PID:%d exited with exit_code:%d", exitpid, WEXITSTATUS(exit_status));
					Log(LOG_ERROR,"PID:%d died on signal:%d", exitpid, WTERMSIG(exit_status));
				}
				Log(LOG_ERROR,"all children exit , father exit too ");
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

void reloadWorker(){
	int i;
	for (i = 0; i < array_n(array_pid); ++i){
		int * childid  = array_get(array_pid,i);
		if(*childid == -1){
			int r = fork();
			if(r == 0){
				array * servers_from = array_create(1,sizeof(redis_conf));
				redis_conf *sct = array_push(servers_from);
				redis_conf *sctt = array_get(server.servers_from, i);
				*sct = *sctt;
				server.servers_from = servers_from;

				array * servers_to = array_create(1,sizeof(redis_conf));
				sct = array_push(servers_to);
				sctt = array_get(server.servers_to, i);
				*sct = *sctt;
				server.servers_to = servers_to;
				Log(LOG_ERROR,"restart worker ,from server %s:%d",sct->ip,sct->port);
				break;
			}else if(r > 0){
				*childid = r;
			}else{
				Log(LOG_ERROR,"fork error, errno:%d",errno);
			}
		}
		if(i == array_n(array_pid) -1){
			return;
		}
	}
	workerLoop();
}

void sigsegvHandler(int sig, siginfo_t *info, void *secret) {
	Log(LOG_ERROR, "memory crash !!!");
	ucontext_t *uc = (ucontext_t*) secret;
	void *buffer[30] = {0};  
    size_t size;  
    char **strings = NULL;  
    size_t i = 0;  
  
    size = backtrace(buffer, 30);  
    //fprintf(stdout, "Obtained %zd stack frames.nm\n", size);  
    // buffer[1] = (void*) uc->uc_mcontext.gregs[16];
    strings = backtrace_symbols(buffer, size);  
    if (strings == NULL)  
    {  
    	Log(LOG_ERROR, "no frames !!");
        //perror("backtrace_symbols.");  
        exit(2);  
    }  
      
    char content[1000]=""; 
    int pos = 0;
    for (i = 0; i < size; i++)  
    {  
    	int nn = sprintf(content+pos,"%s\n",strings[i]);
    	pos+=nn;
    }
    Log(LOG_ERROR,"dump: %s",content);
    free(strings);  
    strings = NULL;  
    exit(0);  
}

void damonize(){
	int pid = fork();
	if(pid < 0){
		Log(LOG_ERROR,"fork error");
		exit(1);
	}
	if(pid >0) exit(0);
	setsid();
}
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
 	sigaction(SIGINT, &act, 0);
 	sigaction(SIGHUP, &act, 0);
 	sigaction(SIGTERM, &act, 0);
 	
 	//重新拉起挂掉的worker
 	act.sa_handler = reloadWorker;
 	sigaction(SIGUSR1,&act,0);

 	act.sa_sigaction = sigsegvHandler;
 	sigemptyset(&act.sa_mask);  
 	act.sa_flags = SA_NODEFER | SA_RESETHAND | SA_SIGINFO;;  
 	sigaction(SIGSEGV, &act, 0);
 	sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGILL, &act, NULL);

	// workerLoop();
	damonize();
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