#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include "main.h"

extern pipe_server server; 

int loadConfig(const char * file){
	if(file == NULL){
		return 1;
	}
	char buf[201];
	FILE *fp = fopen(file,"r");
	if(!fp){
		Log(LOG_WARNING, "the log file can't open %s","error",file);
		return 1;
	}
	char *p;
	char *q;
	while(fgets(buf,201,fp) != NULL){
		if(buf[0] == '#') continue;
		p = strstr(buf,"logfile:");
		if(p!=NULL){
			p+=8;
			while(*p==' ')p++;
			server.logfile = p;
			int i=0;
			while(*(p+i) !='\r' && *(p+i) !='\n' && *(p+i) !='\0') i++;
			server.logfile[i]=0;
			char * c = malloc(strlen(server.logfile)+1);
			memcpy(c,server.logfile,strlen(server.logfile));
			server.logfile = c;
			continue;
		}

		p=strstr(buf,"level:");
		if(p!=NULL){
			p+=6;
			while(*p==' ')p++;
			if(strncmp(p,"DEBUG",5)==0){
				server.logLevel = LOG_DEBUG;
			};
			if(strncmp(p,"NOTICE",6)==0){
				server.logLevel = LOG_NOTICE;
			};
			if(strncmp(p,"WARNING",7)==0){
				server.logLevel = LOG_WARNING;
			};
			if(strncmp(p,"ERROR",5)==0){
				server.logLevel = LOG_ERROR;
			};
			continue;
		}

		p = strstr(buf,"filter:");
		if(p !=NULL){
			p+=7;
			while(*p==' ')p++;
			server.filter = p;
			int i=0;
			while(*(p+i) !='\r' && *(p+i) !='\n' && *(p+i) !='\0') i++;
			server.filter[i]=0;
			char * c = malloc(strlen(server.filter)+1);
			memcpy(c,server.filter,strlen(server.filter));
			server.filter = c;
			continue;
		}

		p=strstr(buf,"prefix:");
		if(p !=NULL){
			p+=7;
			while(*p==' ')p++;
			server.prefix = p;
			int i=0;
			while(*(p+i) !='\r' && *(p+i) !='\n' && *(p+i) !='\0') i++;
			server.prefix[i]=0;
			char * c = malloc(strlen(server.prefix)+1);
			memcpy(c,server.prefix,strlen(server.prefix));
			server.prefix = c;
			continue;
		}

		p=strstr(buf,"removePrefix:");
		if(p !=NULL){
			p+=13;
			while(*p==' ')p++;
			server.removePre = p;
			int i=0;
			while(*(p+i) !='\r' && *(p+i) !='\n' && *(p+i) !='\0') i++;
			server.removePre[i]=0;
			char * c = malloc(strlen(server.removePre)+1);
			memcpy(c,server.removePre,strlen(server.removePre));
			server.removePre = c;
			continue;
		}

		p=strstr(buf,"have:");
		if(p !=NULL){
			p+=5;
			while(*p==' ')p++;
			server.have = p;
			int i=0;
			while(*(p+i) !='\r' && *(p+i) !='\n' && *(p+i) !='\0') i++;
			server.have[i]=0;
			char * c = malloc(strlen(server.have)+1);
			memcpy(c,server.have,strlen(server.have));
			server.have = c;
			continue;
		}

		// servers需要放在配置文件的最后
		p=strstr(buf,"servers:");
		if(p !=NULL){
			while(fgets(buf,201,fp) !=NULL){
				p = buf;
				//解析 from server
				while(*p==' ')p++;
				if(*p=='\r'||*p=='\n'){
					Log(LOG_ERROR, "server block not ok %s,file %s",buf,file);
					return 1;
				}
				redis_conf *sc = array_push(server.servers_from);
				q = p;
				while(*q!=':'&&*q!='\r'&&*q!='\n')q++;
				if(*q=='\r'||*q=='\n'){
					Log(LOG_ERROR, "server block not ok %s,file %s",buf,file);
					return 1;
				}
				sc->ip = malloc(q-p+1);
				strncpy(sc->ip,p,q-p);
				sc->ip[q-p] = 0;
				p=++q;

				while(*q!=':'&&*q!=' '&&*q!='\r'&&*q!='\n')q++;
				if(*q=='\r'||*q=='\n'){
					Log(LOG_ERROR, "server block not ok %s,file %s",buf , file);
					return 1;
				}
				sc->port = toNumber(p,q);
				if(sc->port <=0){
					Log(LOG_ERROR, "server port not ok %s,file %s",buf , file);
					return 1;
				}
				if(*q == ':'){
					p = ++q;
					while(*q!=' '&&*q!='\r'&&*q!='\n')q++;
					if(*q=='\r'||*q=='\n'){
						Log(LOG_ERROR, "server block not ok %s ,file %s",buf,file);
						return 1;
					}
					sc->auth = malloc(q-p+1);
					strncpy(sc->auth,p,q-p);
					sc->auth[q-p] = 0;
				}else{
					sc->auth = NULL;
				}

				//解析to server
				p = q;
				while(*p==' ')p++;
				if(*p=='\r'||*p=='\n'){
					Log(LOG_ERROR, "server block not ok %s,file %s",buf,file);
					return 1;
				}
				sc = array_push(server.servers_to);
				q = p;
				while(*q!=':'&&*q!='\r'&&*q!='\n')q++;
				if(*q=='\r'||*q=='\n'){
					Log(LOG_ERROR, "server block not ok %s,file %s",buf,file);
					return 1;
				}
				sc->ip = malloc(q-p+1);
				strncpy(sc->ip,p,q-p);
				sc->ip[q-p] = 0;
				p=++q;

				while(*q!=':'&&*q!=' '&&*q!='\r'&&*q!='\n')q++;
				if(*q=='\r'||*q=='\n'){
					Log(LOG_ERROR, "server block not ok %s,file %s",buf , file);
					return 1;
				}
				sc->port = toNumber(p,q);
				if(sc->port <=0){
					Log(LOG_ERROR, "server port not ok %s,file %s",buf , file);
					return 1;
				}
				if(*q == ':'){
					p = ++q;
					while(*q!=' '&&*q!='\r'&&*q!='\n'&*q!='\0')q++;
					// if(*q=='\r'||*q=='\n'){
					// 	Log(LOG_ERROR, "server block not ok %s ,file %s",buf,file);
					// 	return 1;
					// }
					sc->auth = malloc(q-p+1);
					strncpy(sc->auth,p,q-p);
					sc->auth[q-p] = 0;
				}else{
					sc->auth = NULL;
				}
			}
		}
	}
}
