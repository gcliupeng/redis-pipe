#ifndef AOF_PROCESS_H_
#define AOF_PRECESS_H_
void * saveAofThread(void *data);
void replicationAofFile(server_contex *th);
void replicationAofBuf(void * data);
void  sendData(void * data);
#endif