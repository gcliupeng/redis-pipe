#ifndef LOOP_H
#define LOOP_H
#include "main.h"
#include "buf.h"
void masterLoop();
void workerLoop();
int connectFrom();
int connectTo();
int dumpRdbFile();
void cycleFunction(void * data);
int processPsyncPart(server_contex * contex);
int sendPartSync(server_contex * contex);
int sendToServerwithRerty(server_contex * contex, buf_t * output);
#endif