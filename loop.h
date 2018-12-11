#ifndef LOOP_H
#define LOOP_H
void masterLoop();
void workerLoop();
int connectFrom();
int connectTo();
int dumpRdbFile();
void cycleFunction(void * data);
#endif