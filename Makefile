
CC= gcc -std=gnu99
CFLAGS= -g
MYLDFLAGS= -lm -lpthread


TARGET = redis-pipe
# OBJS = main.o array.o config.o dist.o md5.o struct.o network.o loop.o ev.o parse2.o lzf_c.o lzf_d.o endianconv.o zipmap.o ziplist.o intset.o hash.o buf.o
OBJS = main.o array.o config.o md5.o struct.o network.o loop.o ev.o rdb_process.o aof_process.o lzf_c.o lzf_d.o endianconv.o zipmap.o ziplist.o intset.o buf.o
all:$(TARGET)

$(TARGET):$(OBJS)
	$(CC) -o $@ $(CFLAGS) $(OBJS) $(MYLDFLAGS)

clean:
	rm -rf *.o
	rm -rf redis-pipe

array.o: array.c array.h
config.o: config.c config.h
main.o: config.c main.c array.c  
md5.o: md5.c
struct.o : struct.c
network.o:network.c
loop.o:loop.c
ev.o :ev.c
rdb_process.o :rdb_process.c
aof_process.o:aof_process.c
lzf_d.o :lzf_d.c
lzf_c.o: lzf_c.c
endianconv.o : endianconv.c
zipmap.o: zipmap.c
ziplist.o :ziplist.c
intset.o :intset.c
buf.o :buf.c