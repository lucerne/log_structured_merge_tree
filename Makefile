CFLAGS = -std=gnu99 -Wall -g -ggdb -O0 
LIBS = -lm -lpthread

concurrent_main: concurrent_main.o lsm.o lib.o level.o 
	gcc $(CFLAGS) -o  $@ $^ $(LIBS)


concurrent_main.o: concurrent_main.c
	gcc $(CFLAGS) -c concurrent_main.c

lsm.o: lsm.c
	gcc $(CFLAGS) -c lsm.c

lib.o: lib.c
	gcc $(CFLAGS) -c lib.c

level.o: level.c
	gcc $(CFLAGS) -c level.c

clean:
	rm -rf *.o concurrent_main
