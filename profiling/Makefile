CC=gcc
CFLAGS= -Wall
DEPS = metaindex.h
OBJ = passthrough.o metaindex.o

LIBS=`pkg-config --cflags --libs glib-2.0` `pkg-config fuse3 --cflags --libs` -lpthread -lcrypto

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS) $(LIBS)

passthrough: $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

.PHONY: clean

clean:
	rm -f *.o passthrough

run:
	make
	sudo ./passthrough /mnt/fs -omodules="subdir,subdir=/backend" -oallow_other -f
    