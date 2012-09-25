LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
LIBSIMPLEHTTP_INC ?= $(LIBSIMPLEHTTP)/..
LIBSIMPLEHTTP_LIB ?= $(LIBSIMPLEHTTP)

CFLAGS = -I$(LIBSIMPLEHTTP_INC) -I$(LIBEVENT)/include -Wall -g -O0
LIBS = -L$(LIBSIMPLEHTTP_LIB) -L$(LIBEVENT)/lib -levent -lsimplehttp -lm -ljson -licui18n -licuuc -licudata

autocomplete: autocomplete.c
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

install:
	/usr/bin/install -d $(TARGET)/bin
	/usr/bin/install autocomplete $(TARGET)/bin

clean:
	rm -rf *.a *.o autocomplete *.dSYM test_output test.db
