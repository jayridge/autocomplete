LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp

CFLAGS = -I$(LIBEVENT)/include -Wall -g -O0
LIBS = -L$(LIBEVENT)/lib -levent -lm -ljson -licui18n -licuuc -licudata

autocomplete: autocomplete.c
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

install:
	/usr/bin/install -d $(TARGET)/bin
	/usr/bin/install autocomplete $(TARGET)/bin

clean:
	rm -rf *.a *.o autocomplete *.dSYM test_output test.db
