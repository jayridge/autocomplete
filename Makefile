LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
UNAME := $(shell uname)

CFLAGS = -I$(LIBEVENT)/include -Wall -g -O0
LIBS = -L$(LIBEVENT)/lib  -ljson -licui18n -licuuc -licudata -levent -ljson -lpthread -lrt -lm -ldl -lstdc++

ifeq ($(UNAME), Linux)
    LIBS += -static
endif


autocomplete: autocomplete.c
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

install:
	/usr/bin/install -d $(TARGET)/bin
	/usr/bin/install autocomplete $(TARGET)/bin

clean:
	rm -rf *.a *.o autocomplete *.dSYM test_output test.db
