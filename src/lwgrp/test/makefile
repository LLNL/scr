INCLUDE = -I../../install_mvapich2/include
LIBS    = -Wl,-rpath,../../install_mvapich2/lib -L../../install_mvapich2/lib -llwgrp

all: clean
	mpicc -g -O0 -o testcommops testcommops.c $(INCLUDE) $(LIBS)

clean:
	rm -rf *.o testcommops testsplit testsplit2 test2level
