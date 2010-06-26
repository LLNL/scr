// mpig++ -g -O0 -I../src -o test_ckpt_C test_ckpt.C -L/usr/local/tools/scr-1.1/lib -lscr

#include <iostream>
#include <fstream>
#include <string>
#include <sstream>

#include "mpi.h"
#include "scr.h"

using namespace std;

int checkpoint()
{
  // inform SCR that we are starting a new checkpoint
  SCR_Start_checkpoint();

  // get our rank
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  // build the filename for our checkpoint file
  stringstream tmp;
  tmp << "rank_" << rank << ".ckpt";
  string name = tmp.str();
cout << "File: " << name << endl;

  // register our checkpoint file with SCR,
  // and ask SCR where to write the file
  char file[SCR_MAX_FILENAME];
  SCR_Route_file(name.c_str(), file); 
cout << "File: " << file << endl;

  // write our checkpoint file
  int valid = 1;

  ofstream ckpt;
  ckpt.open(file, ios::out | ios::trunc);
  ckpt << "hi" << endl;
  ckpt.close();

  // inform SCR whether this process wrote each of its
  // checkpoint files successfully
  SCR_Complete_checkpoint(valid);

  return 0;
}

int main()
{
  MPI_Init(NULL, NULL);

  // initialize the SCR library
  SCR_Init();

  // ask SCR whether we need to checkpoint
  int flag = 0;
  SCR_Need_checkpoint(&flag);
  if (flag) {
    // execute the checkpoint code
    checkpoint();
  }

  // shut down the SCR library
  SCR_Finalize();

  MPI_Finalize();

  return 0;
}
