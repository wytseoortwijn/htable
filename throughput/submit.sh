#!/bin/bash

/home/oortwijn/upc/bin/upcrun -n $3 -N $1 -bind-threads -shared-heap 1100MB -fca_enable 0 main_lin 3
