gfakt
=====
A Python driver for GPU enabled versions of GMP-ECM

gfakt is an easily deployed and ready-to-use command line tool written in Python
which eases use of GPU enabled versions of GMP-ECM the well known integer
factoring program implementing the elliptic curve method.  
Given a system with (lots of) GPUs and CPU cores, the main goal is to make 
the most efficient use of the processing power to run lots and lots of curves.
The GPUs are used to run stage 1 of ECM and the results are sent for processing by the CPUs


Getting started
---------------

Once you have installed and configured your favorite Python interpreter 
download the latest version of gfakt, cd to the directory where you copied 
file gfakt.py. Type gfakt.py without arguments to get some help.  

Examples:  

        > gfakt.py -d 0 -t 8 -c 1280 3e6 -N "(10^223+9)/426599455932706145117142072767"
  
will compute 1280 stage 1 with B1=3e6 using device 0 on the number specified after option -N, once
saved the results are split for performing step 2 using 8 CPU threads in parallel.

You can specify more than one device and more than one number:

        > gfakt.py -d 0 1 -t 16 -c 1440 11e7 -N "a_1" "a_2" ... "a_n"

will run 1440 curves at B1=11e6 on each number a<sub>1</sub>, a<sub>2</sub>, ..., a<sub>n</sub>
using GPU devices 0 and 1 and 16 CPU threads.


