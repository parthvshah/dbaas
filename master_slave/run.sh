#!/bin/bash
#runs switch.py in the background. -u is for unbuffered variable
exec python -u switch.py &
#runs master_slave.py
exec python -u master_slave.py
