#!/bin/bash

exec python -u switch.py &
exec python -u master_slave.py
