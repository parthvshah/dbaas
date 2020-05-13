#!/bin/bash

docker system prune -f
python3 PID.py &
# export OS variable here with pwd command to GET
docker-compose up --build