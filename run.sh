#!/bin/bash

docker system prune -f
python3 PID.py &
docker-compose up --build