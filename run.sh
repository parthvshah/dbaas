#!/bin/bash

docker system prune -y
python3 PID.py &
docker-compose up --build