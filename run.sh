#!/bin/bash

docker system prune -f #cleans docker
python3 PID.py & #runs PID.py in the background
docker-compose up --build