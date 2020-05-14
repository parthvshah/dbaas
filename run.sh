#!/bin/bash

docker system prune -f #cleans docker
python3 PID.py & #runs PID.py in the background
# export OS variable here with pwd command to GET
docker-compose up --build