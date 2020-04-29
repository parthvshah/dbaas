#!/bin/bash

exec python -u zoo.py &
exec python -u scale_watch.py &
exec python -u orchestrator.py runserver --host 0.0.0.0 --port 5000
