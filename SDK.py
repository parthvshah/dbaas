import docker
import time
import requests

client = docker.from_env()

if __name__ == "__main__":
    time.sleep(1) # Change to 2*60 secs

    res = requests.get('http://localhost:5000/api/v1/orch/readcount')
    count = res.json()[0]

