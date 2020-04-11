FROM python:3.7-slim-stretch
# We copy just the requirements.txt first to leverage Docker cache
COPY ./requirements.txt /app/requirements.txt
VOLUME [ "/app" ]

WORKDIR /app

RUN pip install -r requirements.txt

# COPY . /app

# ENTRYPOINT [ "python" ]

# CMD [ "orchestrator.py" ]

