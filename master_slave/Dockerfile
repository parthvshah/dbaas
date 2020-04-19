FROM python:3.7-slim-stretch

ENV PYTHONUNBUFFERED=0

RUN mkdir -p /master_slave
COPY ./requirements.txt /code/requirements.txt
RUN pip install -r /code/requirements.txt
# COPY . /code
WORKDIR /master_slave