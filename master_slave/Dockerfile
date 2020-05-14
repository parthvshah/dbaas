FROM python:3.7-slim-stretch
#Allows log messages to be buffered
ENV PYTHONUNBUFFERED=0 

RUN mkdir -p /master_slave
#copy requirements
COPY ./requirements.txt /code/requirements.txt
#procps is for all the ps utilities
RUN apt-get update && apt-get -y install procps
#Install the requirements one at a time
RUN pip install -r /code/requirements.txt
# COPY . /code
WORKDIR /master_slave