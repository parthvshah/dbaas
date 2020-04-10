FROM python:3.7-slim-stretch


COPY ./requirements.txt /code/requirements.txt
RUN pip install -r /code/requirements.txt
COPY . /code
WORKDIR /code