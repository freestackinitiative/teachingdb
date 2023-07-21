ARG PYTHON_VERSION=3.11.3

FROM python:$PYTHON_VERSION-slim-bullseye

WORKDIR /opt/teachdb

COPY . .

RUN apt-get update --no-install-recommends && \
    pip3 install -r requirements.txt && \
    pip3 install -e . && \
    pytest