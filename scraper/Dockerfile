FROM python:3.11-bookworm

WORKDIR /code

COPY ./requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y wget unzip &&\
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb &&\
    apt install -y ./google-chrome-stable_current_amd64.deb &&\
    rm google-chrome-stable_current_amd64.deb &&\
    apt-get clean
