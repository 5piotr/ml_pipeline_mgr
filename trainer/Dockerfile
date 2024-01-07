FROM python:3.11-bookworm

WORKDIR /code

COPY ./requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY ./src ./src
