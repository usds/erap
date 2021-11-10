FROM python:3.9

RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt-get -y --no-install-recommends install postgresql-client postgis gdal-bin
RUN rm -rf /var/lib/apt/lists/*

RUN mkdir /app
RUN mkdir /app/arp_pipeline

COPY arp_pipeline /app/arp_pipeline
COPY pyproject.toml /app
COPY poetry.lock /app
COPY create_outputs /app
COPY .env /app

WORKDIR /app
ENV PYTHONPATH=${PYTHONPATH}:${PWD}

RUN pip3 install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev

EXPOSE 8082
