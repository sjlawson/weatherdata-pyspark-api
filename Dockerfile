FROM python:3.10.10

WORKDIR /usr/src/app

RUN apt-get update && \
    apt-get install -y curl wget openjdk-11-jdk

COPY . .

WORKDIR /usr/src/app/src

RUN ls -l
RUN pip install --no-cache-dir -r requirements/docker.txt
ENV APP_PORT 5000
CMD [ "./entrypoint.sh" ]