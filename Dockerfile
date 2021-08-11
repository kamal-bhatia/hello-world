FROM python:3.9-slim

#ENV APP_HOME /app
#WORKDIR $APP_HOME
#COPY . ./

RUN install --quiet apache-beam[gcp]

CMD python3 bq_2.py