FROM python:3.9-slim

ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

RUN pip3 install apache-beam[gcp]

#CMD python3 bq_2.py

CMD ["bq_2.py"]
ENTRYPOINT ["python3"]
