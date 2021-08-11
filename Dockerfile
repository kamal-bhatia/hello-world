FROM python:3.9-slim

ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

#RUN pip3 install --upgrade virtualenv --user
#RUN python3 -m virtualenv env
#RUN source env/bin/activate
#RUN pip3 install --quiet apache-beam[gcp]

#CMD python3 bq_2.py

CMD ["python_csv_to_bigquery.py"]
ENTRYPOINT ["python3"]
