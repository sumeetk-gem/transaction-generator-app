# Dockerfile for sample-app msg processor project
FROM python:2.7

ENV DB_SVC_HOME /root/dbsvc
ADD . ${DB_SVC_HOME}
WORKDIR ${DB_SVC_HOME}

RUN pip install pika MySQL-python

CMD ["python", "main.py"]
