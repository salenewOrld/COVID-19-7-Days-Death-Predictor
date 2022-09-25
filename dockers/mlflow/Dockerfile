FROM python:latest
RUN apt-get update
RUN apt install -y make
RUN pip3 install mlflow
RUN pip3 install mlflow[pipeline]
RUN pip3 install mlflow[extras]
EXPOSE 5959 