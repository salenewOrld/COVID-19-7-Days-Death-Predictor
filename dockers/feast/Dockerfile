FROM python:latest
RUN apt-get update
RUN apt install -y make
RUN pip3 install feast
EXPOSE 9696
#ENTRYPOINT [ "feast", "ui", "-h", "0.0.0.0", "-p", "9696" ]