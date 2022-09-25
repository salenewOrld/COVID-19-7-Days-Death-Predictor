FROM continuumio/miniconda3
RUN apt-get update
RUN apt install -y make
RUN conda install -c conda-forge dtale 
EXPOSE 8181
#ENTRYPOINT [ "dtale" ]