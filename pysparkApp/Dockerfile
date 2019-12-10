FROM bde2020/hadoop-base

LABEL Name=pysparkHBase Version=0.0.1

RUN apt-get update
# RUN DEBIAN_FRONTEND=noninteractive apt-get -yq install python3 python3-pip locales

RUN DEBIAN_FRONTEND=noninteractive apt-get update & apt-get -yq install python3 --fix-missing
RUN DEBIAN_FRONTEND=noninteractive apt-get update & apt-get -yq install python3-pip --fix-missing
RUN DEBIAN_FRONTEND=noninteractive apt-get update & apt-get -yq install locales --fix-missing

RUN python3 -m pip install pyspark
RUN python3 -m pip install shapely
RUN python3 -m pip install pandas
RUN apt-get autoremove -y
RUN apt-get clean
RUN cp /usr/bin/python3 /usr/bin/python

RUN touch /usr/share/locale/locale.alias
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && locale-gen
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

COPY hbase-site.xml /etc/hadoop/hbase-site.xml

WORKDIR /backend
COPY shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar .
COPY /backend .

CMD ["python3 incident_modern_context.py"]
