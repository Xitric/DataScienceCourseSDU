FROM bde2020/hadoop-base

LABEL Name=pysparkExampleImage Version=0.0.1 

RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get -yq install python3 python3-pip locales
RUN python3 -m pip install pyspark
RUN apt-get autoremove -y
RUN apt-get clean
RUN cp /usr/bin/python3 /usr/bin/python
# RUN ln -s /usr/bin/python3 python

RUN touch /usr/share/locale/locale.alias
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && locale-gen
ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8  

WORKDIR /app
ADD . /app
ADD hbase-site.xml /etc/hadoop/hbase-site.xml

CMD ["python3 example.py"]