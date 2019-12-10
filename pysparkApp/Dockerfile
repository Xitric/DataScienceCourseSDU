FROM bde2020/hadoop-base

LABEL Name=pysparkHBase Version=0.0.1

# Install Python 3.7.5
RUN apt-get update && apt-get -yq install locales build-essential checkinstall
RUN apt-get -yq install libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev

RUN curl -O https://www.python.org/ftp/python/3.7.5/Python-3.7.5.tgz && \
    mkdir -p /usr/bin/python3 && \
    tar -zxvf Python-3.7.5.tgz -C /usr/bin/python3 --strip-components 1 && \
    rm Python-3.7.5.tgz

WORKDIR /usr/bin/python3
RUN ./configure --enable-optimizations
RUN make altinstall

ENV PATH="/usr/bin/python3:$PATH" 

# Install Python packages
WORKDIR /
RUN python3.7 -m pip install --upgrade pip
RUN python3.7 -m pip install pyspark
RUN python3.7 -m pip install shapely
RUN python3.7 -m pip install pandas
RUN python3.7 -m pip install wheel
COPY geo_pyspark-0.2.0-py3-none-any.whl /
RUN python3.7 -m pip install geo_pyspark-0.2.0-py3-none-any.whl
RUN apt-get autoremove -y
RUN apt-get clean

#RUN cp /usr/bin/python3 /usr/bin/python

RUN touch /usr/share/locale/locale.alias
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && locale-gen
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

COPY hbase-site.xml /etc/hadoop/hbase-site.xml

WORKDIR /backend
COPY shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar .
COPY /backend .

# Remove if we create another container for Spark streaming
EXPOSE 4444
#RUN locate spark
#RUN whereis spark
CMD ["python3.7 data_importer.py"]