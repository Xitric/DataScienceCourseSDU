FROM bde2020/hadoop-base

LABEL Name=spark Version=1.0.0

# Install Python
ENV PYTHON_VERSION 3.7.5
RUN apt-get update && apt-get -yq install locales build-essential checkinstall
RUN apt-get -yq install libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev liblzma-dev

RUN curl -O https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz && \
    mkdir -p /usr/bin/python && \
    tar -zxvf Python-$PYTHON_VERSION.tgz -C /usr/bin/python --strip-components 1 && \
    rm Python-$PYTHON_VERSION.tgz
 
WORKDIR /usr/bin/python
RUN ./configure --enable-optimizations
RUN make altinstall
 
WORKDIR /
ENV PATH=/usr/bin/python:$PATH
# This must match the location of python on the executors
ENV PYSPARK_PYTHON /usr/bin/python/python

# Install Spark
ENV SPARK_VERSION 2.4.4
ENV SPARK_HADOOP_VERSION 2.7
WORKDIR /
RUN curl -O https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION.tgz && \
    tar -xzvf spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION.tgz && \
    mv spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION /opt/spark-$SPARK_VERSION && \
    rm spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION.tgz

ENV SPARK_HOME=/opt/spark-$SPARK_VERSION
ENV PATH=$SPARK_HOME/bin/:$PATH

RUN apt-get autoremove -y
RUN apt-get clean

# Configure history server compatibility
WORKDIR $SPARK_HOME/jars
COPY /jars .
