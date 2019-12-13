FROM bde2020/hadoop-base

LABEL Name=sparkClient Version=1.0.0

# Install Python
ENV PYTHON_VERSION 3.7.5
RUN apt-get update && apt-get -yq install locales build-essential checkinstall
RUN apt-get -yq install libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev

RUN curl -O https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz && \
    mkdir -p /usr/bin/python && \
    tar -zxvf Python-$PYTHON_VERSION.tgz -C /usr/bin/python --strip-components 1 && \
    rm Python-$PYTHON_VERSION.tgz

WORKDIR /usr/bin/python
RUN ./configure --enable-optimizations
RUN make altinstall

WORKDIR /
ENV PATH="/usr/bin/python:$PATH"

# Install Spark
ENV SPARK_VERSION 2.4.4
ENV SPARK_HADOOP_VERSION 2.7
WORKDIR /
RUN curl -O https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION.tgz && \
    tar -xzvf spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION.tgz && \
    mv spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION /opt/spark && \
    rm spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION.tgz

ENV PATH="/opt/spark/bin/:$PATH"
ENV SPARK_HOME="/opt/spark"
ENV PYSPARK_PYTHON="/usr/bin/python/python"

# Install Python packages
WORKDIR /
RUN python -m pip install --upgrade pip
RUN python -m pip install pyspark
RUN python -m pip install shapely
RUN python -m pip install pandas
RUN python -m pip install flask
RUN apt-get autoremove -y
RUN apt-get clean

RUN touch /usr/share/locale/locale.alias
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && locale-gen
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# Set configurations
COPY spark-defaults.conf $SPARK_HOME/conf/
COPY hbase-site.xml $HADOOP_CONF_DIR/

# Enable HBase connectivity
WORKDIR /jobs
COPY /jobs .
COPY shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar .
COPY package.sh .
RUN /bin/bash package.sh

# TODO: Are these ports needed?
EXPOSE 43345
EXPOSE 8080

WORKDIR /service
COPY /service .
# CMD ["python __init__.py"]

# ADD run.sh /run.sh
# RUN chmod a+x /run.sh
# CMD ["/bin/bash", "/run.sh"]
CMD ["/bin/bash"]
