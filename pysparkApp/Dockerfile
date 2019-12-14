FROM bde2020/hadoop-base

LABEL Name=pysparkHBase Version=0.0.1

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
# This must match the location of python on the executors
ENV PYSPARK_PYTHON /usr/bin/python/python

# Install Spark
WORKDIR /
RUN curl -O https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz && \
    tar -xzvf spark-2.4.4-bin-hadoop2.7.tgz && \
    mv spark-2.4.4-bin-hadoop2.7 /opt/spark && \
    rm spark-2.4.4-bin-hadoop2.7.tgz

ENV PATH="/opt/spark/bin/:$PATH"
ENV SPARK_HOME="/opt/spark"

# TODO: Install dependencies (only for local mode)
# Install Python packages
# WORKDIR /
# RUN python -m pip install --upgrade pip
# RUN python -m pip install pyspark
# RUN python -m pip install shapely
# RUN python -m pip install pandas
# RUN python -m pip install wheel
# COPY geo_pyspark-0.2.0-py3-none-any.whl /
# RUN python -m pip install geo_pyspark-0.2.0-py3-none-any.whl
# RUN apt-get autoremove -y
# RUN apt-get clean

# Set configurations
COPY hbase-site.xml $HADOOP_CONF_DIR/

# Enable HBase connectivity
WORKDIR /backend
COPY shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar .
COPY /backend .

# Enable history server connectivity
WORKDIR $SPARK_HOME/jars
COPY /jars .

# Remove if we create another container for Spark streaming
EXPOSE 4000
EXPOSE 4001

ADD run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/bin/bash", "/run.sh"]
