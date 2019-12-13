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

WORKDIR /
ENV PATH="/usr/bin/python3:$PATH"
RUN ln -s /usr/bin/python3 python

# Install Spark
WORKDIR /
RUN curl -O https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz && \
    tar -xzvf spark-2.4.4-bin-hadoop2.7.tgz && \
    mv spark-2.4.4-bin-hadoop2.7 /opt/spark && \
    rm spark-2.4.4-bin-hadoop2.7.tgz

ENV PATH="/opt/spark/bin/:$PATH"
ENV SPARK_HOME="/opt/spark"
ENV PYSPARK_PYTHON="/usr/bin/python3/python"

# Install Python packages
WORKDIR /
RUN python -m pip install --upgrade pip
RUN python -m pip install pyspark
RUN python -m pip install shapely
RUN python -m pip install pandas
RUN python -m pip install wheel
COPY geo_pyspark-0.2.0-py3-none-any.whl /
RUN python -m pip install geo_pyspark-0.2.0-py3-none-any.whl
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
WORKDIR /backend
COPY shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar .
COPY /backend .

WORKDIR $SPARK_HOME/jars
COPY geo_wrapper.jar .
COPY geospark-1.2.0.jar .
COPY geospark-sql_2.3-1.2.0.jar .
# TODO: Remove if we create another container for Spark streaming
EXPOSE 4444

# TODO: Is this needed?
EXPOSE 43345
#CMD ["python data_importer.py"]

ADD submit.sh /submit.sh
RUN chmod a+x /submit.sh
CMD ["/bin/bash", "/submit.sh"]
# CMD ["/bin/bash"]