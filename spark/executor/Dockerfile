FROM xitric/spark-base:latest

# Install Python packages
WORKDIR /
RUN python -m pip install --upgrade pip
RUN python -m pip install pyspark
RUN python -m pip install shapely
RUN python -m pip install pandas
RUN python -m pip install wheel
COPY geo_pyspark-0.2.0-py3-none-any.whl /
RUN python -m pip install geo_pyspark-0.2.0-py3-none-any.whl

# Add configurations
COPY hbase-site.xml $HADOOP_CONF_DIR/

# Add geopspark files
WORKDIR $SPARK_HOME/jars
COPY /jars .
