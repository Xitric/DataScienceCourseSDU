FROM spark-executor:latest

# Enable local HBase connectivity
WORKDIR /backend
COPY shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar .
COPY /backend .


# Enable history server connectivity
WORKDIR $SPARK_HOME/jars
COPY /jars .
COPY geo_wrapper.jar .
COPY geospark-1.2.0.jar .
COPY geospark-sql_2.3-1.2.0.jar .

# Remove if we create another container for Spark streaming
EXPOSE 4000
EXPOSE 4001

ADD submit.sh /submit.sh
RUN chmod a+x /submit.sh

CMD ["/bin/bash", "/submit.sh"]
