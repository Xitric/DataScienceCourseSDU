FROM xitric/spark-executor:latest

RUN apt-get update && apt-get -yq install zip

# Enable local HBase connectivity
WORKDIR /backend
COPY shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar .
COPY mysql-connector-java-8.0.18.jar .
COPY /backend .

RUN zip -r files.zip *

# Remove if we create another container for Spark streaming
EXPOSE 4000
EXPOSE 4001

ADD submit.sh /submit.sh
RUN chmod a+x /submit.sh

CMD ["/bin/bash", "/submit.sh"]
