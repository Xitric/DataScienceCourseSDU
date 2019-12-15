FROM spark-executor:latest

# Enable local HBase connectivity
WORKDIR /backend
COPY shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar .
COPY /backend .

# Remove if we create another container for Spark streaming
EXPOSE 4000
EXPOSE 4001

ADD submit.sh /submit.sh
RUN chmod a+x /submit.sh

CMD ["/bin/bash", "/submit.sh"]
