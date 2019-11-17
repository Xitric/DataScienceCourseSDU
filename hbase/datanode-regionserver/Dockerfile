# # FROM openjdk:8-alpine
# FROM xitric/hbase-regionserver:latest

# RUN apk add -q wget

# ENV HBASE_VERSION="2.2.2"

# RUN wget -q "http://mirrors.dotsrc.org/apache/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz" && \
#     tar -xzf hbase-$HBASE_VERSION-bin.tar.gz && \
#     mv hbase-$HBASE_VERSION opt/hbase-$HBASE_VERSION && \
#     rm hbase-$HBASE_VERSION-bin.tar.gz

# WORKDIR /opt/hbase-$HBASE_VERSION
# # Credit to Dave Beckett, https://github.com/dajobe
# RUN sed -i "s,^. export JAVA_HOME.*,export JAVA_HOME=$JAVA_HOME," conf/hbase-env.sh

# ENV HBASE_HOME="/opt/hbase-$HBASE_VERSION"
# ENV PATH="$HBASE_HOME/bin:$PATH"

# COPY hbase-site.xml conf/hbase-site.xml
# COPY regionservers conf/regionservers
# # CMD [ "start-hbase.sh" ]

# EXPOSE 16000 16010

FROM bde2020/hadoop-datanode:2.0.0-hadoop3.1.2-java8

ENV HBASE_VERSION="2.2.2"

RUN curl -O "http://mirrors.dotsrc.org/apache/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz" && \
    tar -xzf hbase-$HBASE_VERSION-bin.tar.gz && \
    mv hbase-$HBASE_VERSION opt/hbase-$HBASE_VERSION && \
    rm hbase-$HBASE_VERSION-bin.tar.gz

WORKDIR /opt/hbase-$HBASE_VERSION

ENV HBASE_HOME="/opt/hbase-$HBASE_VERSION"
ENV PATH="$HBASE_HOME/bin:$PATH"

COPY hbase-site.xml conf/hbase-site.xml

COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]
