FROM openjdk:8-alpine

ENV HBASE_VERSION="2.2.2"

RUN apk update && apk add -q --no-cache curl bash openssh

RUN curl -O "http://mirrors.dotsrc.org/apache/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz" && \
    tar -xzf hbase-$HBASE_VERSION-bin.tar.gz && \
    mv hbase-$HBASE_VERSION opt/hbase-$HBASE_VERSION && \
    rm hbase-$HBASE_VERSION-bin.tar.gz

RUN apk add -q --no-cache perl

RUN ln -s /opt/hbase-$HBASE_VERSION/conf /etc/hbase

WORKDIR /opt/hbase-$HBASE_VERSION

ENV HBASE_HOME="/opt/hbase-$HBASE_VERSION"
ENV PATH="$HBASE_HOME/bin:$PATH"

COPY hbase-site.xml conf/hbase-site.xml
COPY regionservers conf/regionservers

ADD entrypoint.sh /entrypoint.sh
RUN chmod a+x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]
