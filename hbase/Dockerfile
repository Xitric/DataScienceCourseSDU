FROM openjdk:8

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends net-tools curl netcat

ENV HBASE_VERSION 2.2.2
RUN curl -O "http://mirrors.dotsrc.org/apache/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz" && \ 
    tar -xzvf hbase-$HBASE_VERSION-bin.tar.gz && \ 
    mv hbase-$HBASE_VERSION opt/hbase-$HBASE_VERSION && \ 
    rm hbase-$HBASE_VERSION-bin.tar.gz 

RUN ln -s /opt/hbase-$HBASE_VERSION/conf /etc/hbase
RUN mkdir /opt/hbase-$HBASE_VERSION/logs

ENV HBASE_HOME="/opt/hbase-$HBASE_VERSION"
ENV PATH="$HBASE_HOME/bin:$PATH"
ENV HBASE_CONF_DIR=/etc/hbase
ENV USER=root

ADD entrypoint.sh /entrypoint.sh
RUN chmod a+x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
