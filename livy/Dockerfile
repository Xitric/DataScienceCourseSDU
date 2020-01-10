FROM xitric/spark-executor:latest

RUN apt-get update && apt-get -yq install unzip procps

ENV LIVY_VERSION 0.6.0
WORKDIR /
RUN curl -O http://mirrors.dotsrc.org/apache/incubator/livy/$LIVY_VERSION-incubating/apache-livy-$LIVY_VERSION-incubating-bin.zip && \
    unzip apache-livy-$LIVY_VERSION-incubating-bin.zip && \
    mv apache-livy-$LIVY_VERSION-incubating-bin /opt/livy-$LIVY_VERSION && \
    rm apache-livy-$LIVY_VERSION-incubating-bin.zip

ENV LIVY_HOME=/opt/livy-$LIVY_VERSION
ENV LIVY_CONF_DIR=$LIVY_HOME/conf
ENV PATH=$LIVY_HOME/bin/:$PATH

COPY livy.conf $LIVY_CONF_DIR

EXPOSE 8998
EXPOSE 4000
EXPOSE 4001

ADD run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/bin/bash", "/run.sh"]
