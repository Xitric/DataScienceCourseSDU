# Using standard ubuntu LTS version
FROM ubuntu:18.04

# Settings labels
LABEL Name=flume Version=0.0.1 authors="Jakob Hviid <jah@mmmi.sdu.dk>, Kasper Schultz Davidsen <kdavi16@student.sdu.dk>"

# installing needed packages
RUN apt-get update
RUN apt-get install -y curl openjdk-8-jre-headless vim nano telnet
RUN apt-get autoremove -y
RUN apt-get clean

# Getting FLume Binaries
ENV FLUME_VERSION 1.9.0
RUN curl -O http://ftp.download-by.net/apache/flume/$FLUME_VERSION/apache-flume-$FLUME_VERSION-bin.tar.gz && \
    tar -zxvf apache-flume-$FLUME_VERSION-bin.tar.gz && \
    mv apache-flume-$FLUME_VERSION-bin opt/flume-$FLUME_VERSION && \
    rm apache-flume-$FLUME_VERSION-bin.tar.gz

RUN ln -s /opt/flume-$FLUME_VERSION/conf /etc/flume

# Setting up environment variables
ENV FLUME_HOME="/opt/flume-$FLUME_VERSION"
ENV PATH="$FLUME_HOME/bin:$PATH"
ENV FLUME_CONF_DIR="/etc/flume"
ENV JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-amd64/jre"

# Prepare to run Flume with the required dependencies
COPY run.sh /run.sh
RUN chmod +x /run.sh

WORKDIR /opt/flume-$FLUME_VERSION
COPY components/target/sfgov-flume-components-1.0.0.jar plugins.d/sfgov/lib/sfgov-flume-components-1.0.0.jar
ADD flume.conf conf/

ENTRYPOINT ["/run.sh"]
