FROM bde2020/hadoop-nodemanager:2.0.0-hadoop3.1.2-java8

# Install Python
ENV PYTHON_VERSION 3.7.5
RUN apt-get update && apt-get -yq install locales build-essential checkinstall
RUN apt-get -yq install libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev

RUN curl -O https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz && \
    mkdir -p /usr/bin/python && \
    tar -zxvf Python-$PYTHON_VERSION.tgz -C /usr/bin/python --strip-components 1 && \
    rm Python-$PYTHON_VERSION.tgz
 
WORKDIR /usr/bin/python
RUN ./configure --enable-optimizations
RUN make altinstall
 
WORKDIR /
ENV PATH="/usr/bin/python:$PATH"

# Install Python packages
WORKDIR /
RUN python3.7 -m pip install --upgrade pip
RUN python3.7 -m pip install shapely
RUN python3.7 -m pip install pandas
RUN python3.7 -m pip install wheel
COPY geo_pyspark-0.2.0-py3-none-any.whl /
RUN python3.7 -m pip install geo_pyspark-0.2.0-py3-none-any.whl
RUN apt-get autoremove -y
RUN apt-get clean
