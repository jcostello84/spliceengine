FROM base-mvn

RUN apt-get update \
    && apt-get -y install gcc g++ make wget unzip netcat

RUN cd /tmp \
    && wget https://github.com/google/protobuf/releases/download/v2.4.1/protobuf-2.4.1.zip \
    && unzip protobuf-2.4.1.zip \
    && cd protobuf-2.4.1 \
    && ./configure --prefix=/usr --disable-shared \
    && make && make install

