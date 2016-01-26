FROM rightsup/base:ubuntu
MAINTAINER RightsUp <it@rightsup.com>

# Make snappy gem install properly
RUN apt-get -y install libtool

RUN mkdir /kafka && \
    git clone https://github.com/apache/kafka.git /kafka && \
    cd /kafka && \
    git checkout tags/0.8.2.2
ENV KAFKA_PATH /kafka

RUN mkdir /app
WORKDIR /app

# Prepare the app
COPY . /app

RUN bundle install

# RUN bundle exec rake spec:all
RUN bash