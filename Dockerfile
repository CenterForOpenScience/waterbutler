FROM python:3.5-slim-jessie

RUN usermod -d /home www-data && chown www-data:www-data /home

# Install dependancies
RUN apt-get update \
    && apt-get install -y \
        git \
        par2 \
        libevent-dev \
        libxml2-dev \
        libxslt1-dev \
        zlib1g-dev \
        # cryptography
        build-essential \
        libssl-dev \
        libffi-dev \
        python-dev \
    && apt-get clean \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# grab gosu for easy step-down from root
ENV GOSU_VERSION 1.4
RUN apt-get update \
    && apt-get install -y \
        curl \
        gnupg2 \
    && for key in \
      # GOSU
      B42F6819007F00F88E364FD4036A9C25BF357DD4 \
    ; do \
      gpg --keyserver hkp://ipv4.pool.sks-keyservers.net:80 --recv-keys "$key" || \
      gpg --keyserver hkp://ha.pool.sks-keyservers.net:80 --recv-keys "$key" || \
      gpg --keyserver hkp://pgp.mit.edu:80 --recv-keys "$key" || \
      gpg --keyserver hkp://keyserver.pgp.com:80 --recv-keys "$key" \
    ; done \
    && curl -o /usr/local/bin/gosu -SL "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)" \
  	&& curl -o /usr/local/bin/gosu.asc -SL "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc" \
  	&& gpg --verify /usr/local/bin/gosu.asc \
  	&& rm /usr/local/bin/gosu.asc \
  	&& chmod +x /usr/local/bin/gosu \
    && apt-get clean \
    && apt-get autoremove -y \
        curl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /code
WORKDIR /code

RUN pip install -U pip
RUN pip install setuptools==37.0.0

COPY ./requirements.txt /code/

RUN pip install --no-cache-dir -r /code/requirements.txt

# Copy the rest of the code over
COPY ./ /code/

ARG GIT_COMMIT=
ENV GIT_COMMIT ${GIT_COMMIT}

RUN python setup.py develop

EXPOSE 7777

CMD ["gosu", "www-data", "invoke", "server"]
