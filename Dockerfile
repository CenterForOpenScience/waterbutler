FROM python:3.13-slim

RUN usermod -d /home www-data && chown www-data:www-data /home

# Install dependancies
RUN apt-get update \
    && apt-get install -y \
        git \
        libevent-dev \
        libxml2-dev \
        libxslt1-dev \
        zlib1g-dev \
        # cryptography
        build-essential \
        libssl-dev \
        libffi-dev \
        gnupg2 \
        # grab gosu for easy step-down from root
        cargo \
        rustc \
        gosu \
    && apt-get clean \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /code
WORKDIR /code

RUN pip install -U pip==24.0
RUN pip install setuptools==69.5.1

COPY ./requirements.txt /code/

RUN pip install --no-cache-dir -r /code/requirements.txt

# Copy the rest of the code over
COPY ./ /code/



ARG GIT_COMMIT=
ENV GIT_COMMIT=${GIT_COMMIT}

RUN python setup.py develop

EXPOSE 7777

CMD ["gosu", "www-data", "invoke", "server"]
