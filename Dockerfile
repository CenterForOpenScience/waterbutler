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

COPY pyproject.toml poetry.lock* /code/
RUN pip install poetry==2.1.2
RUN pip install setuptools==80.1.0
RUN poetry install --no-root --without=docs

# Copy the rest of the code over
COPY ./ /code/

ARG GIT_COMMIT=
ENV GIT_COMMIT=${GIT_COMMIT}
ENV POETRY_NO_INTERACTION=1
ENV POETRY_VIRTUALENVS_CREATE=0
ENV POETRY_VIRTUALENVS_IN_PROJECT=1

RUN python3 setup.py egg_info
RUN python3 -m pip install .

EXPOSE 7777

CMD ["gosu", "www-data", "python3", "-m", "invoke", "server"]
