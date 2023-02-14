ARG DOCKER_HUB="index.docker.io"
ARG PYTHON_IMAGE=python:3.8.14-slim

FROM ${DOCKER_HUB}/${PYTHON_IMAGE}

# proxy
ARG http_proxy
ARG https_proxy
ENV http_proxy=$http_proxy
ENV https_proxy=$https_proxy

# set user
ARG SMDRM_UID=1000

# create unpriviledged user
RUN useradd -m -d /app --shell /bin/bash --uid $SMDRM_UID smdrm

# set working directory
WORKDIR /app

# login
USER $SMDRM_UID:$SMDRM_UID

# build virtualenv
ENV PATH=/app/venv/bin:$PATH
COPY requirements.txt /app/requirements.txt
RUN mkdir -p /app/venv /app/data && \
    python -m venv /app/venv && \
    pip install --no-cache-dir -U pip -r requirements.txt

# copy source code
COPY --chown=$SMDRM_UID:$SMDRM_UID build.py config.py VERSION ./

# runtime execution
CMD python build.py
