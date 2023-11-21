FROM ghcr.io/metricq/metricq-python:v5.3 AS BUILDER

USER root
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/* 

COPY --chown=metricq:metricq . /home/metricq/metricq_source_rabbitmq

USER metricq
WORKDIR /home/metricq/metricq_source_rabbitmq

RUN pip install --user .


FROM ghcr.io/metricq/metricq-python:v5.3

COPY --from=BUILDER --chown=metricq:metricq /home/metricq/.local /home/metricq/.local

ENTRYPOINT [ "/home/metricq/.local/bin/metricq-source-rabbitmq" ]
