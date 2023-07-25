FROM python:3.9.16-slim
ARG openjdk_version="11"
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update --yes && \
    apt-get install --yes curl "openjdk-${openjdk_version}-jre-headless" ca-certificates-java procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
#
# Add repository code
WORKDIR /opt/dagster/app
COPY requirements.txt /opt/dagster/app
RUN pip install --upgrade pip && pip uninstall pandas && pip install -r requirements.txt
WORKDIR /opt/dagster/app
# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]