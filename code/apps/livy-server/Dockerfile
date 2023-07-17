FROM docker.io/bitnami/spark:2

USER root
ENV LIVY_HOME /opt/bitnami/livy
WORKDIR /opt/bitnami/
COPY requirements.txt /opt/bitnami/
COPY test.csv /opt/bitnami/
RUN install_packages unzip \
    && apt-get update \
    && apt-get install -y gcc \
    && apt-get -y install curl \
    && curl "https://downloads.apache.org/incubator/livy/0.7.1-incubating/apache-livy-0.7.1-incubating-bin.zip" -O \
    && unzip "apache-livy-0.7.1-incubating-bin" \
    && rm -rf "apache-livy-0.7.1-incubating-bin.zip" \
    && mv "apache-livy-0.7.1-incubating-bin" $LIVY_HOME \
    && mkdir $LIVY_HOME/logs \
    && chown -R 1001:1001 $LIVY_HOME \
    && curl "https://jdbc.postgresql.org/download/postgresql-42.3.7.jar" -O \
    && mv "postgresql-42.3.7.jar" $LIVY_HOME \
    && pip install -r requirements.txt
USER 1001
