FROM apache/airflow:2.10.2
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Add directory to store spark packages
RUN mkdir -p /opt/spark/jars/

# Copy spark packages to the directory
COPY spark_packages/*.jar /opt/spark/jars/

COPY start.sh /start.sh
RUN chmod +x /start.sh

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$JAVA_HOME/bin

# Create Hadoop directory with correct permissions
USER root
RUN mkdir -p $HADOOP_HOME/etc/hadoop && chown -R airflow:root $HADOOP_HOME && chmod -R 775 $HADOOP_HOME
USER airflow

# Set Spark classpath to include the downloaded JARs
ENV SPARK_CLASSPATH=/opt/spark/jars/*

ENTRYPOINT ["/bin/bash","/start.sh"]