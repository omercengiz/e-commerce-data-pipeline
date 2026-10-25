FROM python:3.10-slim-bookworm

# Java (For Spark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk unzip && \
    ln -s "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" /opt/java && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/opt/java
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV PYTHONPATH=/app

# Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

WORKDIR /app
COPY . /app

CMD ["bash"]
