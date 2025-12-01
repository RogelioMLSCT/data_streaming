FROM apache/airflow:3.1.3

USER root
RUN apt-get update -qq && \
    apt-get install -yqq vim python3-pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"
RUN DOWNLOAD_URL="https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && mkdir -p "${JAVA_HOME}" \
    && curl -fL "${DOWNLOAD_URL}" -o "${TMP_DIR}/jdk.tar.gz" \
    && tar xzf "${TMP_DIR}/jdk.tar.gz" -C "${JAVA_HOME}" --strip-components=1 \
    && rm -rf "${TMP_DIR}"


USER ${AIRFLOW_UID}
    
COPY airflow/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt