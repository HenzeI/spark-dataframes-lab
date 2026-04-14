FROM profesorbigdata/spark-base-image_4_1_1_new:latest

USER root

ENV SPARK_HOME=/opt/bd/spark-4.1.1-bin-hadoop3

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    python3-venv \
    && python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir \
       jupyterlab \
       ipykernel \
       pandas \
       pyarrow \
       matplotlib \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/venv/bin:${SPARK_HOME}/bin:${PATH}"

RUN PYVER=$(/opt/venv/bin/python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")') && \
    PY4J_ZIP=$(ls ${SPARK_HOME}/python/lib/py4j-*-src.zip | head -n 1) && \
    printf '%s\n%s\n' \
      "${SPARK_HOME}/python" \
      "${PY4J_ZIP}" \
      > /opt/venv/lib/python${PYVER}/site-packages/spark.pth

RUN mkdir -p /opt/notebooks /opt/spark-apps /tmp/spark-events

WORKDIR /opt/notebooks

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--ServerApp.allow_origin=*", "--IdentityProvider.token=spark"]
