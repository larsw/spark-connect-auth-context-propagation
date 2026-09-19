# A PySpark Connect client that lives on the compose network.
#
# The interactive demo runs on the host (see install.sh and the /etc/hosts aliases). This image
# exists so the automated suite can run with no host configuration at all -- useful for CI, and
# for proving the stack before anyone touches /etc/hosts.
FROM python:3.12-slim

RUN pip install --no-cache-dir \
      pyspark-client==4.1.3 \
      pytest>=8.3 \
      pytest-timeout>=2.3

WORKDIR /work
ENV PYTHONPATH=/work/client/src
ENV PYTHONUNBUFFERED=1
