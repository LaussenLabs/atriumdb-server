FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    wget \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/conda/bin:${PATH}"
ENV TZ="America/Toronto"
ARG miniconda=Miniconda3-py310_23.10.0-1-Linux-x86_64.sh

RUN wget \
    https://repo.anaconda.com/miniconda/${miniconda} \
    && mkdir /root/.conda \
    && mkdir -p /opt \
    && bash ${miniconda} -b -p /opt/conda \
    && ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh \
    && echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc \
    && echo "conda activate base" >> ~/.bashrc\
    && find /opt/conda/ -follow -type f -name '*.a' -delete\
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete\
    && rm -f ${miniconda} \
    && /opt/conda/bin/conda clean -afy \
    && conda init bash

COPY atriumdb-server /src/atriumdb-server/
COPY . /src

RUN python -m pip install --root-user-action=ignore -e . \
    && python -m pip install --root-user-action=ignore -r /src/requirements.txt \
    && python -m pip cache purge
#runs main.py in our containers terminal
WORKDIR /src
CMD ["python", "atriumdb-server"]