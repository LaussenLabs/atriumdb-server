FROM python:3.10-bookworm

COPY lib /src
COPY rest_api /src
COPY tsc_generator /src
COPY wal_writer /src
COPY atriumdb-server /src
COPY requirements.txt /src

RUN python -m pip install --root-user-action=ignore -r /src/requirements.txt \
    && python -m pip install --root-user-action=ignore -e /src/lib \
    && python -m pip cache purge
#runs main.py in our containers terminal
WORKDIR /src
CMD ["/bin/bash"]