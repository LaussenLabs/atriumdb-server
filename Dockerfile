FROM python:3.10-bookworm

COPY atriumdb-server /src/atriumdb-server/
COPY . /src

RUN python -m pip install --root-user-action=ignore -r /src/requirements.txt \
    && python -m pip cache purge
#runs main.py in our containers terminal
WORKDIR /src