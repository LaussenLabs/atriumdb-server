FROM python:3.10-bookworm

COPY . /src

RUN python -m pip install --root-user-action=ignore -r /src/requirements.txt \
    && python -m pip install --root-user-action=ignore /src/atriumdb-2.1.1-py3-none-any.whl[mariadb] \
    && python -m pip install --root-user-action=ignore tqdm \
    && python -m pip install --root-user-action=ignore -e /src/lib \
    && python -m pip cache purge
#runs main.py in our containers terminal
WORKDIR /src
CMD ["/bin/bash"]