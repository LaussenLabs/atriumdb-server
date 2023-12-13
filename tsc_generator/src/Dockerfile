FROM python:3.10-bookworm

COPY packages /src/packages/
COPY . /src

RUN pip install -r /src/requirements.txt
RUN pip install /src/packages/wallib-1.0.0-py3-none-any.whl
#RUN pip install /src/packages/atriumdb-2.0.0-py3-none-any.whl[mariadb]

#runs main.py in our containers terminal
CMD ["python", "./src/main.py"]