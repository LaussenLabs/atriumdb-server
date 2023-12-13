FROM python:3.10-bookworm

COPY async_wal_writer_siriDB.py .
COPY walwriter /walwriter/
COPY packages /packages/
COPY helpers /helpers/
COPY requirements.txt .

RUN pip install -r requirements.txt
RUN pip install /packages/wallib-1.0.0-py3-none-any.whl
#RUN pip install /src/packages/atriumdb-2.0.0-py3-none-any.whl[mariadb]

CMD ["python", "./async_wal_writer_siriDB.py"]