FROM python:3.10-bookworm

COPY services /src/services/
COPY lib /src/lib
COPY . /src

RUN pip install -r /src/requirements.txt

#runs main.py in our containers terminal
#CMD ["python", "./src/main.py"]