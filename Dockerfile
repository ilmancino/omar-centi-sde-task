FROM python:3.12.2-bullseye

RUN useradd -ms /bin/bash -d /home/mancino mancino
ENV WORK_PATH=/home/mancino
WORKDIR ${WORK_PATH}

COPY requirements.txt .
RUN pip install -q --no-cache-dir -r requirements.txt

COPY . ${WORK_PATH}

ENTRYPOINT ["python", "./python/ingestor.py"]
