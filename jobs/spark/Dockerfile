FROM apache/spark-py:3.5.1

WORKDIR /app
COPY main.py /app/
COPY requirements.txt /app/

RUN pip install -r requirements.txt

ENTRYPOINT ["spark-submit", "/app/main.py"]
