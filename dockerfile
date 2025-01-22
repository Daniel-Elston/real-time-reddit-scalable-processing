FROM python:3.12

WORKDIR /live-reddit-sentiment

COPY . /live-reddit-sentiment

RUN pip install -r requirements.txt

CMD ["python", "main.py"]
