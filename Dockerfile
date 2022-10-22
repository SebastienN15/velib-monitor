FROM python:alpine3.16

RUN apk add --update postgresql
RUN apk add libpq-dev

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

CMD ["python", "velib_api.py"]
