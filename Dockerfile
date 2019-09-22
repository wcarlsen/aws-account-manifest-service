FROM python:slim-stretch

RUN apt update

RUN pip install --upgrade pip
RUN pip install pipenv

RUN mkdir -p /var/app
WORKDIR /var/app
COPY . /var/app

RUN pipenv install --system

ENTRYPOINT ["python", "src/service.py"]
