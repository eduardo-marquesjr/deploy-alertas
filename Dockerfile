FROM python:3.8.3-slim

COPY ./requirements.txt /usr/requirements.txt

WORKDIR /usr

RUN pip3 install -r requirements.txt

COPY ./app /usr/app

ENTRYPOINT [ "python3" ]

CMD [ "app/main_alertas.py" ] 