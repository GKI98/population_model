FROM python:3.9

COPY requirements.txt /

RUN pip3 install -r requirements.txt

COPY scripts/ /scripts
COPY cli.py /

WORKDIR /

CMD [ "python", "cli.py" ]
