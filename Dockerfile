FROM python:3.9

COPY requirements.txt /

RUN pip3 install -r requirements.txt

COPY scripts/ /
COPY balance_social_stats.py /
COPY runner.py /

WORKDIR /

CMD [ "python", "runner.py" ]
