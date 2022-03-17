FROM python:3.9

COPY req.txt /

RUN pip3 install -r req.txt

COPY scripts /
COPY cli.py /

WORKDIR /

ENTRYPOINT ["python3" "cli.py" "--"]
