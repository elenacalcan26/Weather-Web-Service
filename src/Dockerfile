FROM python:3.8

COPY requirements.txt /tmp
RUN pip3 install --upgrade pip
RUN pip3 install -r /tmp/requirements.txt

WORKDIR /src
COPY *.py /src

EXPOSE 6000

CMD ["python3", "server.py"]
