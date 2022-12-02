FROM python:3.8

COPY requirements.txt /tmp
RUN pip3 install --upgrade pip
RUN pip3 install -r /tmp/requirements.txt

WORKDIR /src
COPY server.py /src

EXPOSE 500

CMD ["python3", "server.py"]
