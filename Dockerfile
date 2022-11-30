FROM python:latest
# Path: DockerFile
WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

CMD [ "python", "./main.py" ]
