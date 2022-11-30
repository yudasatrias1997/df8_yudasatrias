FROM python:3.11
# Path: DockerFile
WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

CMD [ "python", "main.py" ]
