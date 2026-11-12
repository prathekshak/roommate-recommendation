FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN python3 ml/preprocess.py
EXPOSE 8080 8201 8202 8203
