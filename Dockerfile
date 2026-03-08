FROM python:3.13

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt

COPY . /app

EXPOSE 8080

ENV PORT 8080

CMD ["streamlit", "run", "app.py", "--server.port=8080"]
