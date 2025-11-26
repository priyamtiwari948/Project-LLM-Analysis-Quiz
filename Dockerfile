FROM python:3.10

# System deps for Chromium
RUN apt-get update && apt-get install -y wget unzip libx11-xcb1 libnss3 libxcomposite1 libxcursor1 \
    libasound2 libatk1.0-0 libatk-bridge2.0-0 libcups2 libxkbcommon0 libxrandr2 libgbm1 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN pip install --upgrade pip && pip install -r requirements.txt
RUN playwright install --with-deps chromium

# HF expects an app on 7860
EXPOSE 7860
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "7860"]
