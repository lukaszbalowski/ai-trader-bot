FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instalacja przeglądarki dla trybu Fallback
RUN playwright install chromium
RUN playwright install-deps

COPY . .

RUN mkdir -p /app/data

ENV PYTHONUNBUFFERED=1

# Domyślna komenda startowa (wartość portfolio możesz nadpisać przy uruchamianiu)
CMD ["python", "main.py", "--portfolio", "100"]