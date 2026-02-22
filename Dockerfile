FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY gem_finder.py .

# Default: run once and exit (Railway cron job)
CMD ["python", "gem_finder.py"]
