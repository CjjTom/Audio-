# Use official slim python base
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install ffmpeg and system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    build-essential \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create app dir
WORKDIR /app

# Copy requirements
COPY requirements.txt /app/requirements.txt

# Install python deps
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy app code
COPY main.py /app/main.py

# Create downloads dir and set permissions
RUN mkdir -p /tmp/downloads && chmod 777 /tmp/downloads

# Expose port for web server
EXPOSE 8080

CMD ["python", "main.py"]
