FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY bot.py .
COPY web_app.py .
COPY message.py .
COPY commands/ ./commands/
COPY database/ ./database/
COPY utils/ ./utils/
COPY torn_api/ ./torn_api/

# Copy data/api_inventory files (static API inventory JSON files)
# These are static JSON files that should be part of the image
COPY data/api_inventory/ ./data/api_inventory/

# Create data directory for database (will be volume mounted)
# The database file itself will be stored in a Docker volume
RUN mkdir -p /app/data

# Set proper permissions
RUN chmod -R 755 /app

# Default command (can be overridden in docker-compose)
CMD ["python", "bot.py"]
