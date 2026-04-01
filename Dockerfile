FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    python -c "import textblob; textblob.download_corpora.download_all()" 2>/dev/null || true

# Copy application code
COPY . .

# Create data directory for SQLite
RUN mkdir -p /app/data

# Remove local .env (will use Claw Cloud env vars instead)
RUN rm -f /app/.env

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/jobs')" || exit 1

# Start server
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
