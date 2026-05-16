FROM python:3.11-slim

WORKDIR /app

# git нужен для pip install activity-hub @ git+https://...
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# GITHUB_TOKEN нужен для pip install приватного activity-hub
ARG GITHUB_TOKEN
RUN if [ -n "$GITHUB_TOKEN" ]; then \
      git config --global url."https://${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi
RUN pip install --no-cache-dir -r requirements.txt && \
    git config --global --unset url."https://github.com/".insteadOf 2>/dev/null || true

COPY app.py .

ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

EXPOSE 8501

# Railway инжектирует $PORT; fallback 8501 для локального запуска
CMD ["sh", "-c", "streamlit run app.py --server.port=${PORT:-8501}"]
