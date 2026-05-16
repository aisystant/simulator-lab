FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# activity_hub вендорится прямо в репо (не нужен git при сборке)
COPY activity_hub/ ./activity_hub/
COPY app.py .

ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

EXPOSE 8501

# Railway инжектирует $PORT; fallback 8501 для локального запуска
CMD ["sh", "-c", "streamlit run app.py --server.port=${PORT:-8501}"]
