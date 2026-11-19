FROM python:3.11-slim-bookworm
WORKDIR /app
COPY services/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/ ./services/
COPY data/ ./data/
COPY schemas/ ./schemas/
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m"]
CMD ["services.cli", "--help"]
