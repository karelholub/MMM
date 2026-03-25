# syntax=docker/dockerfile:1.7
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt
# Copy only app code and migrations (smaller layer, faster export)
COPY app ./app
COPY migrations ./migrations
RUN mkdir -p /app/app/data
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
