FROM python:3.12-slim

WORKDIR /app

# Install dependencies first for better Docker layer caching
COPY services/auth/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared infrastructure package
COPY shared/ /app/shared/

# Copy service code
COPY services/auth/ /app/

EXPOSE 8001

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
