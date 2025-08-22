# Use Python base image
FROM python:3.13

# Set working directory
WORKDIR /app

# Copy files
COPY requirements.txt .
COPY ga4_to_excel.py .
COPY credentials.json .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run script on container start
CMD ["python", "ga4_to_excel.py"]
