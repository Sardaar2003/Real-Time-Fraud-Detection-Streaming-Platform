# Use official, minimal Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Set the working directory to /app
WORKDIR /app

# Copy the requirements file and install dependencies
COPY generator/requirements.txt /app/generator/requirements.txt
RUN pip install --no-cache-dir -r /app/generator/requirements.txt

# Copy the application source code and schema files
# This mimics the directory structure of the repository, enabling relative path loading
COPY schemas/ /app/schemas/
COPY generator/ /app/generator/

# Create a non-root system user for security compliance (Least Privilege)
RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup -s /bin/bash appuser && \
    chown -R appuser:appgroup /app

USER appuser

# Set working directory to the generator folder
WORKDIR /app/generator

# Run the generator script when the container launches
ENTRYPOINT ["python", "generator.py"]
CMD ["--rate", "2.0", "--fraud-rate", "0.05"]
