# Use an official Python runtime as a base image
FROM python:3.8-slim

# Set Python to unbuffered mode for development purpose
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the application
CMD ["python", "consumer.py"]