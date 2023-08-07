# Use an Python base image
FROM python:3.10.8

# Set working directory
WORKDIR /app

# Copy files
COPY requirements.txt requirements.txt
COPY etl.py etl.py 

# Install package with pip
RUN pip install -r requirements.txt

# Run container as an executable
ENTRYPOINT [ "python", "etl.py" ]