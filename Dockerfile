# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Create a non-root user and group with an arbitrary UID and GID
RUN groupadd -g 1000 myuser && useradd -u 1000 -g myuser -s /bin/sh myuser

# Set the working directory in the container to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY ./main.py /app
COPY ./requirements.txt /app

# Change the ownership of the /app directory to the non-root user
RUN chown -R myuser:myuser /app

# Switch to the non-root user
USER myuser

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run app.py when the container launches
CMD ["python", "-u", "main.py"]
