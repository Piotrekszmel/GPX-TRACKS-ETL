# Use a Python base image
FROM python:3.8-slim-bullseye

# Set the working directory in the container
WORKDIR /app

RUN apt-get update && \
    apt-get -y upgrade && \
    pip install --upgrade pip

# Copy the requirements file to the container
COPY requirements.txt .

# Install the dependencies from the requirements file
RUN pip install -r requirements.txt

WORKDIR /project
# Copy the code files to the container
COPY etlgpx /project/etlgpx
COPY scripts /project/scripts
COPY data /project/data
# Set the default command to run when the container starts
CMD ["python", "-m", "etlgpx.main"]