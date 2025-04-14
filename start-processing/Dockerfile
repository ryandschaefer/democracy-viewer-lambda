# Use AWS Lambda Python base image
FROM amazonlinux:latest

# Set the working directory within docker image
WORKDIR /usr/src/app

# Add all files
ADD . .

# Install Python dependencies
RUN yum install -y python3.11 python3.11-pip git
RUN python3.11 -m pip install -r requirements.txt
