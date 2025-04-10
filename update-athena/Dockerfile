# Use AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.11

# Add all files
ADD . .

# Install dependencies
RUN pip install --upgrade pip && pip install -r requirements.txt

# Set the CMD to your handler
CMD ["main.lambda_handler"]
