# Load the environment variables from the .env file
source .env

# Configure AWS CLI using the environment variables
aws configure set aws_access_key_id "$S3_KEY"
aws configure set aws_secret_access_key "$S3_SECRET"
aws configure set region "$S3_REGION"

# Pull the repository from S3
bucket_name=$S3_BUCKET
s3_folder="lambda/convert-parquet"
local_folder="."

echo "Pulling repository from S3..."
git pull
aws s3 sync s3://$bucket_name/$s3_folder $local_folder
echo "Repository pulled successfully."