import json
import boto3
from botocore.exceptions import ClientError

COMMON_NAME = "firehose-pyio-test"


def create_destination_bucket(bucket_name):
    client = boto3.client("s3")
    suffix = client._client_config.region_name
    client.create_bucket(Bucket=f"{bucket_name}-{suffix}")


def create_firehose_iam_role(role_name):
    assume_role_policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "firehose.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    client = boto3.client("iam")
    try:
        return client.get_role(RoleName=role_name)
    except ClientError as error:
        if error.response["Error"]["Code"] == "NoSuchEntity":
            resp = client.create_role(
                RoleName=role_name, AssumeRolePolicyDocument=assume_role_policy_document
            )
            client.attach_role_policy(
                RoleName=role_name,
                PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess",
            )
            return resp


def create_delivery_stream(delivery_stream_name, role_arn, bucket_name):
    client = boto3.client("firehose")
    suffix = client._client_config.region_name
    try:
        client.create_delivery_stream(
            DeliveryStreamName=delivery_stream_name,
            DeliveryStreamType="DirectPut",
            S3DestinationConfiguration={
                "RoleARN": role_arn,
                "BucketARN": f"arn:aws:s3:::{bucket_name}-{suffix}",
                "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 0},
            },
        )
    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceInUseException":
            pass
        else:
            raise error


if __name__ == "__main__":
    print("create a destination bucket...")
    create_destination_bucket(COMMON_NAME)
    print("create an iam role...")
    iam_resp = create_firehose_iam_role(COMMON_NAME)
    print("create a delivery stream...")
    create_delivery_stream(COMMON_NAME, iam_resp["Role"]["Arn"], COMMON_NAME)
