import json
import unittest
import boto3
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID as ACCOUNT_ID

from apache_beam.options import pipeline_options
from firehose_pyio.boto3_client import FirehoseClient, Boto3ClientError


def sample_s3_dest_config(bucket_name):
    """Return a simple extended s3 destination configuration."""
    return {
        "RoleARN": f"arn:aws:iam::{ACCOUNT_ID}:role/firehose-test-role",
        "BucketARN": f"arn:aws:s3::{bucket_name}",
    }


def create_delivery_stream(fh_client, **kwargs):
    fh_client.client.create_delivery_stream(**kwargs)


@mock_aws
class ClientErrorTest(unittest.TestCase):
    client = None
    s3_client = None
    delivery_stream_name = "test-delivery-stream"
    bucket_name = "firehose-test-bucket"

    def setUp(self):
        options = pipeline_options.PipelineOptions(
            [
                "--aws_access_key_id",
                "testing",
                "--aws_secret_access_key",
                "testing",
                "--aws_access_key_id",
                "testing",
                "--region_name",
                "us-east-1",
            ]
        )
        self.client = FirehoseClient(options)
        create_delivery_stream(
            self.client,
            DeliveryStreamName=self.delivery_stream_name,
            ExtendedS3DestinationConfiguration=sample_s3_dest_config(self.bucket_name),
        )
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.s3_client.create_bucket(Bucket=self.bucket_name)

    def test_is_delivery_stream_active_return_true_if_stream_is_active(self):
        is_active = self.client.is_delivery_stream_active(self.delivery_stream_name)
        self.assertEqual(is_active, True)

    def test_is_delivery_stream_active_raises_error_if_steam_is_not_existing(self):
        self.assertRaises(
            Boto3ClientError,
            self.client.is_delivery_stream_active,
            "non-existing-delivery-stream",
        )

    def test_put_record_batch_s3_destinations(self):
        boto_response = self.client.put_record_batch(
            self.delivery_stream_name, [json.dumps({"id": i}) for i in range(10)]
        )
        self.assertEqual(boto_response["FailedPutCount"], 0)

        bucket_objects = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        s3_response = self.s3_client.get_object(
            Bucket=self.bucket_name, Key=bucket_objects["Contents"][0]["Key"]
        )
        self.assertEqual(s3_response["Body"].read().decode(), '{"id": 9}')
