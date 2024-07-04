#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import datetime
import unittest
import boto3
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID as ACCOUNT_ID

from firehose_pyio.boto3_client import FirehoseClient, FirehoseClientError


def sample_s3_dest_config(bucket_name):
    """Return a simple extended s3 destination configuration."""
    return {
        "RoleARN": f"arn:aws:iam::{ACCOUNT_ID}:role/firehose-test-role",
        "BucketARN": f"arn:aws:s3::{bucket_name}",
    }


def create_delivery_stream(fh_client, **kwargs):
    fh_client.client.create_delivery_stream(**kwargs)


@mock_aws
class TestBoto3Client(unittest.TestCase):
    fh_client = None
    s3_client = None
    delivery_stream_name = "test-delivery-stream"
    bucket_name = "firehose-test-bucket"

    def setUp(self):
        options = {
            "aws_access_key_id": "testing",
            "aws_secret_access_key": "testing",
            "region_name": "us-east-1",
        }

        self.fh_client = FirehoseClient(options)
        create_delivery_stream(
            self.fh_client,
            DeliveryStreamName=self.delivery_stream_name,
            ExtendedS3DestinationConfiguration=sample_s3_dest_config(self.bucket_name),
        )
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.s3_client.create_bucket(Bucket=self.bucket_name)

    def test_is_delivery_stream_active(self):
        # True if a stream is active
        is_active = self.fh_client.is_delivery_stream_active(self.delivery_stream_name)
        self.assertEqual(is_active, True)
        # ResourceNotFoundException
        self.assertRaises(
            FirehoseClientError,
            self.fh_client.is_delivery_stream_active,
            "non-existing-delivery-stream",
        )

    def test_put_record_batch_with_unsupported_types(self):
        self.assertRaises(
            TypeError,
            self.fh_client.put_record_batch,
            "abc",
            self.delivery_stream_name,
        )

        self.assertRaises(
            TypeError,
            self.fh_client.put_record_batch,
            123,
            self.delivery_stream_name,
        )

    def test_put_record_batch_without_converting_invalid_typed_records_to_json(
        self,
    ):
        # Parameter validation failed: valid types: <class 'bytes'>, <class 'bytearray'>, file-like object
        records = [1, 2, 3]
        self.assertRaises(
            FirehoseClientError,
            self.fh_client.put_record_batch,
            records,
            self.delivery_stream_name,
        )

    def test_put_record_batch_with_converting_unconvertable_records_to_json(self):
        records = [datetime.datetime.now()]
        # Parameter validation failed: valid types: <class 'bytes'>, <class 'bytearray'>, file-like object
        self.assertRaises(
            FirehoseClientError,
            self.fh_client.put_record_batch,
            records,
            self.delivery_stream_name,
            False,
        )
        # Object of type datetime is not JSON serializable
        self.assertRaises(
            FirehoseClientError,
            self.fh_client.put_record_batch,
            records,
            self.delivery_stream_name,
            True,
        )

    def test_put_record_batch_without_converting_records_to_json(self):
        records = ["one", "two", "three"]
        boto_response = self.fh_client.put_record_batch(
            records, self.delivery_stream_name, False
        )
        self.assertEqual(boto_response["FailedPutCount"], 0)

        bucket_objects = self.s3_client.list_objects(Bucket=self.bucket_name)
        s3_response = self.s3_client.get_object(
            Bucket=self.bucket_name, Key=bucket_objects["Contents"][0]["Key"]
        )
        self.assertEqual(s3_response["Body"].read().decode(), "onetwothree")

    def test_put_record_batch_with_converting_records_to_json(self):
        records = ["one", "two", "three"]
        boto_response = self.fh_client.put_record_batch(
            records, self.delivery_stream_name, True
        )
        self.assertEqual(boto_response["FailedPutCount"], 0)

        bucket_objects = self.s3_client.list_objects(Bucket=self.bucket_name)
        s3_response = self.s3_client.get_object(
            Bucket=self.bucket_name, Key=bucket_objects["Contents"][0]["Key"]
        )
        self.assertEqual(s3_response["Body"].read().decode(), '"one""two""three"')

    def test_put_record_batch_without_converting_empty_record_to_json(self):
        records = [""]
        boto_response = self.fh_client.put_record_batch(
            records, self.delivery_stream_name, False
        )
        self.assertEqual(boto_response["FailedPutCount"], 0)

        bucket_objects = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        s3_response = self.s3_client.get_object(
            Bucket=self.bucket_name, Key=bucket_objects["Contents"][0]["Key"]
        )
        self.assertEqual(s3_response["Body"].read().decode(), "")

    def test_put_record_batch_with_converting_empty_record_to_json(self):
        records = [""]
        boto_response = self.fh_client.put_record_batch(
            records, self.delivery_stream_name, True
        )
        self.assertEqual(boto_response["FailedPutCount"], 0)

        bucket_objects = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        s3_response = self.s3_client.get_object(
            Bucket=self.bucket_name, Key=bucket_objects["Contents"][0]["Key"]
        )
        self.assertEqual(s3_response["Body"].read().decode(), '""')

    def test_put_record_batch_with_converting_invalid_typed_records_to_json(self):
        records = [1, 2, 3]
        boto_response = self.fh_client.put_record_batch(
            records, self.delivery_stream_name, True
        )
        self.assertEqual(boto_response["FailedPutCount"], 0)

        bucket_objects = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        s3_response = self.s3_client.get_object(
            Bucket=self.bucket_name, Key=bucket_objects["Contents"][0]["Key"]
        )
        self.assertEqual(s3_response["Body"].read().decode(), "123")
