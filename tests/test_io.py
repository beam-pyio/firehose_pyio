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

import unittest
import boto3
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID as ACCOUNT_ID
import apache_beam as beam
from apache_beam.options import pipeline_options
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from firehose_pyio.boto3_client import FirehoseClient, Boto3ClientError
from firehose_pyio.io import WriteToFirehose


def sample_s3_dest_config(bucket_name):
    """Return a simple extended s3 destination configuration."""
    return {
        "RoleARN": f"arn:aws:iam::{ACCOUNT_ID}:role/firehose-test-role",
        "BucketARN": f"arn:aws:s3::{bucket_name}",
    }


def create_delivery_stream(fh_client, **kwargs):
    fh_client.client.create_delivery_stream(**kwargs)


def collect_bucket_contents(s3_client, bucket_name):
    bucket_contents = []
    bucket_objects = s3_client.list_objects_v2(Bucket=bucket_name)
    for contents in bucket_objects["Contents"]:
        s3_response = s3_client.get_object(Bucket=bucket_name, Key=contents["Key"])
        bucket_contents.append(s3_response["Body"].read().decode())
    return bucket_contents


@mock_aws
class TestWriteToFirehose(unittest.TestCase):
    delivery_stream_name = "test-delivery-stream"
    bucket_name = "firehose-test-bucket"

    def setUp(self):
        options = {
            "aws_access_key_id": "testing",
            "aws_secret_access_key": "testing",
            "region_name": "us-east-1",
        }

        fh_client = FirehoseClient(options)
        create_delivery_stream(
            fh_client,
            DeliveryStreamName=self.delivery_stream_name,
            ExtendedS3DestinationConfiguration=sample_s3_dest_config(self.bucket_name),
        )
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.s3_client.create_bucket(Bucket=self.bucket_name)

        self.pipeline_opts = pipeline_options.PipelineOptions(
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

    def test_write_to_firehose_with_unsupported_types(self):
        # accepts iterable except for string
        with self.assertRaises(TypeError):
            with TestPipeline(options=self.pipeline_opts) as p:
                (
                    p
                    | beam.Create(["one", "two", "three", "four"])
                    | WriteToFirehose(self.delivery_stream_name, True)
                    | beam.Map(lambda e: e["FailedPutCount"])
                )

    def test_write_to_firehose_with_invalid_typed_list_elements(self):
        # parameter validation error if not <class 'bytes'>, <class 'bytearray'>, file-like object
        with self.assertRaises(Boto3ClientError):
            with TestPipeline(options=self.pipeline_opts) as p:
                (
                    p
                    | beam.Create([[1, 2, 3, 4]])
                    | WriteToFirehose(self.delivery_stream_name, False)
                    | beam.Map(lambda e: e["FailedPutCount"])
                )

    def test_write_to_firehose_with_list_elements(self):
        with TestPipeline(options=self.pipeline_opts) as p:
            output = (
                p
                | beam.Create([["one", "two", "three", "four"], [1, 2, 3, 4]])
                | WriteToFirehose(self.delivery_stream_name, True)
                | beam.Map(lambda e: e["FailedPutCount"])
            )
            assert_that(output, equal_to([0, 0]))

        bucket_contents = collect_bucket_contents(self.s3_client, self.bucket_name)
        self.assertSetEqual(
            set(bucket_contents), set(['"one""two""three""four"', "1234"])
        )

    def test_write_to_firehose_with_tuple_elements(self):
        with TestPipeline(options=self.pipeline_opts) as p:
            output = (
                p
                | beam.Create([(1, ["one", "two", "three", "four"]), (2, [1, 2, 3, 4])])
                | WriteToFirehose(self.delivery_stream_name, True)
                | beam.Map(lambda e: e["FailedPutCount"])
            )
            assert_that(output, equal_to([0, 0]))

        bucket_contents = collect_bucket_contents(self.s3_client, self.bucket_name)
        self.assertSetEqual(
            set(bucket_contents), set(['"one""two""three""four"', "1234"])
        )
