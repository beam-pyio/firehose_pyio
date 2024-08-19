import json
import unittest
import pytest
import docker
import boto3
import apache_beam as beam
from apache_beam.transforms.util import BatchElements
from apache_beam import GroupIntoBatches
from apache_beam.options import pipeline_options
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from localstack_utils.localstack import startup_localstack, stop_localstack

from firehose_pyio.io import WriteToFirehose


def create_client(service_name):
    return boto3.client(
        service_name=service_name,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        endpoint_url="http://localhost:4566",
    )


def create_firehose_test_role(role_name):
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
    iam_client = create_client("iam")
    iam_client.create_role(
        RoleName=role_name, AssumeRolePolicyDocument=assume_role_policy_document
    )
    iam_client.attach_role_policy(
        RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
    )
    return iam_client.get_role(RoleName=role_name)


def create_destination_bucket(bucket_name):
    s3_client = create_client("s3")
    return s3_client.create_bucket(Bucket=bucket_name)


def create_delivery_stream(delivery_stream_name, role_name, bucket_name):
    fh_client = create_client("firehose")
    return fh_client.create_delivery_stream(
        DeliveryStreamName=delivery_stream_name,
        S3DestinationConfiguration={
            "RoleARN": f"arn:aws:iam::000000000000:role/{role_name}",
            "BucketARN": f"arn:aws:s3:::{bucket_name}",
        },
    )


def collect_bucket_contents(bucket_name):
    s3_client = create_client("s3")
    bucket_contents = []
    bucket_objects = s3_client.list_objects_v2(Bucket=bucket_name)
    for contents in bucket_objects["Contents"]:
        s3_response = s3_client.get_object(Bucket=bucket_name, Key=contents["Key"])
        bucket_contents.append(s3_response["Body"].read().decode())
    return bucket_contents


@pytest.mark.integration
class TestWriteToFirehose(unittest.TestCase):
    delivery_stream_name = "test-delivery-stream"
    bucket_name = "firehose-pyio-test-bucket"
    role_name = "firehose-test-role"

    def setUp(self):
        startup_localstack()
        ## create resources
        create_firehose_test_role(self.role_name)
        create_destination_bucket(self.bucket_name)
        create_delivery_stream(
            self.delivery_stream_name, self.role_name, self.bucket_name
        )

        self.pipeline_opts = pipeline_options.PipelineOptions(
            [
                "--runner",
                "FlinkRunner",
                "--parallelism",
                "1",
                "--aws_access_key_id",
                "testing",
                "--aws_secret_access_key",
                "testing",
                "--aws_access_key_id",
                "testing",
                "--region_name",
                "us-east-1",
                "--endpoint_url",
                "http://localhost:4566",
            ]
        )

    def tearDown(self):
        stop_localstack()
        docker_client = docker.from_env()
        docker_client.containers.prune()
        return super().tearDown()

    def test_write_to_firehose_with_list_to_batch_elements(self):
        with TestPipeline(options=self.pipeline_opts) as p:
            output = (
                p
                | beam.Create(["one", "two", "three", "four"])
                | BatchElements(min_batch_size=2, max_batch_size=2)
                | WriteToFirehose(self.delivery_stream_name, False, False)
            )
            assert_that(output[None], equal_to([]))

        bucket_contents = collect_bucket_contents(self.bucket_name)
        self.assertSetEqual(set(bucket_contents), set(["onetwo", "threefour"]))

    def test_write_to_firehose_with_tuple_to_group_into_batches(self):
        with TestPipeline(options=self.pipeline_opts) as p:
            output = (
                p
                | beam.Create([(1, "one"), (2, "three"), (1, "two"), (2, "four")])
                | GroupIntoBatches(batch_size=2)
                | WriteToFirehose(self.delivery_stream_name, False, False)
            )
            assert_that(output[None], equal_to([]))

        bucket_contents = collect_bucket_contents(self.bucket_name)
        self.assertSetEqual(set(bucket_contents), set(["onetwo", "threefour"]))

    def test_write_to_firehose_with_list_multilining(self):
        with TestPipeline(options=self.pipeline_opts) as p:
            output = (
                p
                | beam.Create(["one", "two", "three", "four"])
                | BatchElements(min_batch_size=2, max_batch_size=2)
                | WriteToFirehose(self.delivery_stream_name, False, True)
            )
            assert_that(output[None], equal_to([]))

        bucket_contents = collect_bucket_contents(self.bucket_name)
        self.assertSetEqual(set(bucket_contents), set(["one\ntwo\n", "three\nfour\n"]))

    def test_write_to_firehose_with_tuple_multilining(self):
        with TestPipeline(options=self.pipeline_opts) as p:
            output = (
                p
                | beam.Create([(1, "one"), (2, "three"), (1, "two"), (2, "four")])
                | GroupIntoBatches(batch_size=2)
                | WriteToFirehose(self.delivery_stream_name, False, True)
            )
            assert_that(output[None], equal_to([]))

        bucket_contents = collect_bucket_contents(self.bucket_name)
        self.assertSetEqual(set(bucket_contents), set(["one\ntwo\n", "three\nfour\n"]))
