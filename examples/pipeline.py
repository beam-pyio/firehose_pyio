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

import argparse
import datetime
import random
import string
import logging
import boto3
import time

import apache_beam as beam
from apache_beam.transforms.util import BatchElements
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from firehose_pyio.io import WriteToFirehose


def get_all_contents(bucket_name):
    client = boto3.client("s3")
    bucket_objects = client.list_objects_v2(
        Bucket=f"{bucket_name}-{client._client_config.region_name}"
    )
    return bucket_objects.get("Contents") or []


def delete_all_objects(bucket_name):
    client = boto3.client("s3")
    contents = get_all_contents(bucket_name)
    for content in contents:
        client.delete_object(
            Bucket=f"{bucket_name}-{client._client_config.region_name}",
            Key=content["Key"],
        )


def print_bucket_contents(bucket_name):
    client = boto3.client("s3")
    contents = get_all_contents(bucket_name)
    for content in contents:
        resp = client.get_object(
            Bucket=f"{bucket_name}-{client._client_config.region_name}",
            Key=content["Key"],
        )
        print(f"Key - {content['Key']}")
        print(resp["Body"].read().decode())


def create_records(n=100):
    return [
        {
            "id": i,
            "name": "".join(random.choices(string.ascii_letters, k=5)).lower(),
            "created_at": datetime.datetime.now(),
        }
        for i in range(n)
    ]


def convert_ts(record: dict):
    record["created_at"] = record["created_at"].isoformat(timespec="milliseconds")
    return record


def mask_secrets(d: dict):
    return {k: (v if k.find("aws") < 0 else "x" * len(v)) for k, v in d.items()}


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--stream_name",
        default="firehose-pyio-test",
        type=str,
        help="Delivery stream name",
    )
    parser.add_argument(
        "--num_records", default="100", type=int, help="Number of records"
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    print(f"known_args - {known_args}")
    print(f"pipeline options - {mask_secrets(pipeline_options.display_data())}")

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "CreateElements" >> beam.Create(create_records(known_args.num_records))
            | "DatetimeToStr" >> beam.Map(convert_ts)
            | "BatchElements" >> BatchElements(min_batch_size=50)
            | "WriteToFirehose"
            >> WriteToFirehose(
                delivery_stream_name=known_args.stream_name,
                jsonify=True,
                multiline=True,
                max_retry=3,
            )
        )

        logging.getLogger().setLevel(logging.WARN)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    BUCKET_NAME = "firehose-pyio-test"
    print(">> delete existing objects...")
    delete_all_objects(BUCKET_NAME)
    print(">> start pipeline...")
    run()
    time.sleep(1)
    print(">> print bucket contents...")
    print_bucket_contents(BUCKET_NAME)
