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

import json
import typing
import boto3
from apache_beam.options import pipeline_options

from firehose_pyio.options import FirehoseOptions

__all__ = ["FirehoseClient", "FirehoseClientError"]


def get_http_error_code(exc):
    if hasattr(exc, "response"):
        return exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return None


class FirehoseClientError(Exception):
    def __init__(self, message=None, code=None):
        self.message = message
        self.code = code


class FirehoseClient(object):
    """
    Wrapper for boto3 library.
    """

    def __init__(self, options: typing.Union[FirehoseOptions, dict]):
        """Constructor of the FirehoseClient.

        Args:
            options (Union[FirehoseOptions, dict]): Options to create a boto3 Firehose client.
        """
        assert boto3 is not None, "Missing boto3 requirement"
        if isinstance(options, pipeline_options.PipelineOptions):
            options = options.view_as(FirehoseOptions)
            access_key_id = options.aws_access_key_id
            secret_access_key = options.aws_secret_access_key
            session_token = options.aws_session_token
            endpoint_url = options.endpoint_url
            use_ssl = not options.disable_ssl
            region_name = options.region_name
            api_version = options.api_version
            verify = options.verify
        else:
            access_key_id = options.get("aws_access_key_id")
            secret_access_key = options.get("aws_secret_access_key")
            session_token = options.get("aws_session_token")
            endpoint_url = options.get("endpoint_url")
            use_ssl = not options.get("disable_ssl", False)
            region_name = options.get("region_name")
            api_version = options.get("api_version")
            verify = options.get("verify")

        session = boto3.session.Session()
        self.client = session.client(
            service_name="firehose",
            region_name=region_name,
            api_version=api_version,
            use_ssl=use_ssl,
            verify=verify,
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            aws_session_token=session_token,
        )

    def is_delivery_stream_active(self, delivery_stream_name: str):
        """Check if an Amazon Firehose delivery stream is active.

        Args:
            delivery_stream_name (str): Amazon Firehose delivery stream name.

        Raises:
            FirehoseClientError: Firehose client error.

        Returns:
            (bool): Whether or not the given Firehose delivery stream is active.
        """
        try:
            boto_response = self.client.describe_delivery_stream(
                DeliveryStreamName=delivery_stream_name
            )
            return (
                boto_response["DeliveryStreamDescription"]["DeliveryStreamStatus"]
                == "ACTIVE"
            )
        except Exception as e:
            raise FirehoseClientError(str(e), get_http_error_code(e))

    def put_record_batch(
        self,
        records: list,
        delivery_stream_name: str,
        jsonify: bool,
        multiline: bool,
    ):
        """Put records to an Amazon Firehose delivery stream in batch.

        Args:
            records (list): Records to put into a Firehose delivery stream.
            delivery_stream_name (str): Amazon Firehose delivery stream name.
            jsonify (bool): Whether to convert records into JSON.
            multiline (bool): Whether to add a new line at the end of each record.

        Raises:
            FirehoseClientError: Firehose client error.

        Returns:
            (Object): Boto3 response message.
        """

        def process_data(record: str, jsonify: bool, multiline: bool):
            if jsonify:
                record = json.dumps(record)
            if multiline:
                record = f"{record}\n"
            return record

        if not isinstance(records, list):
            raise TypeError("Records should be a list.")
        try:
            boto_response = self.client.put_record_batch(
                DeliveryStreamName=delivery_stream_name,
                Records=[
                    {
                        "Data": process_data(
                            record=r, jsonify=jsonify, multiline=multiline
                        )
                    }
                    for r in records
                ],
            )
            return boto_response
        except Exception as e:
            raise FirehoseClientError(str(e), get_http_error_code(e))


class FakeFirehoseClient:
    def __init__(self, fake_config: dict):
        self.num_keep = fake_config.get("num_keep", 0)

    def put_record_batch(
        self, records: list, delivery_stream_name: str, jsonify: bool, multiline: bool
    ):
        if not isinstance(records, list):
            raise TypeError("Records should be a list.")
        request_responses = []
        for index, _ in enumerate(records):
            if index < self.num_keep:
                request_responses.append({"RecordId": index})
            else:
                request_responses.append(
                    {"ErrorCode": "Error", "ErrorMessage": "This error"}
                )
        return {
            "FailedPutCount": ["RecordId" in r for r in request_responses],
            "Encrypted": False,
            "RequestResponses": request_responses,
        }
