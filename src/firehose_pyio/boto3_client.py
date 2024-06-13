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

import typing

from apache_beam.options import pipeline_options
from firehose_pyio.options import FirehoseOptions


try:
    import boto3

except ImportError:
    boto3 = None


def get_http_error_code(exc):
    if hasattr(exc, "response"):
        return exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return None


class Boto3ClientError(Exception):
    def __init__(self, message=None, code=None):
        self.message = message
        self.code = code


class FirehoseClient(object):
    """
    Wrapper for boto3 library
    """

    def __init__(self, options):
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

    def describe_delivery_stream(self, delivery_stream_name: str):
        try:
            boto_response = self.client.describe_delivery_stream(
                DeliveryStreamName=delivery_stream_name
            )
            return boto_response
        except Exception as e:
            raise Boto3ClientError(str(e), get_http_error_code(e))

    def put_record_batch(self, delivery_stream_name: str, bytes: typing.List[bytes]):
        try:
            boto_response = self.client.put_record_batch(
                DeliveryStreamName=delivery_stream_name,
                Records=[{"Data": byte for byte in bytes}],
            )
            return boto_response
        except Exception as e:
            raise Boto3ClientError(str(e), get_http_error_code(e))
