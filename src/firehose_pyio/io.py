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

import apache_beam as beam
from firehose_pyio.boto3_client import FirehoseClient
from firehose_pyio.options import FirehoseOptions


class _FirehoseWriteFn(beam.DoFn):
    """Create the connector can put records in batch to
    an Amazon Firehose delivery stream.

    Args:
        options (FirehoseOptions | dict): Options to create a boto3 Firehose client
        delivery_stream_name (str): Amazon Firehose delivery stream name
        jsonify (bool): Whether to convert records into JSON. Defaults to False.
    """

    def __init__(
        self,
        options: FirehoseOptions | dict,
        delivery_stream_name: str,
        jsonify: bool,
    ):
        """Constructor of the sink connector of Firehose

        Args:
            options (FirehoseOptions | dict): Options to create a boto3 Firehose client
            delivery_stream_name (str): Amazon Firehose delivery stream name
            jsonify (bool): Whether to convert records into JSON. Defaults to False.
        """
        super().__init__()
        self.options = options
        self.delivery_stream_name = delivery_stream_name
        self.jsonify = jsonify

    def start_bundle(self):
        self.client = FirehoseClient(self.options)
        assert self.client.is_delivery_stream_active(self.delivery_stream_name) is True

    def process(self, element):
        if isinstance(element, tuple):
            element = element[1]
        yield self.client.put_record_batch(
            self.delivery_stream_name, element, self.jsonify
        )

    def finish_bundle(self):
        pass


class WriteToFirehose(beam.PTransform):
    """A transform that puts records into an Amazon Firehose delivery stream

    Takes an input PCollection and put them in batch using the boto3 package.
    Fore more information, visit https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/firehose/client/put_record_batch.html
    """

    def __init__(self, delivery_stream_name: str, jsonify: bool = False):
        super().__init__()
        self.delivery_stream_name = delivery_stream_name
        self.jsonify = jsonify

    def expand(self, pcoll):
        options = pcoll.pipeline.options.view_as(FirehoseOptions)
        return pcoll | beam.ParDo(
            _FirehoseWriteFn(options, self.delivery_stream_name, self.jsonify)
        )
