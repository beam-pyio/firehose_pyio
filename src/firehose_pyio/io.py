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
import apache_beam as beam
from apache_beam import metrics
from apache_beam.pvalue import PCollection


from firehose_pyio.boto3_client import FirehoseClient, FakeFirehoseClient
from firehose_pyio.options import FirehoseOptions

__all__ = ["WriteToFirehose"]


class _FirehoseWriteFn(beam.DoFn):
    """Create the connector can put records in batch to an Amazon Firehose delivery stream.

    Args:
        delivery_stream_name (str): Amazon Firehose delivery stream name.
        jsonify (bool): Whether to convert records into JSON. Defaults to False.
        multiline (bool): Whether to add a new line at the end of each record.
        max_retry (int): Maximum number of retry to put failed records.
        options (Union[FirehoseOptions, dict]): Options to create a boto3 Firehose client.
        fake_config (dict, optional): Config parameters when using FakeFirehoseClient for testing. Defaults to None.
    """

    total_elements_count = metrics.Metrics.counter(
        "_FirehoseWriteFn", "total_elements_count"
    )
    succeeded_elements_count = metrics.Metrics.counter(
        "_FirehoseWriteFn", "succeeded_elements_count"
    )
    failed_elements_count = metrics.Metrics.counter(
        "_FirehoseWriteFn", "failed_elements_count"
    )

    def __init__(
        self,
        delivery_stream_name: str,
        jsonify: bool,
        multiline: bool,
        max_retry: int,
        options: typing.Union[FirehoseOptions, dict],
        fake_config: dict,
    ):
        """Constructor of _FirehoseWriteFn

        Args:
            delivery_stream_name (str): Amazon Firehose delivery stream name.
            jsonify (bool): Whether to convert records into JSON. Defaults to False.
            multiline (bool): Whether to add a new line at the end of each record.
            max_retry (int): Maximum number of retry to put failed records.
            options (Union[FirehoseOptions, dict]): Options to create a boto3 Firehose client.
            fake_config (dict, optional): Config parameters when using FakeFirehoseClient for testing.
        """
        super().__init__()
        self.delivery_stream_name = delivery_stream_name
        self.jsonify = jsonify
        self.multiline = multiline
        self.max_retry = max_retry
        self.options = options
        self.fake_config = fake_config

    def start_bundle(self):
        if not self.fake_config:
            self.client = FirehoseClient(self.options)
            assert (
                self.client.is_delivery_stream_active(self.delivery_stream_name) is True
            )
        else:
            self.client = FakeFirehoseClient(self.fake_config)

    def process(self, element):
        if isinstance(element, tuple):
            element = element[1]
        loop, total, failed = 0, len(element), []
        while loop < self.max_retry:
            responses = self.client.put_record_batch(
                element, self.delivery_stream_name, self.jsonify, self.multiline
            )["RequestResponses"]
            for index, result in enumerate(responses):
                if "RecordId" not in result:
                    failed.append(element[index])
            if len(failed) == 0 or (self.max_retry - loop == 1):
                break
            element = failed
            failed = []
            loop += 1
        self.total_elements_count.inc(total)
        self.succeeded_elements_count.inc(total - len(failed))
        self.failed_elements_count.inc(len(failed))
        return failed

    def finish_bundle(self):
        self.client.close()


class WriteToFirehose(beam.PTransform):
    """A transform that puts records into an Amazon Firehose delivery stream

    Takes an input PCollection and put them in batch using the boto3 package.
    For more information, visit the `Boto3 Documentation <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/firehose/client/put_record_batch.html>`__.

    Note that, if the PCollection element is a tuple (i.e. keyed stream), only the value is used to put records in batch.

    Args:
        delivery_stream_name (str): Amazon Firehose delivery stream name.
        jsonify (bool): Whether to convert records into JSON.
        multiline (bool): Whether to add a new line at the end of each record.
        max_retry (int): Maximum number of retry to put failed records. Defaults to 3.
        fake_config (dict, optional): Config parameters when using FakeFirehoseClient for testing. Defaults to {}.
    """

    def __init__(
        self,
        delivery_stream_name: str,
        jsonify: bool,
        multiline: bool,
        max_retry: int = 3,
        fake_config: dict = {},
    ):
        """Constructor of the transform that puts records into an Amazon Firehose delivery stream

        Args:
            delivery_stream_name (str): Amazon Firehose delivery stream name.
            jsonify (bool): Whether to convert records into JSON.
            multiline (bool): Whether to add a new line at the end of each record.
            max_retry (int): Maximum number of retry to put failed records. Defaults to 3.
            fake_config (dict, optional): Config parameters when using FakeFirehoseClient for testing. Defaults to {}.
        """
        super().__init__()
        self.delivery_stream_name = delivery_stream_name
        self.jsonify = jsonify
        self.multiline = multiline
        self.max_retry = max_retry
        self.fake_config = fake_config

    def expand(self, pcoll: PCollection):
        options = pcoll.pipeline.options.view_as(FirehoseOptions)
        return pcoll | beam.ParDo(
            _FirehoseWriteFn(
                self.delivery_stream_name,
                self.jsonify,
                self.multiline,
                self.max_retry,
                options,
                self.fake_config,
            )
        )
