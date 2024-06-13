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


try:
    from firehose_pyio.boto3_client import FirehoseClient

    BOTO3_INSTALLED = True
except ImportError:
    BOTO3_INSTALLED = False


class _FirehoseWriteFn(beam.DoFn):
    def __init__(self, delivery_stream_name, client):
        super().__init__()

    def start_bundle(self):
        return super().start_bundle()

    def process(self, records):
        pass

    def finish_bundle(self):
        return super().finish_bundle()


class WriteToFirehose(beam.PTransform):
    def __init__(self, delivery_stream_name, client=None, options=None) -> None:
        super().__init__()
        if client is None and options is None:
            raise ValueError("Must provide one of client or options")
        if client is not None:
            self.client = client
        elif BOTO3_INSTALLED:
            self.client = FirehoseClient(options=options)
        else:
            message = (
                "AWS dependencies are not installed, and no alternative "
                "client was provided to Firehose PyIO."
            )
            raise RuntimeError(message)
        self.delivery_stream_name = delivery_stream_name

    def expand(self, input):
        return input | beam.ParDo(
            _FirehoseWriteFn(self.delivery_stream_name, self.client)
        )
