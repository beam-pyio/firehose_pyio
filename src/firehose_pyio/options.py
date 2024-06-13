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

from apache_beam.options.pipeline_options import PipelineOptions


class FirehoseOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # These options are passed to the Firehose PyIO Client
        parser.add_argument(
            "--aws_access_key_id",
            default=None,
            help="The secret key to use when creating the boto3 client.",
        )
        parser.add_argument(
            "--aws_secret_access_key",
            default=None,
            help="The secret key to use when creating the boto3 client.",
        )
        parser.add_argument(
            "--aws_session_token",
            default=None,
            help="The session token to use when creating the boto3 client.",
        )
        parser.add_argument(
            "--endpoint_url",
            default=None,
            help="The complete URL to use for the constructed boto3 client.",
        )
        parser.add_argument(
            "--region_name",
            default=None,
            help="The name of the region associated with the boto3 client.",
        )
        parser.add_argument(
            "--api_version",
            default=None,
            help="The API version to use with the boto3 client.",
        )
        parser.add_argument(
            "--verify",
            default=None,
            help="Whether or not to verify SSL certificates with the boto3 client.",
        )
        parser.add_argument(
            "--disable_ssl",
            default=False,
            action="store_true",
            help=(
                "Whether or not to use SSL with the boto3 client. "
                "By default, SSL is used."
            ),
        )
