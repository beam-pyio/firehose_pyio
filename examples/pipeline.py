import argparse
import datetime
import random
import string
import logging

import apache_beam as beam
from apache_beam.transforms.util import BatchElements
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from firehose_pyio.io import WriteToFirehose

COMMON_NAME = "firehose-pyio-test"


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
    pass


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    _, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    print(f"pipeline options - {pipeline_options.display_data()}")

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "CreateElements" >> beam.Create([])
            | "ConvertTS" >> beam.Map(convert_ts)
            | "BatchElements" >> BatchElements(min_batch_size=20)
            | "WriteToFirehose"
            >> WriteToFirehose(
                delivery_stream_name=COMMON_NAME,
                jsonify=True,
                # multiline=True,
                max_retry=3,
            )
        )

        logging.getLogger().setLevel(logging.WARN)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
