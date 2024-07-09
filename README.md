# firehose_pyio

![doc](https://github.com/beam-pyio/firehose_pyio/workflows/doc/badge.svg)
![test](https://github.com/beam-pyio/firehose_pyio/workflows/test/badge.svg)
[![release](https://img.shields.io/github/release/beam-pyio/firehose_pyio.svg)](https://github.com/beam-pyio/firehose_pyio/releases)
![python](https://img.shields.io/badge/python-3.8%2C%203.9%2C%203.10%2C%203.11%2C%203.12-blue)
![os](https://img.shields.io/badge/OS-Ubuntu%2C%20Mac%2C%20Windows-purple)

[Amazon Data Firehose](https://aws.amazon.com/firehose/) is a fully managed service for delivering real-time streaming data to destinations such as Amazon Simple Storage Service (Amazon S3), Amazon Redshift, Amazon OpenSearch Service and Amazon OpenSearch Serverless. The Apache Beam Python I/O connector for Amazon Data Firehose (`firehose_pyio`) provides a data sink feature that facilitates integration with those services.

## Installation

```bash
$ pip install firehose_pyio
```

## Usage

The connector has the main composite transform ([`WriteToFirehose`](https://beam-pyio.github.io/firehose_pyio/autoapi/firehose_pyio/io/index.html#firehose_pyio.io.WriteToFirehose)), and it expects a list or tuple _PCollection_ element. If the element is a tuple, the tuple's first element is taken. If the element is not of the accepted types, you can apply the [`GroupIntoBatches`](https://beam.apache.org/documentation/transforms/python/aggregation/groupintobatches/) or [`BatchElements`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements) transform beforehand. Then, the element is sent into a Firehose delivery stream using the [`put_record_batch`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/firehose/client/put_record_batch.html) method of the boto3 package. Note that the above batch preprocessing can also be useful to overcome the API limitation listed below.

- Each `PutRecordBatch` request supports up to 500 records. Each record in the request can be as large as 1,000 KB (before base64 encoding), up to a limit of 4 MB for the entire request. These limits cannot be changed.

The transform also has options that control individual records or handling failed records.

- _jsonify_ - A flag that indicates whether to convert a record into Json. Note that a record should be of _bytes_, _bytearray_ or file-like object, and, if it is not of a supported type (e.g. integer), we can convert it into a Json string by specifying this flag to _True_.
- _multiline_ - A flag that indicates whether to add a new line character (`\n`) to each record. It is useful to save records into a _CSV_ or _Jsonline_ file.
- _max_retry_ - The maximum number of retries when there is one or more failed records. Defaults to 3.

### Examples

If a _PCollection_ element is key-value pair (i.e. keyed stream), it can be batched in group using the `GroupIntoBatches` transform before it is connected into the main transform.

```python
import apache_beam as beam
from apache_beam import GroupIntoBatches
from firehose_pyio.io import WriteToFirehose

with beam.Pipeline(options=pipeline_options) as p:
    (
        p
        | beam.Create([(1, "one"), (2, "three"), (1, "two"), (2, "four")])
        | GroupIntoBatches(batch_size=2)
        | WriteToFirehose(
            delivery_stream_name=delivery_stream_name,
            jsonify=True,
            multiline=True,
            max_retry=3
        )
    )
```

For a list element (i.e. unkeyed stream), we can apply the `BatchElements` transform instead.

```python
import apache_beam as beam
from apache_beam.transforms.util import BatchElements
from firehose_pyio.io import WriteToFirehose

with beam.Pipeline(options=pipeline_options) as p:
    (
        p
        | beam.Create(["one", "two", "three", "four"])
        | BatchElements(min_batch_size=2, max_batch_size=2)
        | WriteToFirehose(
            delivery_stream_name=delivery_stream_name,
            jsonify=True,
            multiline=True,
            max_retry=3
        )
    )
```

See [this post](https://beam-pyio.github.io//blog/2024/firehose-pyio-intro/) for more examples.

## Contributing

Interested in contributing? Check out the contributing guidelines. Please note that this project is released with a Code of Conduct. By contributing to this project, you agree to abide by its terms.

## License

`firehose_pyio` was created by Beam PyIO <beam.pyio@gmail.com>. It is licensed under the terms of the Apache License 2.0 license.

## Credits

`firehose_pyio` was created with [`cookiecutter`](https://cookiecutter.readthedocs.io/en/latest/) and the `pyio-cookiecutter` [template](https://github.com/beam-pyio/pyio-cookiecutter).
