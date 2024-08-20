# Changelog

<!--next-version-placeholder-->

## v0.2.1 (22/08/2024)

♻️UPDATE

- Return failed elements by a tagged output, which allows users to determine how to handle them subsequently.
- Provide options that handle failed records.
  - _max_trials_ - The maximum number of trials when there is one or more failed records.
  - _append_error_ - Whether to append error details to failed records.

## v0.1.0 (23/07/2024)

✨NEW

- Add a composite transform (`WriteToFirehose`) that puts records into a Firehose delivery stream in batch, using the [`put_record_batch`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/firehose/client/put_record_batch.html) method of the boto3 package.
- Provide options that control individual records.
  - _jsonify_ - A flag that indicates whether to convert a record into JSON. Note that a record should be of _bytes_, _bytearray_ or file-like object, and, if it is not of a supported type (e.g. integer), we can convert it into a Json string by specifying this flag to _True_.
  - _multiline_ - A flag that indicates whether to add a new line character (`\n`) to each record. It is useful to save records into a _CSV_ or _Jsonline_ file.
- Create a dedicated pipeline option (`FirehoseOptions`) that reads AWS related values (e.g. `aws_access_key_id`) from pipeline arguments.
- Implement metric objects that record the total, succeeded and failed elements counts.
- Add unit and integration testing cases. The [moto](https://github.com/getmoto/moto) and [localstack-utils](https://docs.localstack.cloud/user-guide/tools/testing-utils/) packages are used for unit and integration testing respectively. Also, a custom test client is created for testing retry of failed elements, which is not supported by the moto package.
- Integrate with GitHub Actions by adding workflows for testing, documentation and release management.
