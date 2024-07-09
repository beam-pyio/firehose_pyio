# Changelog

<!--next-version-placeholder-->

## v0.1.0 (23/07/2024)

✨NEW

- Add a composite transform (`WriteToFirehose`) that put records into a Firehose delivery stream in batches.
- Create a dedicated pipeline option (`FirehoseOptions`) that reads AWS related values (e.g. `aws_access_key_id`) from pipeline arguments.
- Implement metric objects that record the total, succeeded and failed elements counts.
- Add unit and integration testing cases. The [moto](https://github.com/getmoto/moto) and [localstack-utils](https://docs.localstack.cloud/user-guide/tools/testing-utils/) are used for unit and integration testing respectively. Also, a test client is created for testing retry behavior, which is not supported by the moto package.
- Integrate with GitHub Actions by adding workflows for testing, documentation and release management.
