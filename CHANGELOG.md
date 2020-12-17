# Changelog


## 0.3.9 - 2020-12-16

### Fixed

* Fix task completion time to use airflow task end time instead of dag end time [@henneberger](https://github.com/henneberger)

## [0.3.8](https://github.com/MarquezProject/marquez-airflow/compare/0.3.7...0.3.8) - 2020-12-08

### Added

* Add schemas resolution to big query output datasets [@henneberger](https://github.com/henneberger)

### Deprecated

* Deprecate support for airflow 1.10.3

### Fixed

* Fix `bigquery.Client.close()`
* Fix thrown exception on `Utils.get_location()`
* Fix bug where all runs fail if DAG fails [@henneberger](https://github.com/henneberger)
* Fix bug where big query operator creates duplicate job versions [@henneberger](https://github.com/henneberger)
* Fix extract not reporting task state on callback & update api flow [@henneberger](https://github.com/henneberger)

## [0.3.7](https://github.com/MarquezProject/marquez-airflow/compare/0.3.6...0.3.7) - 2020-11-09

### Added

* Add support to get postgres table schemas
* Extract inputs / outputs from BigQuery jobs object (please see [Job](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job) docs) [@julienledem](https://github.com/julienledem)

### Changed

* Bump `marquez-python` to [`0.7.11`](https://github.com/MarquezProject/marquez-python/releases/tag/0.7.11)

### Fixed

* Assure extractor associates only the output dataset on complete [@henneberger](https://github.com/henneberger)