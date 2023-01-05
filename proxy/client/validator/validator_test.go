package validator

import (
	_ "github.com/santhosh-tekuri/jsonschema/v5/httploader"
	"github.com/stretchr/testify/suite"
	"testing"
)

type ValidatorTestSuite struct {
	suite.Suite
	validator *Validator
}

func (suite *ValidatorTestSuite) SetupSuite() {
	suite.validator = New()
}

func (suite *ValidatorTestSuite) TestValidateMinimalValidEvent() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			}
		}
	`
	err := suite.validator.Validate(event)
	suite.Nil(err)
}

func (suite *ValidatorTestSuite) TestValidateEventMissingRequired() {
	event := `
		{
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			}
		}
	`
	err := suite.validator.Validate(event)
	suite.NotNil(err)
}

func (suite *ValidatorTestSuite) TestValidateInvalidEventTimeFormat() {
	event := `
		{
			"eventTime": "06/19/1963 08:30:06 PST",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			}
		}
	`
	err := suite.validator.Validate(event)
	suite.NotNil(err)
}

func (suite *ValidatorTestSuite) TestValidateInvalidUriFormat() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://aa:bb:cc:[dd]:ee:ff/",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			}
		}
	`
	err := suite.validator.Validate(event)
	suite.NotNil(err)
}

func (suite *ValidatorTestSuite) TestValidateInvalidUUIDFormat() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "invalid-uuid-format"
			}
		}
	`
	err := suite.validator.Validate(event)
	suite.NotNil(err)
}

func (suite *ValidatorTestSuite) TestValidateRunFacet() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
				"facets": {
					"parent": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
						"run": {
							"runId": "00000000-1111-2222-3333-444444444444"
						},
						"job": {
							"namespace": "proxy-testing",
							"name": "proxy-testing-parent-job"
						}
					}
				}
			}
		}
	`
	err := suite.validator.Validate(event)
	suite.Nil(err)
}

func (suite *ValidatorTestSuite) TestValidateRunFacetWithAdditional() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
				"facets": {
					"parent": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
						"run": {
							"runId": "00000000-1111-2222-3333-444444444444"
						},
						"job": {
							"namespace": "proxy-testing",
							"name": "proxy-testing-parent-job"
						}
					},
					"unknownRunFacet": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/UnknownRunFacet.json",
						"something": "unknown"
					}
				}
			}
		}
	`
	err := suite.validator.Validate(event)
	suite.Nil(err)
}

func (suite *ValidatorTestSuite) TestValidateRunFacetWithAdditionalMissingRequired() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
				"facets": {
					"parent": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
						"run": {
							"runId": "00000000-1111-2222-3333-444444444444"
						},
						"job": {
							"namespace": "proxy-testing",
							"name": "proxy-testing-parent-job"
						}
					},
					"unknownRunFacet": {
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/UnknownRunFacet.json",
						"something": "unknown"
					}
				}
			}
		}
	`
	err := suite.validator.Validate(event)
	suite.NotNil(err)
}

func (suite *ValidatorTestSuite) TestValidateRunFacetMissingRequired() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
				"facets": {
					"parent": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
						"job": {
							"namespace": "proxy-testing",
							"name": "proxy-testing-parent-job"
						}
					}
				}
			}
		}
	`
	err := suite.validator.Validate(event)
	suite.NotNil(err)
}

func (suite *ValidatorTestSuite) TestValidateRunFacetInvalidFormat() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
				"facets": {
					"parent": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
						"run": {
							"runId": "invalid-parent-run-uuid"
						},
						"job": {
							"namespace": "proxy-testing",
							"name": "proxy-testing-parent-job"
						}
					}
				}
			}
		}
	`
	err := suite.validator.Validate(event)
	suite.NotNil(err)
}

func (suite *ValidatorTestSuite) TestValidateJobFacet() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job",
				"facets": {
					"sql": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json",
						"query": "SELECT 1"
					}
				}
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			}
		}
	`
	err := suite.validator.Validate(event)
	suite.Nil(err)
}

func (suite *ValidatorTestSuite) TestValidateJobFacetWithAdditional() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job",
				"facets": {
					"sql": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json",
						"query": "SELECT 1"
					},
					"unknownJobFacet": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/UnknownJobFacet.json",
						"something": "unknown"
					}
				}
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			}
		}
	`
	err := suite.validator.Validate(event)
	suite.Nil(err)
}

func (suite *ValidatorTestSuite) TestValidateJobFacetWithAdditionalMissingRequired() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job",
				"facets": {
					"sql": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json",
						"query": "SELECT 1"
					},
					"unknownJobFacet": {
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/UnknownJobFacet.json",
						"something": "unknown"
					}
				}
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			}
		}
	`
	err := suite.validator.Validate(event)
	suite.NotNil(err)
}

func (suite *ValidatorTestSuite) TestValidateJobFacetMissingRequired() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job",
				"facets": {
					"sql": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json"
					}
				}
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			}
		}
	`
	err := suite.validator.Validate(event)
	suite.NotNil(err)
}

func (suite *ValidatorTestSuite) TestValidateJobFacetInvalidFormat() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job",
				"facets": {
					"sql": {
						"_producer": "https://aa:bb:cc:[dd]:ee:ff/",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json",
						"query": "SELECT 1"
					}
				}
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			}
		}
	`
	err := suite.validator.Validate(event)
	suite.NotNil(err)
}

func (suite *ValidatorTestSuite) TestValidateDataset() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			},
			"inputs": [{
				"namespace": "proxy-testing",
				"name": "input-data"
			}],
			"outputs": [{
				"namespace": "proxy-testing",
				"name": "output-data"
			}]
		}
	`
	err := suite.validator.Validate(event)
	suite.Nil(err)
}

func (suite *ValidatorTestSuite) TestValidateDatasetMissingRequired() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			},
			"inputs": [{
				"name": "input-data"
			}]
		}
	`
	err := suite.validator.Validate(event)
	suite.NotNil(err)
}

func (suite *ValidatorTestSuite) TestValidateInputDatasetFacet() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			},
			"inputs": [{
				"namespace": "proxy-testing",
				"name": "input-data",
				"inputFacets": {
					"dataQualityMetrics": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsInputDatasetFacet.json",
						"rowCount": 10,
						"bytes": 100,
						"columnMetrics": {
							"col": {
								"nullCount": 0,
								"distinctCount": 10
							}
						}
					}
				}
			}]
		}
	`
	err := suite.validator.Validate(event)
	suite.Nil(err)
}

func (suite *ValidatorTestSuite) TestValidateInputDatasetFacetMissingRequired() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			},
			"inputs": [{
				"namespace": "proxy-testing",
				"name": "input-data",
				"inputFacets": {
					"dataQualityMetrics": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsInputDatasetFacet.json",
						"rowCount": 10,
						"bytes": 100
					}
				}
			}]
		}
	`
	err := suite.validator.Validate(event)
	suite.NotNil(err)
}

func (suite *ValidatorTestSuite) TestValidateOutputDatasetFacet() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			},
			"outputs": [{
				"namespace": "proxy-testing",
				"name": "output-data",
				"outputFacets": {
					"outputStatistics": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/OutputStatisticsOutputDatasetFacet.json",
						"rowCount": 10,
						"size": 100
					}
				}
			}]
		}
	`
	err := suite.validator.Validate(event)
	suite.Nil(err)
}

func (suite *ValidatorTestSuite) TestValidateOutputDatasetFacetMissingRequired() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			},
			"outputs": [{
				"namespace": "proxy-testing",
				"name": "output-data",
				"outputFacets": {
					"outputStatistics": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/OutputStatisticsOutputDatasetFacet.json",
						"size": 100
					}
				}
			}]
		}
	`
	err := suite.validator.Validate(event)
	suite.NotNil(err)
}

func (suite *ValidatorTestSuite) TestValidateDatasetFacet() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			},
			"inputs": [{
				"namespace": "proxy-testing",
				"name": "input-data",
				"facets": {
					"dataSource": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
						"name": "input-data-name",
						"uri": "file:///dataset/input.parquet"
					}
				}
			}]
		}
	`
	err := suite.validator.Validate(event)
	suite.Nil(err)
}

func (suite *ValidatorTestSuite) TestValidateDatasetFacetInvalidFormat() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			},
			"inputs": [{
				"namespace": "proxy-testing",
				"name": "input-data",
				"facets": {
					"dataSource": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
						"name": "input-data-name",
						"uri": "https://aa:bb:cc:[dd]:ee:ff/"
					}
				}
			}]
		}
	`
	err := suite.validator.Validate(event)
	suite.NotNil(err)
}

func (suite *ValidatorTestSuite) TestValidateDatasetFacetMissingRequired() {
	event := `
		{
			"eventTime": "2022-12-22T00:00:00.000Z",
			"producer": "https://openlineage.io/proxy/client/testing",
			"schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
			"job": {
				"namespace": "proxy-testing",
				"name": "proxy-testing-job"
			},
			"run": {
				"runId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			},
			"inputs": [{
				"namespace": "proxy-testing",
				"name": "input-data",
				"facets": {
					"dataSource": {
						"_producer": "https://openlineage.io/proxy/client/testing",
						"name": "input-data-name",
						"uri": "file:///dataset/input.parquet"
					}
				}
			}]
		}
	`
	err := suite.validator.Validate(event)
	suite.NotNil(err)
}

func TestValidatorTestSuite(t *testing.T) {
	suite.Run(t, new(ValidatorTestSuite))
}
