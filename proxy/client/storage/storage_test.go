package storage

import (
	"fmt"
	"github.com/OpenLineage/OpenLineage/client-proxy/config"
	"github.com/stretchr/testify/suite"
	"log"
	"os"
	"path"
	"strings"
	"testing"
)

type StorageTestSuite struct {
	suite.Suite
	db   *Storage
	conf config.Config
}

func (suite *StorageTestSuite) BeforeTest(suiteName, testName string) {
	var err error
	tempDb := fmt.Sprintf("%s-%s.db", suiteName, testName)
	suite.conf = config.Config{
		SqliteDatabasePath:      path.Join(os.TempDir(), tempDb),
		DatabaseMigrationSource: "./migrations",
	}

	err = Migrate(suite.conf)
	if err != nil {
		suite.Fail("Fail to initialize db")
	}

	suite.db, err = New(suite.conf)
	if err != nil {
		suite.Fail("Fail to initialize db")
	}

	log.Printf("Initialized %s", suite.conf.SqliteDatabasePath)
}

func (suite *StorageTestSuite) AfterTest(_, _ string) {
	_ = os.Remove(suite.conf.SqliteDatabasePath)
}

func (suite *StorageTestSuite) TestGetCurrentPartition() {
	p, err := suite.db.GetCurrentPartition()
	suite.Nil(err)

	suite.True(strings.HasPrefix(p.Name, "lineage_events_partition_"))
	suite.Equal(uint64(0), p.Size)
	suite.True(p.IsCurrent)
}

func (suite *StorageTestSuite) TestRotatePartition() {
	currentPartition, err := suite.db.GetCurrentPartition()
	suite.Nil(err)

	newPartition, err := suite.db.RotatePartition()
	suite.Nil(err)

	p, err := suite.db.GetCurrentPartition()
	suite.Nil(err)

	suite.Equal(p, newPartition)
	suite.NotEqual(p, currentPartition)
}

func (suite *StorageTestSuite) TestInsertLineageEvent() {
	p, err := suite.db.GetCurrentPartition()
	suite.Nil(err)

	err = suite.db.InsertLineageEvent(p, "{}")
	suite.Nil(err)

	var rowCount int
	err = suite.db.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) AS count FROM %s", p.Name)).Scan(&rowCount)
	suite.Nil(err)
	suite.Equal(rowCount, 1)
}

func TestStorageTestSuite(t *testing.T) {
	suite.Run(t, new(StorageTestSuite))
}
