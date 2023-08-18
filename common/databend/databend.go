// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package databend

import (
	"database/sql"
	"fmt"
	_ "github.com/databendcloud/databend-go"
	"k8s.io/klog"
	"net/url"
	"strings"
)

const (
	DEFAULT_TABLE = "k8s-event"
)

type DatabendService struct {
	db          *sql.DB
	table       string
	dsn         string
	clusterName string
}

type DatabendKubeEventPoint struct {
	ClusterName              string
	Namespace                string
	Kind                     string
	Name                     string
	Type                     string
	Reason                   string
	Message                  string
	EventID                  string
	FirstOccurrenceTimestamp string
	LastOccurrenceTimestamp  string
}

func (mySvc DatabendService) SaveData(sinkData []interface{}) error {

	if len(sinkData) == 0 {
		klog.Warningf("insert data is []")
		return nil
	}

	prepareStatement := fmt.Sprintf("INSERT INTO %s (cluster_name,namespace,kind,name,type,reason,message,event_id,first_occurrence_time,last_occurrence_time) VALUES(?,?,?,?,?,?,?,?,?,?)", mySvc.table)

	// Prepare statement for inserting data
	klog.Infof(prepareStatement)
	stmtIns, err := mySvc.db.Prepare(prepareStatement)
	if err != nil {
		klog.Errorf("failed to Prepare statement for inserting data ")
		klog.Errorf(err.Error())
		return err
	}

	defer stmtIns.Close()
	tx, _ := mySvc.db.Begin()
	for _, data := range sinkData {

		ked := data.(DatabendKubeEventPoint)
		if ked.ClusterName == "" {
			ked.ClusterName = mySvc.clusterName
		}
		klog.Infof("Begin Insert Databend Data ...")
		klog.Infof("Namespace: %s, Kind: %s, Name: %s, Type: %s, Reason: %s, Message: %s, EventID: %s, FirstOccurrenceTimestamp: %s, LastOccurrenceTimestamp: %s ", ked.Namespace, ked.Kind, ked.Name, ked.Type, ked.Reason, ked.Message, ked.EventID, ked.FirstOccurrenceTimestamp, ked.LastOccurrenceTimestamp)
		_, err = tx.Stmt(stmtIns).Exec(ked.ClusterName, ked.Namespace, ked.Kind, ked.Name, ked.Type, ked.Reason, ked.Message, ked.EventID, ked.FirstOccurrenceTimestamp, ked.LastOccurrenceTimestamp)
		if err != nil {
			klog.Errorf("failed to Prepare statement for inserting data ")
			return err
		}
		klog.Infof("Insert Databend Data Suc...")
	}
	tx.Commit()
	return nil
}

func (mySvc DatabendService) FlushData() error {
	return nil
}

func (mySvc DatabendService) CreateDatabase(name string) error {
	return nil
}

func (mySvc DatabendService) CloseDB() error {
	return mySvc.db.Close()
}

func NewDatabendClient(uri *url.URL) (*DatabendService, error) {
	databendSvc := &DatabendService{}
	if uri.Query().Get("table") != "" {
		databendSvc.table = uri.Query().Get("table")
		slice := strings.Split(uri.RawQuery, "&")
		databendSvc.dsn = slice[0]
	} else {
		databendSvc.table = DEFAULT_TABLE
		databendSvc.dsn = uri.RawQuery
	}
	if uri.Query().Get("cluster_name") != "" {
		databendSvc.clusterName = uri.Query().Get("cluster_name")
	}

	klog.Infof("databend jdbc url: %s", databendSvc.dsn)

	db, err := sql.Open("databend", databendSvc.dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect mysql according jdbc url string: %s", err)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)

	// Open doesn't open a connection. Validate DSN data:
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("cannot open a connection for databend according jdbc url string: %s", err)
	}

	databendSvc.db = db

	return databendSvc, nil
}
