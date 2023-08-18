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
	"encoding/json"
	databendcommon "github.com/AliyunContainerService/kube-eventer/common/databend"
	"github.com/AliyunContainerService/kube-eventer/core"
	kubeapi "k8s.io/api/core/v1"
	"k8s.io/klog"
	"net/url"
	"sync"
)

// SaveDataFunc is a pluggable function to enforce limits on the object
type SaveDataFunc func(sinkData []interface{}) error

type databendSink struct {
	databendSvc *databendcommon.DatabendService
	saveData    SaveDataFunc
	flushData   func() error
	closeDB     func() error
	sync.RWMutex
	uri *url.URL
}

const (
	// Maximum number of databend Points to be sent in one batch.
	maxSendBatchSize = 1
)

func (sink *databendSink) createDatabase() error {

	if sink.databendSvc == nil {
		databendSvc, err := databendcommon.NewDatabendClient(sink.uri)
		if err != nil {
			return err
		}
		sink.databendSvc = databendSvc
	}

	return nil
}

// Generate point value for event
func getEventValue(event *kubeapi.Event) (string, error) {
	// TODO: check whether indenting is required.
	bytes, err := json.MarshalIndent(event, "", " ")
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func eventToPoint(event *kubeapi.Event) (*databendcommon.DatabendKubeEventPoint, error) {

	value, err := getEventValue(event)
	if err != nil {
		return nil, err
	}
	klog.Infof(value)

	point := databendcommon.DatabendKubeEventPoint{
		ClusterName:              event.ClusterName,
		Name:                     event.InvolvedObject.Name,
		Namespace:                event.InvolvedObject.Namespace,
		EventID:                  string(event.UID),
		Type:                     event.Type,
		Reason:                   event.Reason,
		Message:                  event.Message,
		Kind:                     event.InvolvedObject.Kind,
		FirstOccurrenceTimestamp: event.FirstTimestamp.Time.String(),
		LastOccurrenceTimestamp:  event.LastTimestamp.Time.String(),
	}

	return &point, nil

}

func (sink *databendSink) ExportEvents(eventBatch *core.EventBatch) {

	sink.Lock()
	defer sink.Unlock()

	dataPoints := make([]databendcommon.DatabendKubeEventPoint, 0, 10)
	for _, event := range eventBatch.Events {

		point, err := eventToPoint(event)
		if err != nil {
			klog.Warningf("Failed to convert event to point: %v", err)
			klog.Warningf("Skip this event")
			continue
		}

		dataPoints = append(dataPoints, *point)
		if len(dataPoints) >= maxSendBatchSize {
			err = sink.saveData([]interface{}{*point})
			if err != nil {
				klog.Warningf("Failed to export data to Databend sink: %v", err)
			}
			dataPoints = make([]databendcommon.DatabendKubeEventPoint, 0, 1)
		}

	}

}

func (sink *databendSink) Name() string {
	return "databend Sink"
}

func (sink *databendSink) Stop() {
	defer sink.closeDB()
}

// Returns a thread-safe implementation of core.EventSink for InfluxDB.
func CreateDatabendSink(uri *url.URL) (core.EventSink, error) {

	var mySink databendSink

	databendSvc, err := databendcommon.NewDatabendClient(uri)
	if err != nil {
		return nil, err
	}

	mySink.databendSvc = databendSvc
	mySink.saveData = func(sinkData []interface{}) error {
		return databendSvc.SaveData(sinkData)
	}
	mySink.flushData = func() error {
		return databendSvc.FlushData()
	}
	mySink.closeDB = func() error {
		return databendSvc.CloseDB()
	}
	mySink.uri = uri

	klog.V(3).Info("Databend Sink setup successfully")
	return &mySink, nil
}
