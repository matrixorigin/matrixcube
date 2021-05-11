package cluster

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	_ "github.com/matrixorigin/matrixcube/components/prophet/schedulers"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/stretchr/testify/assert"
)

func TestReportSplit(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory))

	left := &metadata.TestResource{ResID: 1, Start: []byte("a"), End: []byte("b")}
	right := &metadata.TestResource{ResID: 2, Start: []byte("b"), End: []byte("c")}
	request := &rpcpb.Request{}
	request.ReportSplit.Left, _ = left.Marshal()
	request.ReportSplit.Right, _ = right.Marshal()
	_, err = cluster.HandleReportSplit(request)
	assert.NoError(t, err)

	request.ReportSplit.Left, _ = right.Marshal()
	request.ReportSplit.Right, _ = left.Marshal()
	_, err = cluster.HandleReportSplit(request)
	assert.Error(t, err)
}

func TestReportBatchSplit(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory))

	resources := []*metadata.TestResource{
		{ResID: 1, Start: []byte(""), End: []byte("a")},
		{ResID: 2, Start: []byte("a"), End: []byte("b")},
		{ResID: 3, Start: []byte("b"), End: []byte("c")},
		{ResID: 3, Start: []byte("c"), End: []byte("")},
	}

	request := &rpcpb.Request{}
	for _, res := range resources {
		v, _ := res.Marshal()
		request.BatchReportSplit.Resources = append(request.BatchReportSplit.Resources, v)
	}

	_, err = cluster.HandleBatchReportSplit(request)
	assert.NoError(t, err)
}
