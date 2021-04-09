package cluster

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/deepfabric/prophet/core"
	"github.com/deepfabric/prophet/metadata"
	"github.com/deepfabric/prophet/pb/metapb"
	"github.com/deepfabric/prophet/pb/rpcpb"
	"github.com/deepfabric/prophet/schedule"
	"github.com/deepfabric/prophet/util"
)

// HandleResourceHeartbeat processes CachedResource reports from client.
func (c *RaftCluster) HandleResourceHeartbeat(res *core.CachedResource) error {
	if err := c.processResourceHeartbeat(res); err != nil {
		return err
	}

	c.RLock()
	co := c.coordinator
	c.RUnlock()
	co.opController.Dispatch(res, schedule.DispatchFromHeartBeat)
	return nil
}

// HandleAskSplit handles the split request.
func (c *RaftCluster) HandleAskSplit(request *rpcpb.Request) (*rpcpb.AskSplitRsp, error) {
	reqResource := c.adapter.NewResource()
	err := reqResource.Unmarshal(request.AskSplit.Data)
	if err != nil {
		return nil, err
	}

	err = c.ValidRequestResource(reqResource)
	if err != nil {
		return nil, err
	}

	newResourceID, err := c.AllocID()
	if err != nil {
		return nil, err
	}

	peerIDs := make([]uint64, len(reqResource.Peers()))
	for i := 0; i < len(peerIDs); i++ {
		if peerIDs[i], err = c.AllocID(); err != nil {
			return nil, err
		}
	}

	// Disable merge for the 2 resources in a period of time.
	c.GetMergeChecker().RecordResourceSplit([]uint64{reqResource.ID(), newResourceID})

	split := &rpcpb.AskSplitRsp{}
	split.SplitID.NewID = newResourceID
	split.SplitID.NewPeerIDs = peerIDs

	util.GetLogger().Infof("alloc ids %+v for resource %d split",
		newResourceID,
		peerIDs)

	return split, nil
}

// ValidRequestResource is used to decide if the resource is valid.
func (c *RaftCluster) ValidRequestResource(reqResource metadata.Resource) error {
	startKey, _ := reqResource.Range()
	res := c.GetResourceByKey(startKey)
	if res == nil {
		return fmt.Errorf("resource not found, request resource: %v", reqResource)
	}
	// If the request epoch is less than current resource epoch, then returns an error.
	reqResourceEpoch := reqResource.Epoch()
	resourceEpoch := res.Meta.Epoch()
	if reqResourceEpoch.GetVersion() < resourceEpoch.GetVersion() ||
		reqResourceEpoch.GetConfVer() < resourceEpoch.GetConfVer() {
		return fmt.Errorf("invalid resource epoch, request: %v, current: %v", reqResourceEpoch, resourceEpoch)
	}
	return nil
}

// HandleAskBatchSplit handles the batch split request.
func (c *RaftCluster) HandleAskBatchSplit(request *rpcpb.Request) (*rpcpb.AskBatchSplitRsp, error) {
	reqResource := c.adapter.NewResource()
	err := reqResource.Unmarshal(request.AskBatchSplit.Data)
	if err != nil {
		return nil, err
	}

	splitCount := request.AskBatchSplit.Count
	err = c.ValidRequestResource(reqResource)
	if err != nil {
		return nil, err
	}
	splitIDs := make([]rpcpb.SplitID, 0, splitCount)
	recordResources := make([]uint64, 0, splitCount+1)

	for i := 0; i < int(splitCount); i++ {
		newResourceID, err := c.AllocID()
		if err != nil {
			return nil, err
		}

		peerIDs := make([]uint64, len(reqResource.Peers()))
		for i := 0; i < len(peerIDs); i++ {
			if peerIDs[i], err = c.AllocID(); err != nil {
				return nil, err
			}
		}

		recordResources = append(recordResources, newResourceID)
		splitIDs = append(splitIDs, rpcpb.SplitID{
			NewID:      newResourceID,
			NewPeerIDs: peerIDs,
		})

		util.GetLogger().Infof("alloc ids %+v for resource %d split",
			peerIDs,
			newResourceID)
	}

	recordResources = append(recordResources, reqResource.ID())
	// Disable merge the resources in a period of time.
	c.GetMergeChecker().RecordResourceSplit(recordResources)

	// If resource splits during the scheduling process, resources with abnormal
	// status may be left, and these resources need to be checked with higher
	// priority.
	c.AddSuspectResources(recordResources...)

	return &rpcpb.AskBatchSplitRsp{SplitIDs: splitIDs}, nil
}

func (c *RaftCluster) checkSplitResource(left metadata.Resource, right metadata.Resource) error {
	if left == nil || right == nil {
		return errors.New("invalid split resource")
	}

	leftStart, leftEnd := left.Range()
	rightStart, rightEnd := right.Range()

	if !bytes.Equal(leftEnd, rightStart) {
		return errors.New("invalid split resource with leftEnd != rightStart")
	}

	if len(rightEnd) == 0 || bytes.Compare(leftStart, rightEnd) < 0 {
		return nil
	}

	return errors.New("invalid split resource")
}

func (c *RaftCluster) checkSplitResources(resources []metadata.Resource) error {
	if len(resources) <= 1 {
		return errors.New("invalid split resource")
	}

	for i := 1; i < len(resources); i++ {
		left := resources[i-1]
		right := resources[i]

		leftStart, leftEnd := left.Range()
		rightStart, rightEnd := right.Range()

		if !bytes.Equal(leftEnd, rightStart) {
			return errors.New("invalid split resource")
		}
		if len(rightEnd) != 0 && bytes.Compare(leftStart, rightEnd) >= 0 {
			return errors.New("invalid split resource")
		}
	}
	return nil
}

// HandleReportSplit handles the report split request.
func (c *RaftCluster) HandleReportSplit(request *rpcpb.Request) (*rpcpb.ReportSplitRsp, error) {
	left := c.adapter.NewResource()
	err := left.Unmarshal(request.ReportSplit.Left)
	if err != nil {
		return nil, err
	}

	right := c.adapter.NewResource()
	err = right.Unmarshal(request.ReportSplit.Right)
	if err != nil {
		return nil, err
	}

	err = c.checkSplitResource(left, right)
	if err != nil {
		util.GetLogger().Warningf("report split resource is invalid, left %+v, right %+v, error %+v",
			left,
			right,
			err)
		return nil, err
	}

	// Build origin resource by using left and right.
	origin := right.Clone()
	origin.SetEpoch(metapb.ResourceEpoch{})
	start, _ := left.Range()
	origin.SetStartKey(start)
	util.GetLogger().Infof("resource %d split, generate new resource, meta %+v",
		origin.ID(),
		left)
	return &rpcpb.ReportSplitRsp{}, nil
}

// HandleBatchReportSplit handles the batch report split request.
func (c *RaftCluster) HandleBatchReportSplit(request *rpcpb.Request) (*rpcpb.BatchReportSplitRsp, error) {
	var resources []metadata.Resource
	for _, data := range request.BatchReportSplit.Resources {
		res := c.adapter.NewResource()
		err := res.Unmarshal(data)
		if err != nil {
			return nil, err
		}
		resources = append(resources, res)
	}

	hrm := core.ResourcesToHexMeta(resources)
	err := c.checkSplitResources(resources)
	if err != nil {
		util.GetLogger().Warningf("report batch split resource %s is invalid, error %+v",
			hrm.String(),
			err)
		return nil, err
	}
	last := len(resources) - 1
	origin := resources[last].Clone()
	hrm = core.ResourcesToHexMeta(resources[:last])
	util.GetLogger().Infof("resource %d batch split, generate new resources, origion %s, total %d",
		origin.ID(),
		hrm.String(),
		last)
	return &rpcpb.BatchReportSplitRsp{}, nil
}
