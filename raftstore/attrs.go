package raftstore

const (
	// AttrBuf internal buf attr
	AttrBuf = "internal.temp.buf"
	// AttrWriteRequestApplyMax max index of write requests in current batch apply
	AttrWriteRequestApplyMax = "internal.write.request.apply.total"
	// AttrWriteRequestApplyCurrent current index of write requests in current batch apply
	AttrWriteRequestApplyCurrent = "internal.write.request.apply.current"
)
