package raftstore

import (
	"bytes"
	"encoding/hex"

	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/fagongzi/util/format"
)

func formatRequest(req *raftcmdpb.Request) string {
	var buf bytes.Buffer
	buf.WriteString("request<id:")
	buf.WriteString(hex.EncodeToString(req.ID))
	buf.WriteString(",type:")
	buf.WriteString(req.Type.String())
	buf.WriteString(",key:")
	buf.WriteString(string(req.Key))
	buf.WriteString(",custom:")
	buf.Write(format.UInt64ToString(req.CustemType))
	buf.WriteString(">")
	return buf.String()
}
