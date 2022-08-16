// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package service

import (
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"net"
)

func Offset(addr net.Addr, clientID string, impl KafsarServer, reqList []*codec.ListOffsetsTopic) ([]*codec.ListOffsetsTopicResp, error) {
	result := make([]*codec.ListOffsetsTopicResp, len(reqList))
	var err error
	for i, req := range reqList {
		f := &codec.ListOffsetsTopicResp{}
		f.Topic = req.Topic
		f.PartitionRespList = make([]*codec.ListOffsetsPartitionResp, len(req.PartitionReqList))
		for j, partitionReq := range req.PartitionReqList {
			f.PartitionRespList[j], err = impl.OffsetListPartition(addr, f.Topic, clientID, partitionReq)
			if err != nil {
				return nil, err
			}
		}
		result[i] = f
	}
	return result, nil
}
