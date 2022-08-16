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

func OffsetLeaderEpoch(addr net.Addr, impl KafsarServer, reqList []*codec.OffsetLeaderEpochTopicReq) ([]*codec.OffsetForLeaderEpochTopicResp, error) {
	respList := make([]*codec.OffsetForLeaderEpochTopicResp, len(reqList))
	for i, topicReq := range reqList {
		f := &codec.OffsetForLeaderEpochTopicResp{}
		f.Topic = topicReq.Topic
		f.PartitionRespList = make([]*codec.OffsetForLeaderEpochPartitionResp, 0)
		for _, partitionReq := range topicReq.PartitionReqList {
			partition, err := impl.OffsetLeaderEpoch(addr, topicReq.Topic, partitionReq)
			if err != nil {
				return nil, err
			}
			if partition != nil {
				f.PartitionRespList = append(f.PartitionRespList, partition)
			}
		}
		respList[i] = f
	}
	return respList, nil
}
