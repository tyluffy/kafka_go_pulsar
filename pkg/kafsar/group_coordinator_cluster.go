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

package kafsar

import (
	"github.com/paashzj/kafka_go/pkg/service"
)

type GroupCoordinatorCluster struct {
}

func NewGroupCoordinatorCluster() *GroupCoordinatorCluster {
	return &GroupCoordinatorCluster{}
}

func (gcc *GroupCoordinatorCluster) HandleJoinGroup(groupId, memberId, clientId, protocolType string, sessionTimeoutMs int,
	protocols []*service.GroupProtocol) (*service.JoinGroupResp, error) {
	panic("implement handle join group")
}

func (gcc *GroupCoordinatorCluster) HandleSyncGroup(groupId, memberId string, generation int,
	groupAssignments []*service.GroupAssignment) (*service.SyncGroupResp, error) {
	panic("implement handle sync group")
}

func (gcc *GroupCoordinatorCluster) HandleLeaveGroup(groupId string,
	members []*service.LeaveGroupMember) (*service.LeaveGroupResp, error) {
	panic("implement handle leave group")
}

func (gcc *GroupCoordinatorCluster) GetGroup(groupId string) (*Group, error) {
	panic("implement get group")
}
func (gcc *GroupCoordinatorCluster) HandleHeartBeat(groupId string) *service.HeartBeatResp {
	panic("implement handle heart beat")
}
