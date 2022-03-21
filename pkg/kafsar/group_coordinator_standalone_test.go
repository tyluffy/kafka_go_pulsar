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
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	clientId         = "consumer-a1e12365-ddfa-43fc-826e-9661fb54c274-1"
	groupId          = "test-group-id"
	memberId         = ""
	sessionTimeoutMs = 30000
	protocolType     = "consumer"
	protocol         = service.GroupProtocol{
		ProtocolName:     "range",
		ProtocolMetadata: "000100000001000474657374ffffffff00000000",
	}
	groupProtocol []*service.GroupProtocol
	protocols     = append(groupProtocol, &protocol)
	generation    = 1

	kafsarConfig = KafsarConfig{
		MaxConsumersPerGroup:     1,
		GroupMinSessionTimeoutMs: 0,
		GroupMaxSessionTimeoutMs: 30000,
		InitialDelayedJoinMs:     3000,
		RebalanceTickMs:          100,
	}
)

func TestHandleJoinGroup(t *testing.T) {
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	resp, err := groupCoordinator.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, resp.ErrorCode)
	group, err := groupCoordinator.GetGroup(groupId)
	assert.Nil(t, err)
	assert.Equal(t, CompletingRebalance, group.groupStatus)

	resp, err = groupCoordinator.HandleJoinGroup("test-group-id-2", memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, resp.ErrorCode)
	group, err = groupCoordinator.GetGroup(groupId)
	assert.Nil(t, err)
	assert.Equal(t, CompletingRebalance, group.groupStatus)
	assert.Equal(t, 2, len(groupCoordinator.groupManager))
}

func TestGroupRebalance(t *testing.T) {
	config := KafsarConfig{
		MaxConsumersPerGroup:     10,
		GroupMinSessionTimeoutMs: 0,
		GroupMaxSessionTimeoutMs: 30000,
		InitialDelayedJoinMs:     3000,
		RebalanceTickMs:          100,
	}
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, config, nil)
	go func() {
		resp, err := groupCoordinator.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
		assert.Nil(t, err)
		assert.Equal(t, service.NONE, resp.ErrorCode)
		group, err := groupCoordinator.GetGroup(groupId)
		assert.Nil(t, err)
		assert.Equal(t, CompletingRebalance, group.groupStatus)
	}()

	resp, err := groupCoordinator.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, resp.ErrorCode)
	group, err := groupCoordinator.GetGroup(groupId)
	assert.Nil(t, err)
	assert.Equal(t, CompletingRebalance, group.groupStatus)
}

func TestNotifyReJoinGroup(t *testing.T) {
	config := KafsarConfig{
		MaxConsumersPerGroup:     10,
		GroupMinSessionTimeoutMs: 0,
		GroupMaxSessionTimeoutMs: 30000,
		InitialDelayedJoinMs:     3000,
		RebalanceTickMs:          100,
	}
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, config, nil)
	resp1, err := groupCoordinator.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, resp1.ErrorCode)
	group, err := groupCoordinator.GetGroup(groupId)
	assert.Nil(t, err)
	assert.Equal(t, CompletingRebalance, group.groupStatus)
	memberId1 := resp1.MemberId
	assert.Equal(t, resp1.LeaderId, memberId1)

	go func() {
		time.Sleep(1 * time.Second)
		heartBeatResp := groupCoordinator.HandleHeartBeat(groupId)
		assert.Equal(t, service.REBALANCE_IN_PROGRESS, heartBeatResp.ErrorCode)
	}()
	resp2, err := groupCoordinator.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, resp2.ErrorCode)
	assert.Nil(t, err)
	assert.Equal(t, CompletingRebalance, group.groupStatus)
	assert.Equal(t, memberId1, resp2.LeaderId)
}

func TestHandleJoinGroupMultiMember(t *testing.T) {
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	resp, err := groupCoordinator.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, resp.ErrorCode)

	resp, err = groupCoordinator.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.UNKNOWN_SERVER_ERROR, resp.ErrorCode)
}

func TestHandleJoinGroupInvalidParams(t *testing.T) {
	// invalid groupId
	groupCoordinatorEmptyGroupId := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	groupIdEmpty := ""
	resp, err := groupCoordinatorEmptyGroupId.HandleJoinGroup(groupIdEmpty, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.INVALID_GROUP_ID, resp.ErrorCode)

	// invalid protocol
	groupCoordinatorEmptyProtocol := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	var protocolsEmpty []*service.GroupProtocol
	resp, err = groupCoordinatorEmptyProtocol.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocolsEmpty)
	assert.Nil(t, err)
	assert.Equal(t, service.INCONSISTENT_GROUP_PROTOCOL, resp.ErrorCode)
	groupCoordinatorEmptyProtocolType := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	protocolTypeEmpty := ""
	resp, err = groupCoordinatorEmptyProtocolType.HandleJoinGroup(groupId, memberId, clientId, protocolTypeEmpty, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.INCONSISTENT_GROUP_PROTOCOL, resp.ErrorCode)
}

func TestHandleSyncGroup(t *testing.T) {
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	joinGroupResp, err := groupCoordinator.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)
	assert.Equal(t, joinGroupResp.MemberId, joinGroupResp.LeaderId)
	assert.Equal(t, 1, len(joinGroupResp.Members))

	memberId = joinGroupResp.MemberId
	assignment := service.GroupAssignment{
		MemberId:         memberId,
		MemberAssignment: "0001000000010004746573740000000100000000ffffffff",
	}
	var groupAssignment []*service.GroupAssignment
	groupAssignments := append(groupAssignment, &assignment)
	syncGroupResp, err := groupCoordinator.HandleSyncGroup(groupId, memberId, generation, groupAssignments)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, syncGroupResp.ErrorCode)
	assert.Equal(t, Stable, groupCoordinator.groupManager[groupId].groupStatus)
}

func TestHandleSyncGroupInvalidParams(t *testing.T) {
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	joinGroupResp, err := groupCoordinator.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	memberId = joinGroupResp.MemberId
	assignment := service.GroupAssignment{
		MemberId:         memberId,
		MemberAssignment: "0001000000010004746573740000000100000000ffffffff",
	}
	var groupAssignment []*service.GroupAssignment
	groupAssignments := append(groupAssignment, &assignment)
	// invalid groupId
	groupIdEmpty := ""
	syncGroupResp, err := groupCoordinator.HandleSyncGroup(groupIdEmpty, memberId, generation, groupAssignments)
	assert.Nil(t, err)
	assert.Equal(t, service.INVALID_GROUP_ID, syncGroupResp.ErrorCode)

	// invalid memberId
	memberIdInvalid := "test-member-id-invalid"
	syncGroupResp, err = groupCoordinator.HandleSyncGroup(groupId, memberIdInvalid, generation, groupAssignments)
	assert.Nil(t, err)
	assert.Equal(t, service.UNKNOWN_MEMBER_ID, syncGroupResp.ErrorCode)
}

func TestLeaveGroup(t *testing.T) {
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	resp, err := groupCoordinator.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, resp.ErrorCode)

	var members []*service.LeaveGroupMember
	leaveGroupMembers := append(members, &service.LeaveGroupMember{
		MemberId: memberId,
	})
	leaveGroupResp, err := groupCoordinator.HandleLeaveGroup(groupId, leaveGroupMembers)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, leaveGroupResp.ErrorCode)
}

func TestHeartBeatRebalanceInProgress(t *testing.T) {
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	groupCoordinator.groupManager[groupId] = &Group{
		groupId:     groupId,
		groupStatus: PreparingRebalance,
	}

	resp := groupCoordinator.HandleHeartBeat(groupId)
	assert.Equal(t, resp.ErrorCode, service.REBALANCE_IN_PROGRESS)
}

func TestHeartBeatInvalidGroupId(t *testing.T) {
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	resp := groupCoordinator.HandleHeartBeat("")
	assert.Equal(t, resp.ErrorCode, service.INVALID_GROUP_ID)
	resp = groupCoordinator.HandleHeartBeat("no_group_id")
	assert.Equal(t, resp.ErrorCode, service.INVALID_GROUP_ID)
}

func TestHeartBeatNone(t *testing.T) {
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	groupCoordinator.groupManager[groupId] = &Group{
		groupId:     groupId,
		groupStatus: Empty,
	}
	resp := groupCoordinator.HandleHeartBeat(groupId)
	assert.Equal(t, resp.ErrorCode, service.NONE)
}
