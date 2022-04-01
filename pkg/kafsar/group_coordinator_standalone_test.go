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

const (
	groupId      = "test-group-id"
	generation   = 1
	testUsername = "testGroupUser"
)

var (
	memberId     = ""
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
	resp, err := groupCoordinator.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, resp.ErrorCode)
	group, err := groupCoordinator.GetGroup(testUsername, groupId)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, CompletingRebalance, group.groupStatus)

	resp, err = groupCoordinator.HandleJoinGroup(testUsername, "test-group-id-2", memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, resp.ErrorCode)
	group, err = groupCoordinator.GetGroup(testUsername, groupId)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, CompletingRebalance, group.groupStatus)
	assert.Equal(t, 2, len(groupCoordinator.groupManager))
}

func TestMultiMembersJoinSameGroup(t *testing.T) {
	config := KafsarConfig{
		MaxConsumersPerGroup:     10,
		GroupMinSessionTimeoutMs: 0,
		GroupMaxSessionTimeoutMs: 30000,
		InitialDelayedJoinMs:     3000,
		RebalanceTickMs:          100,
	}
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, config, nil)
	resp1, err := groupCoordinator.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, resp1.ErrorCode)
	group, err := groupCoordinator.GetGroup(testUsername, groupId)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, CompletingRebalance, group.groupStatus)

	go func() {
		time.Sleep(3500 * time.Millisecond)
		heartBeatResp := groupCoordinator.HandleHeartBeat(testUsername, groupId)
		assert.Equal(t, service.REBALANCE_IN_PROGRESS, heartBeatResp.ErrorCode)
	}()
	resp2, err := groupCoordinator.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, resp2.ErrorCode)
	group, err = groupCoordinator.GetGroup(testUsername, groupId)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, CompletingRebalance, group.groupStatus)
	assert.Equal(t, 1, len(groupCoordinator.groupManager))
	assert.Equal(t, 2, len(group.members))
	time.Sleep(1 * time.Second)
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
		resp, err := groupCoordinator.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
		assert.Nil(t, err)
		assert.Equal(t, service.NONE, resp.ErrorCode)
		group, err := groupCoordinator.GetGroup(testUsername, groupId)
		assert.Nil(t, err)
		assert.Equal(t, CompletingRebalance, group.groupStatus)
	}()

	resp, err := groupCoordinator.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, resp.ErrorCode)
	group, err := groupCoordinator.GetGroup(testUsername, groupId)
	if err != nil {
		t.Fatal(err)
	}
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
	resp1, err := groupCoordinator.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, resp1.ErrorCode)
	group, err := groupCoordinator.GetGroup(testUsername, groupId)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, CompletingRebalance, group.groupStatus)
	memberId1 := resp1.MemberId
	assert.Equal(t, resp1.LeaderId, memberId1)

	go func() {
		time.Sleep(1 * time.Second)
		heartBeatResp := groupCoordinator.HandleHeartBeat(testUsername, groupId)
		assert.Equal(t, service.REBALANCE_IN_PROGRESS, heartBeatResp.ErrorCode)
	}()
	resp2, err := groupCoordinator.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, resp2.ErrorCode)
	assert.Equal(t, CompletingRebalance, group.groupStatus)
	assert.Equal(t, memberId1, resp2.LeaderId)
}

func TestHandleJoinGroupMultiMember(t *testing.T) {
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	resp, err := groupCoordinator.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, resp.ErrorCode)

	resp, err = groupCoordinator.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.UNKNOWN_SERVER_ERROR, resp.ErrorCode)
}

func TestHandleJoinGroupInvalidParams(t *testing.T) {
	// invalid groupId
	groupCoordinatorEmptyGroupId := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	groupIdEmpty := ""
	resp, err := groupCoordinatorEmptyGroupId.HandleJoinGroup(testUsername, groupIdEmpty, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.INVALID_GROUP_ID, resp.ErrorCode)

	// invalid protocol
	groupCoordinatorEmptyProtocol := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	var protocolsEmpty []*service.GroupProtocol
	resp, err = groupCoordinatorEmptyProtocol.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocolsEmpty)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.INCONSISTENT_GROUP_PROTOCOL, resp.ErrorCode)
	groupCoordinatorEmptyProtocolType := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	protocolTypeEmpty := ""
	resp, err = groupCoordinatorEmptyProtocolType.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolTypeEmpty, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.INCONSISTENT_GROUP_PROTOCOL, resp.ErrorCode)
}

func TestHandleSyncGroup(t *testing.T) {
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	joinGroupResp, err := groupCoordinator.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
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
	syncGroupResp, err := groupCoordinator.HandleSyncGroup(testUsername, groupId, memberId, generation, groupAssignments)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, syncGroupResp.ErrorCode)
	assert.Equal(t, Stable, groupCoordinator.groupManager[testUsername+groupId].groupStatus)
}

func TestHandleSyncGroupInvalidParams(t *testing.T) {
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	joinGroupResp, err := groupCoordinator.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
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
	syncGroupResp, err := groupCoordinator.HandleSyncGroup(testUsername, groupIdEmpty, memberId, generation, groupAssignments)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.INVALID_GROUP_ID, syncGroupResp.ErrorCode)

	// invalid memberId
	memberIdInvalid := "test-member-id-invalid"
	syncGroupResp, err = groupCoordinator.HandleSyncGroup(testUsername, groupId, memberIdInvalid, generation, groupAssignments)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.UNKNOWN_MEMBER_ID, syncGroupResp.ErrorCode)
}

func TestLeaveGroup(t *testing.T) {
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	resp, err := groupCoordinator.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, resp.ErrorCode)
	group := groupCoordinator.groupManager[testUsername+groupId]
	assert.Equal(t, group.leader, resp.MemberId)

	var members []*service.LeaveGroupMember
	leaveGroupMembers := append(members, &service.LeaveGroupMember{
		MemberId: resp.MemberId,
	})
	leaveGroupResp, err := groupCoordinator.HandleLeaveGroup(testUsername, groupId, leaveGroupMembers)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, leaveGroupResp.ErrorCode)
	assert.Empty(t, group.leader)
}

func TestMultiConsumerLeaveGroup(t *testing.T) {
	config := KafsarConfig{
		MaxConsumersPerGroup:     10,
		GroupMinSessionTimeoutMs: 0,
		GroupMaxSessionTimeoutMs: 30000,
		InitialDelayedJoinMs:     3000,
		RebalanceTickMs:          100,
	}
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, config, nil)
	// leader member join group
	resp1, err := groupCoordinator.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, resp1.ErrorCode)
	group := groupCoordinator.groupManager[testUsername+groupId]
	assert.Equal(t, group.leader, resp1.MemberId)

	// follower member join group
	resp2, err := groupCoordinator.HandleJoinGroup(testUsername, groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, resp2.ErrorCode)
	assert.Equal(t, group.leader, resp1.MemberId)

	// leader member leave group
	var members []*service.LeaveGroupMember
	leaveGroupMembers := append(members, &service.LeaveGroupMember{
		MemberId: resp1.MemberId,
	})
	leaveGroupResp, err := groupCoordinator.HandleLeaveGroup(testUsername, groupId, leaveGroupMembers)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, leaveGroupResp.ErrorCode)
	assert.Empty(t, group.leader)

	// follower member rejoin group
	resp2, err = groupCoordinator.HandleJoinGroup(testUsername, groupId, resp2.MemberId, clientId, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, resp2.ErrorCode)
	assert.Equal(t, resp2.MemberId, group.leader)
}

func TestHeartBeatRebalanceInProgress(t *testing.T) {
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	groupCoordinator.groupManager[testUsername+groupId] = &Group{
		groupId:     groupId,
		groupStatus: PreparingRebalance,
	}

	resp := groupCoordinator.HandleHeartBeat(testUsername, groupId)
	assert.Equal(t, resp.ErrorCode, service.REBALANCE_IN_PROGRESS)
}

func TestHeartBeatInvalidGroupId(t *testing.T) {
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	resp := groupCoordinator.HandleHeartBeat(testUsername, "")
	assert.Equal(t, resp.ErrorCode, service.INVALID_GROUP_ID)
	resp = groupCoordinator.HandleHeartBeat(testUsername, "no_group_id")
	assert.Equal(t, resp.ErrorCode, service.REBALANCE_IN_PROGRESS)
}

func TestHeartBeatNone(t *testing.T) {
	groupCoordinator := NewGroupCoordinatorStandalone(PulsarConfig{}, kafsarConfig, nil)
	groupCoordinator.groupManager[testUsername+groupId] = &Group{
		groupId:     groupId,
		groupStatus: Empty,
	}
	resp := groupCoordinator.HandleHeartBeat(testUsername, groupId)
	assert.Equal(t, resp.ErrorCode, service.NONE)
}
