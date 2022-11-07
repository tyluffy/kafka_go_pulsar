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
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type GroupCoordinatorStandalone struct {
	pulsarConfig PulsarConfig
	kafsarConfig KafsarConfig
	pulsarClient pulsar.Client
	mutex        sync.RWMutex
	groupManager map[string]*Group
}

func NewGroupCoordinatorStandalone(pulsarConfig PulsarConfig, kafsarConfig KafsarConfig, pulsarClient pulsar.Client) *GroupCoordinatorStandalone {
	coordinatorImpl := GroupCoordinatorStandalone{pulsarConfig: pulsarConfig, kafsarConfig: kafsarConfig, pulsarClient: pulsarClient}
	coordinatorImpl.groupManager = make(map[string]*Group)
	return &coordinatorImpl
}

func (g *GroupCoordinatorStandalone) HandleJoinGroup(username, groupId, memberId, clientId, protocolType string, sessionTimeoutMs int,
	protocols []*codec.GroupProtocol) (*codec.JoinGroupResp, error) {
	// do parameters check
	memberId, code, err := g.joinGroupParamsCheck(clientId, groupId, memberId, sessionTimeoutMs, g.kafsarConfig)
	if err != nil {
		logrus.Errorf("join group %s params check failed, cause: %s", groupId, err)
		return &codec.JoinGroupResp{
			MemberId:  memberId,
			ErrorCode: code,
		}, nil
	}

	g.mutex.Lock()
	group, exist := g.groupManager[username+groupId]
	if !exist {
		group = &Group{
			groupId:          groupId,
			groupStatus:      Empty,
			protocolType:     protocolType,
			members:          make(map[string]*memberMetadata),
			canRebalance:     true,
			sessionTimeoutMs: sessionTimeoutMs,
			partitionedTopic: make([]string, 0),
		}
		g.groupManager[username+groupId] = group
	}
	g.mutex.Unlock()

	code, err = g.joinGroupProtocolCheck(group, protocolType, protocols, g.kafsarConfig)
	if err != nil {
		logrus.Errorf("join group %s protocol check failed, cause: %s", groupId, err)
		return &codec.JoinGroupResp{
			MemberId:  memberId,
			ErrorCode: code,
		}, nil
	}

	numMember := g.getGroupMembersLen(group)
	if g.kafsarConfig.MaxConsumersPerGroup > 0 && numMember >= g.kafsarConfig.MaxConsumersPerGroup {
		logrus.Errorf("join group failed, exceed maximum number of members. groupId: %s, memberId: %s, current: %d, maxConsumersPerGroup: %d",
			groupId, memberId, numMember, g.kafsarConfig.MaxConsumersPerGroup)
		return &codec.JoinGroupResp{
			MemberId:  memberId,
			ErrorCode: codec.GROUP_MAX_SIZE_REACHED,
		}, nil
	}

	if g.getGroupStatus(group) == Dead {
		logrus.Errorf("join group failed, cause group status is dead. groupId: %s, memberId: %s", groupId, memberId)
		return &codec.JoinGroupResp{
			MemberId:  memberId,
			ErrorCode: codec.UNKNOWN_MEMBER_ID,
		}, nil
	}
	isNewMember := memberId == EmptyMemberId
	if g.getGroupStatus(group) == PreparingRebalance {
		if isNewMember || !g.checkMemberExist(group, memberId) {
			memberId, err = g.addNewMemberAndReBalance(group, clientId, memberId, protocolType, protocols)
			if err != nil {
				logrus.Errorf("member %s join group %s failed, cause: %s", memberId, groupId, err)
				return &codec.JoinGroupResp{
					MemberId:  memberId,
					ErrorCode: codec.COORDINATOR_LOAD_IN_PROGRESS,
				}, nil
			}
		}
		err := g.awaitingJoin(group, memberId, g.kafsarConfig.RebalanceTickMs, sessionTimeoutMs)
		if err != nil {
			logrus.Errorf("member %s join group %s failed, case: %s", memberId, groupId, err)
			if isNewMember {
				g.deleteMember(group, memberId)
			}
			return &codec.JoinGroupResp{
				MemberId:  memberId,
				ErrorCode: codec.COORDINATOR_LOAD_IN_PROGRESS,
			}, nil
		}
		members := g.getLeaderMembers(group, memberId)
		return &codec.JoinGroupResp{
			ErrorCode:    codec.NONE,
			GenerationId: group.generationId,
			ProtocolType: &group.protocolType,
			ProtocolName: group.supportedProtocol,
			LeaderId:     g.getMemberLeader(group),
			MemberId:     memberId,
			Members:      members,
		}, nil
	}

	if g.getGroupStatus(group) == CompletingRebalance {
		if isNewMember || !g.checkMemberExist(group, memberId) {
			memberId, err = g.addNewMemberAndReBalance(group, clientId, memberId, protocolType, protocols)
			if err != nil {
				logrus.Errorf("member %s join group %s failed, cause: %s", memberId, groupId, err)
				return &codec.JoinGroupResp{
					MemberId:  memberId,
					ErrorCode: codec.COORDINATOR_LOAD_IN_PROGRESS,
				}, nil
			}
		} else {
			if !matchProtocols(group.groupProtocols, protocols) {
				// member is joining with the different metadata
				err := g.updateMemberAndRebalance(group, clientId, memberId, protocolType, protocols, g.kafsarConfig.InitialDelayedJoinMs)
				if err != nil {
					logrus.Errorf("member %s join group %s failed, cause: %s", memberId, groupId, err)
					return &codec.JoinGroupResp{
						MemberId:  memberId,
						ErrorCode: codec.COORDINATOR_LOAD_IN_PROGRESS,
					}, nil
				}
			}
		}
		members := g.getLeaderMembers(group, memberId)
		err := g.awaitingJoin(group, memberId, g.kafsarConfig.RebalanceTickMs, sessionTimeoutMs)
		if err != nil {
			logrus.Errorf("member %s join group %s failed, case: %s", memberId, groupId, err)
			if isNewMember {
				g.deleteMember(group, memberId)
			}
			return &codec.JoinGroupResp{
				MemberId:  memberId,
				ErrorCode: codec.COORDINATOR_LOAD_IN_PROGRESS,
			}, nil
		}
		return &codec.JoinGroupResp{
			ErrorCode:    codec.NONE,
			GenerationId: group.generationId,
			ProtocolType: &group.protocolType,
			ProtocolName: group.supportedProtocol,
			LeaderId:     g.getMemberLeader(group),
			MemberId:     memberId,
			Members:      members,
		}, nil
	}

	if g.getGroupStatus(group) == Empty || g.getGroupStatus(group) == Stable {
		if isNewMember || !g.checkMemberExist(group, memberId) {
			// avoid multi new member join an empty group
			memberId, err = g.addNewMemberAndReBalance(group, clientId, memberId, protocolType, protocols)
			if err != nil {
				logrus.Errorf("member %s join group %s failed, cause: %s", memberId, groupId, err)
				return &codec.JoinGroupResp{
					MemberId:  memberId,
					ErrorCode: codec.COORDINATOR_LOAD_IN_PROGRESS,
				}, nil
			}
		} else {
			if g.isMemberLeader(group, memberId) || !matchProtocols(group.groupProtocols, protocols) {
				err := g.updateMemberAndRebalance(group, clientId, memberId, protocolType, protocols, g.kafsarConfig.InitialDelayedJoinMs)
				if err != nil {
					logrus.Errorf("member %s join group %s failed, cause: %s", memberId, groupId, err)
					return &codec.JoinGroupResp{
						MemberId:  memberId,
						ErrorCode: codec.COORDINATOR_LOAD_IN_PROGRESS,
					}, nil
				}
			}
		}
		err := g.awaitingJoin(group, memberId, g.kafsarConfig.RebalanceTickMs, sessionTimeoutMs)
		if err != nil {
			logrus.Errorf("member %s join group %s failed, case: %s", memberId, groupId, err)
			if isNewMember {
				g.deleteMember(group, memberId)
			}
			return &codec.JoinGroupResp{
				MemberId:  memberId,
				ErrorCode: codec.COORDINATOR_LOAD_IN_PROGRESS,
			}, nil
		}
		members := g.getLeaderMembers(group, memberId)
		var generationId int
		group.groupLock.Lock()
		generationId = group.generationId
		group.groupLock.Unlock()
		return &codec.JoinGroupResp{
			ErrorCode:    codec.NONE,
			GenerationId: generationId,
			ProtocolType: &group.protocolType,
			ProtocolName: group.supportedProtocol,
			LeaderId:     g.getMemberLeader(group),
			MemberId:     memberId,
			Members:      members,
		}, nil
	}
	return &codec.JoinGroupResp{
		MemberId:  memberId,
		ErrorCode: codec.UNKNOWN_SERVER_ERROR,
	}, nil
}

func (g *GroupCoordinatorStandalone) HandleSyncGroup(username, groupId, memberId string, generation int,
	groupAssignments []*codec.GroupAssignment) (*codec.SyncGroupResp, error) {
	code, err := g.syncGroupParamsCheck(groupId, memberId)
	if err != nil {
		logrus.Errorf("member %s snyc group %s failed, cause: %s", memberId, groupId, err)
		return &codec.SyncGroupResp{ErrorCode: code}, nil
	}
	g.mutex.RLock()
	group, exist := g.groupManager[username+groupId]
	g.mutex.RUnlock()
	if !exist {
		logrus.Errorf("sync group %s failed, cause invalid groupId", groupId)
		return &codec.SyncGroupResp{
			ErrorCode: codec.INVALID_GROUP_ID,
		}, nil
	}
	curMember, exist := group.members[memberId]
	if !exist {
		logrus.Errorf("sync group %s failed, cause invalid memberId %s", groupId, memberId)
		return &codec.SyncGroupResp{
			ErrorCode: codec.UNKNOWN_MEMBER_ID,
		}, nil
	}

	// TODO generation check
	if g.getGroupStatus(group) == Empty || g.getGroupStatus(group) == Dead {
		return &codec.SyncGroupResp{
			ErrorCode: codec.UNKNOWN_MEMBER_ID,
		}, nil
	}

	// maybe new member add, need to rebalance again
	if g.getGroupStatus(group) == PreparingRebalance {
		return &codec.SyncGroupResp{
			ErrorCode: codec.REBALANCE_IN_PROGRESS,
		}, nil
	}

	if g.getGroupStatus(group) == CompletingRebalance {
		// get assignment from leader member
		if g.isMemberLeader(group, memberId) {
			for i := range groupAssignments {
				logrus.Infof("Assignment %#+v received from leader %s for group %s for generation %d", groupAssignments[i], memberId, groupId, generation)
				group.members[groupAssignments[i].MemberId].assignment = groupAssignments[i].MemberAssignment
			}
		}
		group.groupMemberLock.Lock()
		curMember.syncGenerationId = curMember.joinGenerationId
		group.groupMemberLock.Unlock()
		err := g.awaitingSync(group, g.kafsarConfig.RebalanceTickMs, group.sessionTimeoutMs, memberId)
		if g.isMemberLeader(group, memberId) {
			g.setGroupStatus(group, Stable)
		}
		group.groupMemberLock.RLock()
		curMemberAssignment := curMember.assignment
		group.groupMemberLock.RUnlock()
		if err != nil {
			logrus.Errorf("member %s sync group %s failed, cause: %s", memberId, groupId, err)
			return &codec.SyncGroupResp{
				ErrorCode:        codec.REBALANCE_IN_PROGRESS,
				MemberAssignment: curMemberAssignment,
			}, nil
		}
		if g.isMemberLeader(group, memberId) {
			g.setGroupStatus(group, Stable)
		}

		return &codec.SyncGroupResp{
			ErrorCode:        codec.NONE,
			MemberAssignment: curMemberAssignment,
		}, nil
	}

	// if the group is stable, we just return the current assignment
	if g.getGroupStatus(group) == Stable {
		return &codec.SyncGroupResp{
			ErrorCode:        codec.NONE,
			MemberAssignment: curMember.assignment,
		}, nil
	}
	return &codec.SyncGroupResp{
		ErrorCode: codec.UNKNOWN_SERVER_ERROR,
	}, nil
}

func (g *GroupCoordinatorStandalone) HandleLeaveGroup(username, groupId string,
	members []*codec.LeaveGroupMember) (*codec.LeaveGroupResp, error) {
	// reject if groupId is empty
	if groupId == "" {
		logrus.Errorf("leave group failed, cause groupId is empty")
		return &codec.LeaveGroupResp{
			ErrorCode: codec.INVALID_GROUP_ID,
		}, nil
	}
	g.mutex.RLock()
	group, exist := g.groupManager[username+groupId]
	g.mutex.RUnlock()
	if !exist {
		logrus.Errorf("leave group failed, cause group not exist")
		return &codec.LeaveGroupResp{
			ErrorCode: codec.INVALID_GROUP_ID,
		}, nil
	}
	for i := range members {
		if members[i].MemberId == g.getMemberLeader(group) {
			g.setMemberLeader(group, "")
		}
		g.deleteMember(group, members[i].MemberId)
		logrus.Infof("reader member: %s success leave group: %s", members[i].MemberId, groupId)
	}
	group.groupLock.Lock()
	group.generationId++
	group.groupLock.Unlock()
	if len(group.members) == 0 {
		g.setGroupStatus(group, Empty)
	} else {
		// any member leave group should do rebalance
		g.setGroupStatus(group, PreparingRebalance)
	}
	return &codec.LeaveGroupResp{ErrorCode: codec.NONE, Members: members}, nil
}

func (g *GroupCoordinatorStandalone) GetGroup(username, groupId string) (*Group, error) {
	g.mutex.RLock()
	group, exist := g.groupManager[username+groupId]
	g.mutex.RUnlock()
	if !exist {
		return nil, errors.New("invalid groupId")
	}
	return group, nil
}

func (g *GroupCoordinatorStandalone) addMemberAndRebalance(group *Group, clientId, memberId, protocolType string, protocols []*codec.GroupProtocol, rebalanceDelayMs int) (string, error) {
	if memberId == EmptyMemberId {
		memberId = clientId + "-" + uuid.New().String()
	}
	protocolMap := make(map[string][]byte)
	for i := range protocols {
		protocolMap[protocols[i].ProtocolName] = protocols[i].ProtocolMetadata
	}
	if g.getGroupStatus(group) == Empty {
		g.vote(group, protocols)
	}
	group.groupMemberLock.Lock()
	group.members[memberId] = &memberMetadata{
		clientId:     clientId,
		memberId:     memberId,
		metadata:     protocolMap[group.supportedProtocol],
		protocolType: protocolType,
		protocols:    protocolMap,
	}
	group.groupMemberLock.Unlock()
	return memberId, g.doRebalance(group, rebalanceDelayMs)
}

func (g *GroupCoordinatorStandalone) updateMemberAndRebalance(group *Group, clientId, memberId, protocolType string, protocols []*codec.GroupProtocol, rebalanceDelayMs int) error {
	return g.doRebalance(group, rebalanceDelayMs)
}

func (g *GroupCoordinatorStandalone) HandleHeartBeat(username, groupId, memberId string) *codec.HeartbeatResp {
	if groupId == "" {
		logrus.Errorf("groupId is empty.")
		return &codec.HeartbeatResp{
			ErrorCode: codec.INVALID_GROUP_ID,
		}
	}
	g.mutex.RLock()
	group, exist := g.groupManager[username+groupId]
	if !exist {
		g.mutex.RUnlock()
		// the group will not exist when the broker restart, rebalance is required
		logrus.Warningf("get group failed. cause group not exist, groupId: %s", groupId)
		return &codec.HeartbeatResp{
			ErrorCode: codec.REBALANCE_IN_PROGRESS,
		}
	}
	group.groupMemberLock.RLock()
	_, memberExist := group.members[memberId]
	group.groupMemberLock.RUnlock()
	if !memberExist {
		g.mutex.RUnlock()
		logrus.Warningf("get member failed. cause member not exist, groupId: %s, memberId: %s", groupId, memberId)
		return &codec.HeartbeatResp{
			ErrorCode: codec.REBALANCE_IN_PROGRESS,
		}
	}
	g.mutex.RUnlock()
	if g.getGroupStatus(group) == PreparingRebalance || g.getGroupStatus(group) == CompletingRebalance || g.getGroupStatus(group) == Dead {
		logrus.Infof("preparing rebalance. groupId: %s", groupId)
		return &codec.HeartbeatResp{
			ErrorCode: codec.REBALANCE_IN_PROGRESS,
		}
	}
	return &codec.HeartbeatResp{ErrorCode: codec.NONE}
}

func (g *GroupCoordinatorStandalone) prepareRebalance(group *Group) {
	g.setGroupStatus(group, PreparingRebalance)
}

func (g *GroupCoordinatorStandalone) doRebalance(group *Group, rebalanceDelayMs int) error {
	group.groupLock.Lock()
	g.prepareRebalance(group)
	if group.canRebalance {
		group.canRebalance = false
		logrus.Infof("preparing to rebalance group %s with old generation %d", group.groupId, group.generationId)
		time.Sleep(time.Duration(rebalanceDelayMs) * time.Millisecond)
		g.setGroupStatus(group, CompletingRebalance)
		group.generationId++
		logrus.Infof("completing rebalance group %s with new generation %d", group.groupId, group.generationId)
		group.canRebalance = true
		group.groupLock.Unlock()
		return nil
	} else {
		group.groupLock.Unlock()
		return g.awaitingRebalance(group, g.kafsarConfig.RebalanceTickMs, group.sessionTimeoutMs, CompletingRebalance)
	}
}

func (g *GroupCoordinatorStandalone) vote(group *Group, protocols []*codec.GroupProtocol) {
	// TODO make clear multiple protocol scene
	group.groupLock.Lock()
	group.supportedProtocol = protocols[0].ProtocolName
	group.groupLock.Unlock()
}

func (g *GroupCoordinatorStandalone) awaitingRebalance(group *Group, rebalanceTickMs int, sessionTimeout int, waitForStatus GroupStatus) error {
	start := time.Now()
	for {
		if g.getGroupStatus(group) == waitForStatus || g.getGroupMembersLen(group) == 0 {
			return nil
		}
		if time.Since(start).Milliseconds() >= int64(sessionTimeout) {
			return errors.Errorf("relalance timeout")
		}
		time.Sleep(time.Duration(rebalanceTickMs) * time.Millisecond)
	}
}

func (g *GroupCoordinatorStandalone) getGroupStatus(group *Group) GroupStatus {
	group.groupStatusLock.RLock()
	status := group.groupStatus
	group.groupStatusLock.RUnlock()
	return status
}

func (g *GroupCoordinatorStandalone) getGroupGenerationId(group *Group) int {
	group.groupLock.RLock()
	groupGenerationId := group.generationId
	group.groupLock.RUnlock()
	return groupGenerationId
}

func (g *GroupCoordinatorStandalone) getGroupMembersLen(group *Group) int {
	group.groupMemberLock.RLock()
	groupMembersLen := len(group.members)
	group.groupMemberLock.RUnlock()
	return groupMembersLen
}

func (g *GroupCoordinatorStandalone) setGroupStatus(group *Group, status GroupStatus) {
	group.groupStatusLock.Lock()
	group.groupStatus = status
	group.groupStatusLock.Unlock()
}

func (g *GroupCoordinatorStandalone) syncGroupParamsCheck(groupId, memberId string) (codec.ErrorCode, error) {
	// reject if groupId is empty
	if groupId == "" {
		return codec.INVALID_GROUP_ID, errors.Errorf("groupId is empty")
	}
	// reject if memberId is empty
	if memberId == "" {
		return codec.MEMBER_ID_REQUIRED, errors.Errorf("memberId is empty")
	}
	return codec.NONE, nil
}

func (g *GroupCoordinatorStandalone) joinGroupParamsCheck(clientId, groupId, memberId string, sessionTimeoutMs int, config KafsarConfig) (string, codec.ErrorCode, error) {
	// reject if groupId is empty
	if groupId == "" {
		return memberId, codec.INVALID_GROUP_ID, errors.Errorf("empty groupId")
	}

	// reject if sessionTimeoutMs is invalid
	if sessionTimeoutMs < config.GroupMinSessionTimeoutMs || sessionTimeoutMs > config.GroupMaxSessionTimeoutMs {
		return memberId, codec.INVALID_SESSION_TIMEOUT, errors.Errorf("invalid sessionTimeoutMs: %d. minSessionTimeoutMs: %d, maxSessionTimeoutMs: %d",
			sessionTimeoutMs, config.GroupMinSessionTimeoutMs, config.GroupMaxSessionTimeoutMs)
	}
	return memberId, codec.NONE, nil
}

func (g *GroupCoordinatorStandalone) joinGroupProtocolCheck(group *Group, protocolType string, protocols []*codec.GroupProtocol, config KafsarConfig) (codec.ErrorCode, error) {
	// if the new member does not support the group protocol, reject it
	if g.getGroupStatus(group) != Empty {
		if group.protocolType != protocolType {
			return codec.INCONSISTENT_GROUP_PROTOCOL, errors.Errorf("invalid protocolType: %s, and this group protocolType is %s", protocolType, group.protocolType)
		}
		if !supportsProtocols(group.groupProtocols, protocols) {
			return codec.INCONSISTENT_GROUP_PROTOCOL, errors.Errorf("protocols not match")
		}
	}

	// reject if first member with empty group protocol or protocolType is empty
	if g.getGroupStatus(group) == Empty {
		if protocolType == "" {
			return codec.INCONSISTENT_GROUP_PROTOCOL, errors.Errorf("empty protocolType")
		}
		if len(protocols) == 0 {
			return codec.INCONSISTENT_GROUP_PROTOCOL, errors.Errorf("empty protocol")
		}
	}
	return codec.NONE, nil
}

func supportsProtocols(groupProtocols map[string]string, memberProtocols []*codec.GroupProtocol) bool {
	// TODO groupProtocols must be contains memberProtocols
	return true
}

func matchProtocols(groupProtocols map[string]string, memberProtocols []*codec.GroupProtocol) bool {
	return true
}

func (g *GroupCoordinatorStandalone) isMemberLeader(group *Group, memberId string) bool {
	return g.getMemberLeader(group) == memberId
}

func (g *GroupCoordinatorStandalone) getMemberLeader(group *Group) string {
	group.groupMemberLock.RLock()
	leader := group.leader
	group.groupMemberLock.RUnlock()
	return leader
}

func (g *GroupCoordinatorStandalone) setMemberLeader(group *Group, leader string) {
	group.groupMemberLock.Lock()
	group.leader = leader
	group.groupMemberLock.Unlock()
}

func (g *GroupCoordinatorStandalone) deleteMember(group *Group, memberId string) {
	group.groupMemberLock.Lock()
	delete(group.members, memberId)
	group.groupMemberLock.Unlock()
}

func (g *GroupCoordinatorStandalone) getLeaderMembers(group *Group, memberId string) (members []*codec.Member) {
	if g.getMemberLeader(group) == "" {
		g.setMemberLeader(group, memberId)
	}
	if g.isMemberLeader(group, memberId) {
		for _, member := range group.members {
			members = append(members, &codec.Member{MemberId: member.memberId, GroupInstanceId: nil, Metadata: member.metadata})
		}
	}
	return members
}

func (g *GroupCoordinatorStandalone) checkMemberExist(group *Group, memberId string) bool {
	_, exist := group.members[memberId]
	return exist
}

func (g *GroupCoordinatorStandalone) awaitingJoin(group *Group, memberId string, rebalanceTickMs int, sessionTimeout int) error {
	start := time.Now()
	for {
		groupGenerationId := g.getGroupGenerationId(group)
		curMember := group.members[memberId]
		if curMember == nil {
			return errors.Errorf("cur member missing when awaitingJoin.")
		}
		group.groupMemberLock.Lock()
		if curMember.joinGenerationId != groupGenerationId {
			curMember.joinGenerationId = groupGenerationId
		}
		group.groupMemberLock.Unlock()
		if g.checkJoinMemberGenerationId(group, memberId) {
			g.setGroupStatus(group, CompletingRebalance)
			return nil
		}
		if time.Since(start).Milliseconds() >= int64(sessionTimeout) {
			return errors.Errorf("join wait timeout")
		}
		time.Sleep(time.Duration(rebalanceTickMs) * time.Millisecond)
	}
}

func (g *GroupCoordinatorStandalone) checkJoinMemberGenerationId(group *Group, memberId string) bool {
	group.groupMemberLock.RLock()
	for _, member := range group.members {
		if member.joinGenerationId != g.getGroupGenerationId(group) {
			group.groupMemberLock.RUnlock()
			logrus.Debugf("wait for other member join. curMemberId = %s", memberId)
			return false
		}
	}
	group.groupMemberLock.RUnlock()
	return true
}

func (g *GroupCoordinatorStandalone) awaitingSync(group *Group, rebalanceTickMs int, sessionTimeout int, memberId string) error {
	start := time.Now()
	for {
		if g.checkSyncMemberGenerationId(group, memberId) {
			return nil
		}
		if time.Since(start).Milliseconds() >= int64(sessionTimeout) {
			return errors.Errorf("sync wait timeout")
		}
		time.Sleep(time.Duration(rebalanceTickMs) * time.Millisecond)
	}
}

func (g *GroupCoordinatorStandalone) checkSyncMemberGenerationId(group *Group, memberId string) bool {
	group.groupMemberLock.RLock()
	for _, member := range group.members {
		if member.syncGenerationId != member.joinGenerationId {
			group.groupMemberLock.RUnlock()
			logrus.Debugf("wait for other member sync. curMemberId = %s", memberId)
			return false
		}
	}
	group.groupMemberLock.RUnlock()
	return true
}

func (g *GroupCoordinatorStandalone) addNewMemberAndReBalance(group *Group, clientId, memberId, protocolType string, protocols []*codec.GroupProtocol) (string, error) {
	group.groupNewMemberLock.Lock()
	if g.getGroupMembersLen(group) > 0 && g.getGroupStatus(group) != Stable {
		logrus.Warnf("new member wait for stable. Current group status is CompletingRebalance.")
		err := g.awaitingRebalance(group, g.kafsarConfig.RebalanceTickMs, sessionTimeoutMs, Stable)
		// avoid new member joined before sync-consumer leaving the sync loop
		time.Sleep((time.Duration(g.kafsarConfig.RebalanceTickMs) + 100) * time.Millisecond)
		if err != nil {
			group.groupNewMemberLock.Unlock()
			logrus.Errorf("new member join group %s failed. Current group status is %d, cause: %s, tickMs: %d, timeout: %d",
				group.groupId, group.groupStatus, err, g.kafsarConfig.RebalanceTickMs, sessionTimeoutMs)
			return memberId, err
		}
	}
	memberId, err := g.addMemberAndRebalance(group, clientId, memberId, protocolType, protocols, g.kafsarConfig.InitialDelayedJoinMs)
	group.groupNewMemberLock.Unlock()
	return memberId, err
}
