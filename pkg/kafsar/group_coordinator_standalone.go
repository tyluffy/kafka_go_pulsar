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
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/pkg/errors"
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
	protocols []*service.GroupProtocol) (*service.JoinGroupResp, error) {
	// do parameters check
	memberId, code, err := g.joinGroupParamsCheck(clientId, groupId, memberId, sessionTimeoutMs, g.kafsarConfig)
	if err != nil {
		logrus.Errorf("join group %s params check failed, cause: %s", groupId, err)
		return &service.JoinGroupResp{
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
		return &service.JoinGroupResp{
			MemberId:  memberId,
			ErrorCode: code,
		}, nil
	}

	group.groupLock.RLock()
	numMember := len(group.members)
	group.groupLock.RUnlock()
	if g.kafsarConfig.MaxConsumersPerGroup > 0 && numMember >= g.kafsarConfig.MaxConsumersPerGroup {
		logrus.Errorf("join group failed, exceed maximum number of members. groupId: %s, memberId: %s, current: %d, maxConsumersPerGroup: %d",
			groupId, memberId, numMember, g.kafsarConfig.MaxConsumersPerGroup)
		return &service.JoinGroupResp{
			MemberId:  memberId,
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}

	if g.getGroupStatus(group) == Dead {
		logrus.Errorf("join group failed, cause group status is dead. groupId: %s, memberId: %s", groupId, memberId)
		return &service.JoinGroupResp{
			MemberId:  memberId,
			ErrorCode: service.UNKNOWN_MEMBER_ID,
		}, nil
	}

	if g.getGroupStatus(group) == PreparingRebalance {
		if memberId == EmptyMemberId || !g.checkMemberExist(group, memberId) {
			memberId, err = g.addMemberAndRebalance(group, clientId, memberId, protocolType, protocols, g.kafsarConfig.InitialDelayedJoinMs)
			if err != nil {
				logrus.Errorf("member %s join group %s failed, cause: %s", memberId, groupId, err)
				return &service.JoinGroupResp{
					MemberId:  memberId,
					ErrorCode: service.REBALANCE_IN_PROGRESS,
				}, nil
			}
		} else {
			err := g.updateMemberAndRebalance(group, clientId, memberId, protocolType, protocols, g.kafsarConfig.InitialDelayedJoinMs)
			if err != nil {
				return &service.JoinGroupResp{
					MemberId:  memberId,
					ErrorCode: service.REBALANCE_IN_PROGRESS,
				}, nil
			}
		}
		members := g.getLeaderMembers(group, memberId)
		return &service.JoinGroupResp{
			ErrorCode:    service.NONE,
			GenerationId: group.generationId,
			ProtocolType: &group.protocolType,
			ProtocolName: group.supportedProtocol,
			LeaderId:     g.getMemberLeader(group),
			MemberId:     memberId,
			Members:      members,
		}, nil
	}

	if g.getGroupStatus(group) == CompletingRebalance {
		if memberId == EmptyMemberId || !g.checkMemberExist(group, memberId) {
			memberId, err = g.addMemberAndRebalance(group, clientId, memberId, protocolType, protocols, g.kafsarConfig.InitialDelayedJoinMs)
			if err != nil {
				logrus.Errorf("member %s join group %s failed, cause: %s", memberId, groupId, err)
				return &service.JoinGroupResp{
					MemberId:  memberId,
					ErrorCode: service.REBALANCE_IN_PROGRESS,
				}, nil
			}
		} else {
			if !matchProtocols(group.groupProtocols, protocols) {
				// member is joining with the different metadata
				err := g.updateMemberAndRebalance(group, clientId, memberId, protocolType, protocols, g.kafsarConfig.InitialDelayedJoinMs)
				if err != nil {
					logrus.Errorf("member %s join group %s failed, cause: %s", memberId, groupId, err)
					return &service.JoinGroupResp{
						MemberId:  memberId,
						ErrorCode: service.REBALANCE_IN_PROGRESS,
					}, nil
				}
			}
		}
		members := g.getLeaderMembers(group, memberId)
		return &service.JoinGroupResp{
			ErrorCode:    service.NONE,
			GenerationId: group.generationId,
			ProtocolType: &group.protocolType,
			ProtocolName: group.supportedProtocol,
			LeaderId:     g.getMemberLeader(group),
			MemberId:     memberId,
			Members:      members,
		}, nil
	}

	if g.getGroupStatus(group) == Empty || g.getGroupStatus(group) == Stable {
		if memberId == EmptyMemberId || !g.checkMemberExist(group, memberId) {
			memberId, err = g.addMemberAndRebalance(group, clientId, memberId, protocolType, protocols, g.kafsarConfig.InitialDelayedJoinMs)
			if err != nil {
				logrus.Errorf("member %s join group %s failed, cause: %s", memberId, groupId, err)
				return &service.JoinGroupResp{
					MemberId:  memberId,
					ErrorCode: service.REBALANCE_IN_PROGRESS,
				}, nil
			}
		} else {
			if g.isMemberLeader(group, memberId) || !matchProtocols(group.groupProtocols, protocols) {
				err := g.updateMemberAndRebalance(group, clientId, memberId, protocolType, protocols, g.kafsarConfig.InitialDelayedJoinMs)
				if err != nil {
					logrus.Errorf("member %s join group %s failed, cause: %s", memberId, groupId, err)
					return &service.JoinGroupResp{
						MemberId:  memberId,
						ErrorCode: service.REBALANCE_IN_PROGRESS,
					}, nil
				}
			}
		}
		members := g.getLeaderMembers(group, memberId)
		return &service.JoinGroupResp{
			ErrorCode:    service.NONE,
			GenerationId: group.generationId,
			ProtocolType: &group.protocolType,
			ProtocolName: group.supportedProtocol,
			LeaderId:     g.getMemberLeader(group),
			MemberId:     memberId,
			Members:      members,
		}, nil
	}
	return &service.JoinGroupResp{
		MemberId:  memberId,
		ErrorCode: service.UNKNOWN_SERVER_ERROR,
	}, nil
}

func (g *GroupCoordinatorStandalone) HandleSyncGroup(username, groupId, memberId string, generation int,
	groupAssignments []*service.GroupAssignment) (*service.SyncGroupResp, error) {
	code, err := g.syncGroupParamsCheck(groupId, memberId)
	if err != nil {
		logrus.Errorf("member %s snyc group %s failed, cause: %s", memberId, groupId, err)
		return &service.SyncGroupResp{ErrorCode: code}, nil
	}
	g.mutex.RLock()
	group, exist := g.groupManager[username+groupId]
	g.mutex.RUnlock()
	if !exist {
		logrus.Errorf("sync group %s failed, cause invalid groupId", groupId)
		return &service.SyncGroupResp{
			ErrorCode: service.INVALID_GROUP_ID,
		}, nil
	}
	_, exist = group.members[memberId]
	if !exist {
		logrus.Errorf("sync group %s failed, cause invalid memberId %s", groupId, memberId)
		return &service.SyncGroupResp{
			ErrorCode: service.UNKNOWN_MEMBER_ID,
		}, nil
	}
	// TODO generation check

	if g.getGroupStatus(group) == Empty || g.getGroupStatus(group) == Dead {
		return &service.SyncGroupResp{
			ErrorCode: service.UNKNOWN_MEMBER_ID,
		}, nil
	}

	// maybe new member add, need to rebalance again
	if g.getGroupStatus(group) == PreparingRebalance {
		return &service.SyncGroupResp{
			ErrorCode: service.REBALANCE_IN_PROGRESS,
		}, nil
	}

	if g.getGroupStatus(group) == CompletingRebalance {
		// get assignment from leader member
		if g.isMemberLeader(group, memberId) {
			for i := range groupAssignments {
				logrus.Infof("Assignment %#+v received from leader %s for group %s for generation %d", groupAssignments[i], memberId, groupId, generation)
				group.members[groupAssignments[i].MemberId].assignment = []byte(groupAssignments[i].MemberAssignment)
			}
			g.setGroupStatus(group, Stable)
			return &service.SyncGroupResp{
				ErrorCode:        service.NONE,
				MemberAssignment: string(group.members[memberId].assignment),
			}, nil
		}
		err := g.awaitingRebalance(group, g.kafsarConfig.RebalanceTickMs, group.sessionTimeoutMs, Stable)
		if err != nil {
			logrus.Errorf("member %s sync group %s failed, cause: %s", memberId, groupId, err)
			return &service.SyncGroupResp{
				ErrorCode:        service.REBALANCE_IN_PROGRESS,
				MemberAssignment: string(group.members[memberId].assignment),
			}, nil
		}
		return &service.SyncGroupResp{
			ErrorCode:        service.NONE,
			MemberAssignment: string(group.members[memberId].assignment),
		}, nil

	}

	// if the group is stable, we just return the current assignment
	if g.getGroupStatus(group) == Stable {
		return &service.SyncGroupResp{
			ErrorCode:        service.NONE,
			MemberAssignment: string(group.members[memberId].assignment),
		}, nil
	}
	return &service.SyncGroupResp{
		ErrorCode: service.UNKNOWN_SERVER_ERROR,
	}, nil
}

func (g *GroupCoordinatorStandalone) HandleLeaveGroup(username, groupId string,
	members []*service.LeaveGroupMember) (*service.LeaveGroupResp, error) {
	// reject if groupId is empty
	if groupId == "" {
		logrus.Errorf("leave group failed, cause groupId is empty")
		return &service.LeaveGroupResp{
			ErrorCode: service.INVALID_GROUP_ID,
		}, nil
	}
	g.mutex.RLock()
	group, exist := g.groupManager[username+groupId]
	g.mutex.RUnlock()
	if !exist {
		logrus.Errorf("leave group failed, cause group not exist")
		return &service.LeaveGroupResp{
			ErrorCode: service.INVALID_GROUP_ID,
		}, nil
	}
	membersMetadata := group.members
	for i := range members {
		if members[i].MemberId == g.getMemberLeader(group) {
			g.setMemberLeader(group, "")
		}
		delete(membersMetadata, members[i].MemberId)
		logrus.Infof("reader member: %s success leave group: %s", members[i].MemberId, groupId)
	}
	if len(group.members) == 0 {
		g.setGroupStatus(group, Empty)
	}
	// any member leave group should do rebalance
	g.setGroupStatus(group, PreparingRebalance)
	return &service.LeaveGroupResp{ErrorCode: service.NONE, Members: members}, nil
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

func (g *GroupCoordinatorStandalone) addMemberAndRebalance(group *Group, clientId, memberId, protocolType string, protocols []*service.GroupProtocol, rebalanceDelayMs int) (string, error) {
	if memberId == EmptyMemberId {
		memberId = clientId + "-" + uuid.New().String()
	}
	protocolMap := make(map[string]string)
	for i := range protocols {
		protocolMap[protocols[i].ProtocolName] = protocols[i].ProtocolMetadata
	}
	if g.getGroupStatus(group) == Empty {
		g.vote(group, protocols)
	}
	group.groupLock.Lock()
	group.members[memberId] = &memberMetadata{
		clientId:     clientId,
		memberId:     memberId,
		metadata:     protocolMap[group.supportedProtocol],
		protocolType: protocolType,
		protocols:    protocolMap,
	}
	group.groupLock.Unlock()
	g.prepareRebalance(group)
	return memberId, g.doRebalance(group, rebalanceDelayMs)
}

func (g *GroupCoordinatorStandalone) updateMemberAndRebalance(group *Group, clientId, memberId, protocolType string, protocols []*service.GroupProtocol, rebalanceDelayMs int) error {
	g.prepareRebalance(group)
	return g.doRebalance(group, rebalanceDelayMs)
}

func (g *GroupCoordinatorStandalone) HandleHeartBeat(username, groupId string) *service.HeartBeatResp {
	if groupId == "" {
		logrus.Errorf("groupId is empty.")
		return &service.HeartBeatResp{
			ErrorCode: service.INVALID_GROUP_ID,
		}
	}
	g.mutex.RLock()
	group, exist := g.groupManager[username+groupId]
	g.mutex.RUnlock()
	if !exist {
		logrus.Errorf("get group failed. cause group not exist, groupId: %s", groupId)
		return &service.HeartBeatResp{
			ErrorCode: service.REBALANCE_IN_PROGRESS,
		}
	}
	if g.getGroupStatus(group) == PreparingRebalance || g.getGroupStatus(group) == CompletingRebalance || g.getGroupStatus(group) == Dead {
		logrus.Infof("preparing rebalance. groupId: %s", groupId)
		return &service.HeartBeatResp{
			ErrorCode: service.REBALANCE_IN_PROGRESS,
		}
	}
	return &service.HeartBeatResp{ErrorCode: service.NONE}
}

func (g *GroupCoordinatorStandalone) prepareRebalance(group *Group) {
	g.setGroupStatus(group, PreparingRebalance)
}

func (g *GroupCoordinatorStandalone) doRebalance(group *Group, rebalanceDelayMs int) error {
	group.groupLock.Lock()
	if group.canRebalance {
		group.canRebalance = false
		group.groupLock.Unlock()
		logrus.Infof("preparing to rebalance group %s with old generation %d", group.groupId, group.generationId)
		time.Sleep(time.Duration(rebalanceDelayMs) * time.Millisecond)
		g.setGroupStatus(group, CompletingRebalance)
		group.generationId++
		logrus.Infof("completing rebalance group %s with new generation %d", group.groupId, group.generationId)
		group.canRebalance = true
		return nil
	} else {
		group.groupLock.Unlock()
		return g.awaitingRebalance(group, g.kafsarConfig.RebalanceTickMs, group.sessionTimeoutMs, CompletingRebalance)
	}
}

func (g *GroupCoordinatorStandalone) vote(group *Group, protocols []*service.GroupProtocol) {
	// TODO make clear multiple protocol scene
	group.groupLock.Lock()
	group.supportedProtocol = protocols[0].ProtocolName
	group.groupLock.Unlock()
}

func (g *GroupCoordinatorStandalone) awaitingRebalance(group *Group, rebalanceTickMs int, sessionTimeout int, waitForStatus GroupStatus) error {
	start := time.Now()
	for {
		if g.getGroupStatus(group) == waitForStatus {
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

func (g *GroupCoordinatorStandalone) setGroupStatus(group *Group, status GroupStatus) {
	group.groupStatusLock.Lock()
	group.groupStatus = status
	group.groupStatusLock.Unlock()
}

func (g *GroupCoordinatorStandalone) syncGroupParamsCheck(groupId, memberId string) (service.ErrorCode, error) {
	// reject if groupId is empty
	if groupId == "" {
		return service.INVALID_GROUP_ID, errors.Errorf("groupId is empty")
	}
	// reject if memberId is empty
	if memberId == "" {
		return service.MEMBER_ID_REQUIRED, errors.Errorf("memberId is empty")
	}
	return service.NONE, nil
}

func (g *GroupCoordinatorStandalone) joinGroupParamsCheck(clientId, groupId, memberId string, sessionTimeoutMs int, config KafsarConfig) (string, service.ErrorCode, error) {
	// reject if groupId is empty
	if groupId == "" {
		return memberId, service.INVALID_GROUP_ID, errors.Errorf("empty groupId")
	}

	// reject if sessionTimeoutMs is invalid
	if sessionTimeoutMs < config.GroupMinSessionTimeoutMs || sessionTimeoutMs > config.GroupMaxSessionTimeoutMs {
		return memberId, service.INVALID_SESSION_TIMEOUT, errors.Errorf("invalid sessionTimeoutMs: %d. minSessionTimeoutMs: %d, maxSessionTimeoutMs: %d",
			sessionTimeoutMs, config.GroupMinSessionTimeoutMs, config.GroupMaxSessionTimeoutMs)
	}
	return memberId, service.NONE, nil
}

func (g *GroupCoordinatorStandalone) joinGroupProtocolCheck(group *Group, protocolType string, protocols []*service.GroupProtocol, config KafsarConfig) (service.ErrorCode, error) {
	// if the new member does not support the group protocol, reject it
	if g.getGroupStatus(group) != Empty {
		if group.protocolType != protocolType {
			return service.INCONSISTENT_GROUP_PROTOCOL, errors.Errorf("invalid protocolType: %s, and this group protocolType is %s", protocolType, group.protocolType)
		}
		if !supportsProtocols(group.groupProtocols, protocols) {
			return service.INCONSISTENT_GROUP_PROTOCOL, errors.Errorf("protocols not match")
		}
	}

	// reject if first member with empty group protocol or protocolType is empty
	if g.getGroupStatus(group) == Empty {
		if protocolType == "" {
			return service.INCONSISTENT_GROUP_PROTOCOL, errors.Errorf("empty protocolType")
		}
		if len(protocols) == 0 {
			return service.INCONSISTENT_GROUP_PROTOCOL, errors.Errorf("empty protocol")
		}
	}
	return service.NONE, nil
}

func supportsProtocols(groupProtocols map[string]string, memberProtocols []*service.GroupProtocol) bool {
	// TODO groupProtocols must be contains memberProtocols
	return true
}

func matchProtocols(groupProtocols map[string]string, memberProtocols []*service.GroupProtocol) bool {
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

func (g *GroupCoordinatorStandalone) getLeaderMembers(group *Group, memberId string) (members []*service.Member) {
	if g.getMemberLeader(group) == "" {
		g.setMemberLeader(group, memberId)
	}
	if g.isMemberLeader(group, memberId) {
		for _, member := range group.members {
			members = append(members, &service.Member{MemberId: member.memberId, GroupInstanceId: nil, Metadata: member.metadata})
		}
	}
	return members
}

func (g *GroupCoordinatorStandalone) checkMemberExist(group *Group, memberId string) bool {
	_, exist := group.members[memberId]
	return exist
}
