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

func (gcs *GroupCoordinatorStandalone) HandleJoinGroup(groupId, memberId, clientId, protocolType string, sessionTimeoutMs int,
	protocols []*service.GroupProtocol) (*service.JoinGroupResp, error) {
	// do parameters check
	memberId, code, err := gcs.joinGroupParamsCheck(clientId, groupId, memberId, sessionTimeoutMs, gcs.kafsarConfig)
	if err != nil {
		logrus.Errorf("join group %s failed, cause: %s", groupId, err)
		return &service.JoinGroupResp{
			MemberId:  memberId,
			ErrorCode: code,
		}, nil
	}

	gcs.mutex.Lock()
	group, exist := gcs.groupManager[groupId]
	if !exist {
		group = &Group{
			groupId:      groupId,
			groupStatus:  Empty,
			protocolType: protocolType,
			members:      make(map[string]*memberMetadata),
			canRebalance: true,
		}
		gcs.groupManager[groupId] = group
	}
	gcs.mutex.Unlock()

	code, err = gcs.joinGroupProtocolCheck(group, protocolType, protocols, gcs.kafsarConfig)
	if err != nil {
		logrus.Errorf("join group %s failed, cause: %s", groupId, err)
		return &service.JoinGroupResp{
			MemberId:  memberId,
			ErrorCode: code,
		}, nil
	}

	group.groupLock.RLock()
	numMember := len(group.members)
	group.groupLock.RUnlock()
	if gcs.kafsarConfig.MaxConsumersPerGroup > 0 && numMember >= gcs.kafsarConfig.MaxConsumersPerGroup {
		logrus.Errorf("join group failed, exceed maximum number of members. groupId: %s, memberId: %s, current: %d, maxConsumersPerGroup: %d",
			groupId, memberId, numMember, gcs.kafsarConfig.MaxConsumersPerGroup)
		return &service.JoinGroupResp{
			MemberId:  memberId,
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}

	if gcs.getGroupStatus(group) == Dead {
		logrus.Errorf("join group failed, cause group status is dead. groupId: %s, memberId: %s", groupId, memberId)
		return &service.JoinGroupResp{
			MemberId:  memberId,
			ErrorCode: service.UNKNOWN_MEMBER_ID,
		}, nil
	}

	if gcs.getGroupStatus(group) == PreparingRebalance {
		if memberId == EmptyMemberId {
			memberId = gcs.addMemberAndRebalance(group, clientId, protocolType, protocols, gcs.kafsarConfig.InitialDelayedJoinMs)
		} else {
			gcs.updateMemberAndRebalance(group, clientId, memberId, protocolType, protocols, gcs.kafsarConfig.InitialDelayedJoinMs)
		}
		var members []*service.Member
		if isMemberLeader(group, memberId) {
			for _, member := range group.members {
				members = append(members, &service.Member{MemberId: member.memberId, GroupInstanceId: nil, Metadata: member.metadata})
			}
		}
		return &service.JoinGroupResp{
			ErrorCode:    service.NONE,
			GenerationId: group.generationId,
			ProtocolType: &group.protocolType,
			ProtocolName: group.supportedProtocol,
			LeaderId:     group.leader,
			MemberId:     memberId,
			Members:      members,
		}, nil
	}

	if gcs.getGroupStatus(group) == CompletingRebalance {
		if memberId == EmptyMemberId {
			memberId = gcs.addMemberAndRebalance(group, clientId, protocolType, protocols, gcs.kafsarConfig.InitialDelayedJoinMs)
		} else {
			if !matchProtocols(group.groupProtocols, protocols) {
				// member is joining with the different metadata
				gcs.updateMemberAndRebalance(group, clientId, memberId, protocolType, protocols, gcs.kafsarConfig.InitialDelayedJoinMs)
			}
		}
		var members []*service.Member
		if isMemberLeader(group, memberId) {
			for _, member := range group.members {
				members = append(members, &service.Member{MemberId: member.memberId, GroupInstanceId: nil, Metadata: member.metadata})
			}
		}
		return &service.JoinGroupResp{
			ErrorCode:    service.NONE,
			GenerationId: group.generationId,
			ProtocolType: &group.protocolType,
			ProtocolName: group.supportedProtocol,
			LeaderId:     group.leader,
			MemberId:     memberId,
			Members:      members,
		}, nil
	}

	if gcs.getGroupStatus(group) == Empty || gcs.getGroupStatus(group) == Stable {
		if memberId == EmptyMemberId {
			memberId = gcs.addMemberAndRebalance(group, clientId, protocolType, protocols, gcs.kafsarConfig.InitialDelayedJoinMs)
		} else {
			if isMemberLeader(group, memberId) || !matchProtocols(group.groupProtocols, protocols) {
				gcs.updateMemberAndRebalance(group, clientId, memberId, protocolType, protocols, gcs.kafsarConfig.InitialDelayedJoinMs)
			}
		}
		var members []*service.Member
		if isMemberLeader(group, memberId) {
			for _, member := range group.members {
				members = append(members, &service.Member{MemberId: member.memberId, GroupInstanceId: nil, Metadata: member.metadata})
			}
		}
		return &service.JoinGroupResp{
			ErrorCode:    service.NONE,
			GenerationId: group.generationId,
			ProtocolType: &group.protocolType,
			ProtocolName: group.supportedProtocol,
			LeaderId:     group.leader,
			MemberId:     memberId,
			Members:      members,
		}, nil
	}
	return &service.JoinGroupResp{
		MemberId:  memberId,
		ErrorCode: service.UNKNOWN_SERVER_ERROR,
	}, nil
}

func (gcs *GroupCoordinatorStandalone) HandleSyncGroup(groupId, memberId string, generation int,
	groupAssignments []*service.GroupAssignment) (*service.SyncGroupResp, error) {
	code, err := gcs.syncGroupParamsCheck(groupId, memberId)
	if err != nil {
		logrus.Errorf("member %s snyc group %s failed, cause: %s", memberId, groupId, err)
		return &service.SyncGroupResp{ErrorCode: code}, nil
	}
	gcs.mutex.RLock()
	group, exist := gcs.groupManager[groupId]
	gcs.mutex.RUnlock()
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

	if gcs.getGroupStatus(group) == Empty || gcs.getGroupStatus(group) == Dead {
		return &service.SyncGroupResp{
			ErrorCode: service.UNKNOWN_MEMBER_ID,
		}, nil
	}

	// maybe new member add, need to rebalance again
	if gcs.getGroupStatus(group) == PreparingRebalance {
		return &service.SyncGroupResp{
			ErrorCode: service.REBALANCE_IN_PROGRESS,
		}, nil
	}

	if gcs.getGroupStatus(group) == CompletingRebalance {
		// get assignment from leader member
		if isMemberLeader(group, memberId) {
			logrus.Infof("Assignment received from leader %s for group %s for generation %d", memberId, groupId, generation)
			for i := range groupAssignments {
				group.members[groupAssignments[i].MemberId].assignment = []byte(groupAssignments[i].MemberAssignment)
			}
			gcs.setGroupStatus(group, Stable)
			return &service.SyncGroupResp{
				ErrorCode:        service.NONE,
				MemberAssignment: string(group.members[memberId].assignment),
			}, nil
		}
		gcs.awaitingRebalance(group, gcs.kafsarConfig.RebalanceTickMs, Stable)
		return &service.SyncGroupResp{
			ErrorCode:        service.NONE,
			MemberAssignment: string(group.members[memberId].assignment),
		}, nil

	}

	// if the group is stable, we just return the current assignment
	if gcs.getGroupStatus(group) == Stable {
		return &service.SyncGroupResp{
			ErrorCode:        service.NONE,
			MemberAssignment: string(group.members[memberId].assignment),
		}, nil
	}
	return &service.SyncGroupResp{
		ErrorCode: service.UNKNOWN_SERVER_ERROR,
	}, nil
}

func (gcs *GroupCoordinatorStandalone) HandleLeaveGroup(groupId string,
	members []*service.LeaveGroupMember) (*service.LeaveGroupResp, error) {
	// reject if groupId is empty
	if groupId == "" {
		logrus.Errorf("leave group failed, cause groupId is empty")
		return &service.LeaveGroupResp{
			ErrorCode: service.INVALID_GROUP_ID,
		}, nil
	}
	gcs.mutex.RLock()
	group, exist := gcs.groupManager[groupId]
	gcs.mutex.RUnlock()
	if !exist {
		logrus.Errorf("leave group failed, cause group not exist")
		return &service.LeaveGroupResp{
			ErrorCode: service.INVALID_GROUP_ID,
		}, nil
	}
	membersMetadata := group.members
	for i := range members {
		delete(membersMetadata, members[i].MemberId)
		logrus.Infof("reader member: %s success leave group: %s", members[i].MemberId, groupId)
	}
	if len(group.members) == 0 {
		gcs.setGroupStatus(group, Empty)
	}
	consumerMetadata := group.consumerMetadata
	if consumerMetadata != nil {
		consumerMetadata.reader.Close()
	}
	group.consumerMetadata = nil
	return &service.LeaveGroupResp{ErrorCode: service.NONE, Members: members}, nil
}

func (gcs *GroupCoordinatorStandalone) GetGroup(groupId string) (*Group, error) {
	gcs.mutex.RLock()
	group, exist := gcs.groupManager[groupId]
	gcs.mutex.RUnlock()
	if !exist {
		return nil, errors.New("invalid groupId")
	}
	return group, nil
}

func (gcs *GroupCoordinatorStandalone) addMemberAndRebalance(group *Group, clientId, protocolType string, protocols []*service.GroupProtocol, rebalanceDelayMs int) string {
	memberId := clientId + "-" + uuid.New().String()
	protocolMap := make(map[string]string)
	for i := range protocols {
		protocolMap[protocols[i].ProtocolName] = protocols[i].ProtocolMetadata
	}
	if gcs.getGroupStatus(group) == Empty {
		group.leader = memberId
		gcs.vote(group, protocols)
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
	gcs.prepareRebalance(group)
	gcs.doRebalance(group, rebalanceDelayMs)
	return memberId
}

func (gcs *GroupCoordinatorStandalone) updateMemberAndRebalance(group *Group, clientId, memberId, protocolType string, protocols []*service.GroupProtocol, rebalanceDelayMs int) {
	gcs.prepareRebalance(group)
	gcs.doRebalance(group, rebalanceDelayMs)
}

func (gcs *GroupCoordinatorStandalone) HandleHeartBeat(groupId string) *service.HeartBeatResp {
	if groupId == "" {
		logrus.Errorf("groupId is empty.")
		return &service.HeartBeatResp{
			ErrorCode: service.INVALID_GROUP_ID,
		}
	}
	gcs.mutex.RLock()
	group, exist := gcs.groupManager[groupId]
	gcs.mutex.RUnlock()
	if !exist {
		logrus.Errorf("get group failed. cause group not exist, groupId: %s", groupId)
		return &service.HeartBeatResp{
			ErrorCode: service.INVALID_GROUP_ID,
		}
	}
	if gcs.getGroupStatus(group) == PreparingRebalance {
		logrus.Infof("preparing rebalance. groupId: %s", groupId)
		return &service.HeartBeatResp{
			ErrorCode: service.REBALANCE_IN_PROGRESS,
		}
	}
	return &service.HeartBeatResp{ErrorCode: service.NONE}
}

func (gcs *GroupCoordinatorStandalone) prepareRebalance(group *Group) {
	gcs.setGroupStatus(group, PreparingRebalance)
}

func (gcs *GroupCoordinatorStandalone) doRebalance(group *Group, rebalanceDelayMs int) {
	group.groupLock.Lock()
	if group.canRebalance {
		group.canRebalance = false
		group.groupLock.Unlock()
		logrus.Infof("preparing to rebalance group %s with old generation %d", group.groupId, group.generationId)
		time.Sleep(time.Duration(rebalanceDelayMs) * time.Millisecond)
		gcs.setGroupStatus(group, CompletingRebalance)
		group.generationId++
		logrus.Infof("completing rebalance group %s with new generation %d", group.groupId, group.generationId)
		group.canRebalance = true
	} else {
		group.groupLock.Unlock()
		gcs.awaitingRebalance(group, gcs.kafsarConfig.RebalanceTickMs, CompletingRebalance)
	}
}

func (gcs *GroupCoordinatorStandalone) vote(group *Group, protocols []*service.GroupProtocol) {
	// TODO make clear multiple protocol scene
	group.supportedProtocol = protocols[0].ProtocolName
}

func (gcs *GroupCoordinatorStandalone) awaitingRebalance(group *Group, rebalanceTickMs int, waitForStatus GroupStatus) {
	for {
		if gcs.getGroupStatus(group) == waitForStatus {
			break
		}
		time.Sleep(time.Duration(rebalanceTickMs) * time.Millisecond)
	}
}

func (gcs *GroupCoordinatorStandalone) getGroupStatus(group *Group) GroupStatus {
	group.groupStatusLock.RLock()
	status := group.groupStatus
	group.groupStatusLock.RUnlock()
	return status
}

func (gcs *GroupCoordinatorStandalone) setGroupStatus(group *Group, status GroupStatus) {
	group.groupStatusLock.Lock()
	group.groupStatus = status
	group.groupStatusLock.Unlock()
}

func (gcs *GroupCoordinatorStandalone) syncGroupParamsCheck(groupId, memberId string) (service.ErrorCode, error) {
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

func (gcs *GroupCoordinatorStandalone) joinGroupParamsCheck(clientId, groupId, memberId string, sessionTimeoutMs int, config KafsarConfig) (string, service.ErrorCode, error) {
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

func (gcs *GroupCoordinatorStandalone) joinGroupProtocolCheck(group *Group, protocolType string, protocols []*service.GroupProtocol, config KafsarConfig) (service.ErrorCode, error) {
	// if the new member does not support the group protocol, reject it
	if gcs.getGroupStatus(group) != Empty {
		if group.protocolType != protocolType {
			return service.INCONSISTENT_GROUP_PROTOCOL, errors.Errorf("invalid protocolType: %s, and this group protocolType is %s", protocolType, group.protocolType)
		}
		if !supportsProtocols(group.groupProtocols, protocols) {
			return service.INCONSISTENT_GROUP_PROTOCOL, errors.Errorf("protocols not match")
		}
	}

	// reject if first member with empty group protocol or protocolType is empty
	if gcs.getGroupStatus(group) == Empty {
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

func isMemberLeader(group *Group, memberId string) bool {
	return group.leader == memberId
}
