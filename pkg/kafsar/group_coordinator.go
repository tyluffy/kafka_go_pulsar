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
	"container/list"
	"github.com/apache/pulsar-client-go/pulsar"
	"sync"
)

type Group struct {
	groupId            string
	partitionedTopic   []string
	groupStatus        GroupStatus
	supportedProtocol  string
	groupProtocols     map[string]string
	protocolType       string
	leader             string
	members            map[string]*memberMetadata
	canRebalance       bool
	generationId       int
	groupLock          sync.RWMutex
	groupStatusLock    sync.RWMutex
	groupMemberLock    sync.RWMutex
	groupNewMemberLock sync.RWMutex
	sessionTimeoutMs   int
}

type memberMetadata struct {
	clientId         string
	memberId         string
	metadata         []byte
	assignment       []byte
	protocolType     string
	protocols        map[string][]byte
	joinGenerationId int
	syncGenerationId int
}

type ReaderMetadata struct {
	groupId    string
	channel    chan pulsar.ReaderMessage
	reader     pulsar.Reader
	messageIds *list.List
	mutex      sync.RWMutex
}

type GroupStatus int

const (
	PreparingRebalance GroupStatus = 1 + iota
	CompletingRebalance
	Stable
	Dead
	Empty
)

type GroupCoordinatorType int

const (
	Standalone GroupCoordinatorType = 0 + iota
	Cluster
)

const (
	EmptyMemberId = ""
)
