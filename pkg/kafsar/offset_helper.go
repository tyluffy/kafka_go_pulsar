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
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"strconv"
)

func convOffset(message pulsar.Message, continuousOffset bool) int64 {
	if continuousOffset {
		index := message.Index()
		if index == nil {
			panic(any("continuous offset mode, index field must be set"))
		}
		return int64(*index) + 1
	}
	return ConvertMsgId(message.ID())
}

func ConvertMsgId(messageId pulsar.MessageID) int64 {
	offset, _ := strconv.Atoi(fmt.Sprint(messageId.LedgerID()) + fmt.Sprint(messageId.EntryID()) + fmt.Sprint(messageId.PartitionIdx()))
	return int64(offset)
}
