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

package utils

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/paashzj/kafka_go_pulsar/pkg/constant"
	"github.com/sirupsen/logrus"
	"time"
)

func ReadEarliestMsg(topic string, maxWaitMs int, partition int, pulsarClient pulsar.Client) pulsar.Message {
	readerOptions := pulsar.ReaderOptions{
		Topic:          topic + fmt.Sprintf(constant.PartitionSuffixFormat, partition),
		Name:           constant.OffsetReaderEarliestName,
		StartMessageID: pulsar.EarliestMessageID(),
	}

	return readNextMsg(readerOptions, maxWaitMs, pulsarClient)
}

func readNextMsg(operation pulsar.ReaderOptions, maxWaitMs int, pulsarClient pulsar.Client) pulsar.Message {
	reader, err := pulsarClient.CreateReader(operation)
	if err != nil {
		logrus.Warnf("create pulsar lasted read failed. topic: %s, err: %s", operation.Topic, err)
		return nil
	}
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(maxWaitMs)*time.Millisecond)
	defer cancel()
	message, err := reader.Next(ctx)
	if err != nil {
		logrus.Errorf("get message failed. topic: %s", operation.Topic)
		return nil
	}
	return message
}
