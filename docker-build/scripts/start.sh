#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

export PULSAR_ALLOW_AUTO_TOPIC_CREATION_TYPE=partitioned
export PULSAR_BROKER_ENTRY_METADATA_INTERCEPTORS=org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor
export PULSAR_EXPOSING_BROKER_ENTRY_METADATA_TO_CLIENT_ENABLED=true
export REMOTE_MODE=false
# generate config
/opt/sh/pulsar/mate/config_gen
# start pulsar standalone
$PULSAR_HOME/bin/pulsar-daemon start standalone -nfw >>$PULSAR_HOME/pulsar.stdout.log 2>>$PULSAR_HOME/pulsar.stderr.log
sleep 60
/opt/sh/kafsar
