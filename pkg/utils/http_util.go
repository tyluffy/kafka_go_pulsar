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
	"errors"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"time"
)

var client *http.Client

func init() {
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:       1,
			IdleConnTimeout:    30 * time.Second,
			DisableCompression: false,
		},
	}
}

func HttpGet(url string, params map[string]string, header map[string]string) (resp []byte, err error) {
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		logrus.Errorf("new request failed. err: %s", err)
		return nil, err
	}
	query := request.URL.Query()
	for key, value := range params {
		query.Add(key, value)
	}
	request.Header.Set("Content-Type", "application/json")
	for key, value := range header {
		request.Header.Set(key, value)
	}
	request.URL.RawQuery = query.Encode()
	response, err := client.Do(request)
	if err != nil {
		logrus.Errorf("send request failed. err: %s", err)
		return nil, err
	}
	defer response.Body.Close()
	msg, err := ioutil.ReadAll(response.Body)
	if err != nil {
		logrus.Errorf("get response failed. err: %s", err)
		return nil, err
	}
	if statusCodeSuccess(response.StatusCode) {
		return msg, nil
	}
	logrus.Errorf("http request failed. code is： %d, msg: %s", response.StatusCode, string(msg))
	return nil, errors.New("http request failed")
}

func statusCodeSuccess(code int) bool {
	return code >= http.StatusOK && code < http.StatusMultipleChoices
}
