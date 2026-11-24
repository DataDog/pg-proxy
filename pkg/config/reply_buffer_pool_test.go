// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package config

import (
	"encoding/json"
	"strings"
	"testing"
)

// reply_buffer_pool turns the reply buffer pool on from YAML and from JSON. An unset value
// keeps it off and is not serialized.
func TestReplyBufferPoolSetting(t *testing.T) {
	fromYAML, err := ReadYAML(strings.NewReader("proxy:\n  reply_buffer_pool: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	fromJSON, err := ReadJSON(strings.NewReader(`{"proxy":{"replyBufferPool":true}}`))
	if err != nil {
		t.Fatal(err)
	}
	if !fromYAML.Proxy.ReplyBufferPool || fromYAML.Proxy != fromJSON.Proxy {
		t.Fatalf("the setting was not read: YAML %+v, JSON %+v", fromYAML.Proxy, fromJSON.Proxy)
	}

	omitted, err := ReadYAML(strings.NewReader("proxy:\n  tls_required: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if omitted.Proxy.ReplyBufferPool {
		t.Fatal("an unset reply_buffer_pool turned the pool on")
	}
	data, err := json.Marshal(omitted)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "replyBufferPool") {
		t.Fatalf("an unset reply_buffer_pool was serialized: %s", data)
	}
}
