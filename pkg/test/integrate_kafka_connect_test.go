package test

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestKafkaConnect(t *testing.T) {
	setupPulsar()
	_, port := setupKafsar()
	time.Sleep(3 * time.Second)
	topic := "my-topic"
	partition := 0
	addr := fmt.Sprintf("localhost:%d", port)
	_, err := kafka.DialLeader(context.Background(), "tcp", addr, topic, partition)
	assert.Nil(t, err)
}
