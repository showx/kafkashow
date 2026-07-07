package kafka

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Client struct {
	brokers []string
	client  *kafka.Client
}

type TopicInfo struct {
	Name       string
	Partitions int
}

type PartitionInfo struct {
	ID         int
	Leader     int
	Replicas   []int
	FirstOffset int64
	LastOffset  int64
}

type GroupInfo struct {
	ID     string
	State  string
	Members int
}

type GroupMember struct {
	ID            string
	ClientID      string
	Host          string
	AssignedTopics []string
}

type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       string
	Value     string
	Time      time.Time
}

func New(brokers []string) (*Client, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("至少需要一个 broker 地址")
	}

	c := &Client{
		brokers: brokers,
		client: &kafka.Client{
			Addr:    kafka.TCP(brokers[0]),
			Timeout: 10 * time.Second,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.ping(ctx); err != nil {
		return nil, fmt.Errorf("连接 Kafka 失败: %w", err)
	}

	return c, nil
}

func (c *Client) Brokers() []string {
	return c.brokers
}

func (c *Client) ping(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", c.brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Brokers()
	return err
}

func (c *Client) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	conn, err := kafka.DialContext(ctx, "tcp", c.brokers[0])
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, err
	}

	topicMap := make(map[string]int)
	for _, p := range partitions {
		if strings.HasPrefix(p.Topic, "__") {
			continue
		}
		topicMap[p.Topic]++
	}

	topics := make([]TopicInfo, 0, len(topicMap))
	for name, count := range topicMap {
		topics = append(topics, TopicInfo{Name: name, Partitions: count})
	}
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})
	return topics, nil
}

func (c *Client) GetTopicPartitions(ctx context.Context, topic string) ([]PartitionInfo, error) {
	conn, err := kafka.DialContext(ctx, "tcp", c.brokers[0])
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, err
	}

	result := make([]PartitionInfo, 0, len(partitions))
	for _, p := range partitions {
		info := PartitionInfo{
			ID:       p.ID,
			Leader:   p.Leader.ID,
			Replicas: replicaIDs(p.Replicas),
		}

		first, last, err := c.partitionOffsets(ctx, topic, p.ID)
		if err == nil {
			info.FirstOffset = first
			info.LastOffset = last
		}
		result = append(result, info)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result, nil
}

func (c *Client) partitionOffsets(ctx context.Context, topic string, partition int) (int64, int64, error) {
	conn, err := kafka.DialLeader(ctx, "tcp", c.brokers[0], topic, partition)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()

	first, err := conn.ReadFirstOffset()
	if err != nil {
		return 0, 0, err
	}
	last, err := conn.ReadLastOffset()
	if err != nil {
		return 0, 0, err
	}
	return first, last, nil
}

func (c *Client) ReadMessages(ctx context.Context, topic string, partition int, startOffset int64, limit int) ([]Message, error) {
	conn, err := kafka.DialLeader(ctx, "tcp", c.brokers[0], topic, partition)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if startOffset < 0 {
		last, err := conn.ReadLastOffset()
		if err != nil {
			return nil, err
		}
		startOffset = max(0, last-int64(limit))
	}

	if _, err := conn.Seek(startOffset, 0); err != nil {
		return nil, err
	}

	batch := conn.ReadBatchWith(kafka.ReadBatchConfig{
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  3 * time.Second,
	})
	defer batch.Close()

	messages := make([]Message, 0, limit)
	for len(messages) < limit {
		msg, err := batch.ReadMessage()
		if err != nil {
			break
		}
		messages = append(messages, Message{
			Topic:     topic,
			Partition: partition,
			Offset:    msg.Offset,
			Key:       string(msg.Key),
			Value:     string(msg.Value),
			Time:      msg.Time,
		})
	}
	return messages, nil
}

func (c *Client) ListGroups(ctx context.Context) ([]GroupInfo, error) {
	resp, err := c.client.ListGroups(ctx, &kafka.ListGroupsRequest{})
	if err != nil {
		return nil, err
	}

	groups := make([]GroupInfo, 0, len(resp.Groups))
	for _, g := range resp.Groups {
		groups = append(groups, GroupInfo{
			ID:    g.GroupID,
			State: "listed",
		})
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].ID < groups[j].ID
	})
	return groups, nil
}

func (c *Client) DescribeGroup(ctx context.Context, groupID string) (string, []GroupMember, error) {
	resp, err := c.client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
		GroupIDs: []string{groupID},
	})
	if err != nil {
		return "", nil, err
	}
	if len(resp.Groups) == 0 {
		return "", nil, fmt.Errorf("consumer group 不存在: %s", groupID)
	}

	group := resp.Groups[0]
	members := make([]GroupMember, 0, len(group.Members))
	for _, m := range group.Members {
		members = append(members, GroupMember{
			ID:       m.MemberID,
			ClientID: m.ClientID,
			Host:     m.ClientHost,
		})
	}
	return group.GroupState, members, nil
}

func replicaIDs(replicas []kafka.Broker) []int {
	ids := make([]int, len(replicas))
	for i, r := range replicas {
		ids[i] = r.ID
	}
	return ids
}

func protocolState(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
