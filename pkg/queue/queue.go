package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anuranpaul/task-scheduler/pkg/logger"
	"github.com/redis/go-redis/v9"
)

// Queue provides a distributed job queue abstraction over Redis Streams.
// It supports priority queuing, consumer groups, and automatic recovery.
type Queue struct {
	client    *redis.Client
	streamKey string
	groupName string
	config    QueueConfig

	// Metrics
	messagesPublished    int64
	messagesConsumed     int64
	messagesAcknowledged int64
	messagesFailed       int64
	lastPublishTime      time.Time
	lastConsumeTime      time.Time
	metricsMu            sync.RWMutex

	// State
	closed bool
	mu     sync.RWMutex
}

// QueueConfig contains configuration for the queue.
type QueueConfig struct {
	StreamName       string
	GroupName        string
	MaxLen           int64         // Maximum stream length (0 = unlimited)
	BlockTimeout     time.Duration // Consumer block timeout
	ClaimMinIdle     time.Duration // Minimum idle time before claiming
	BatchSize        int64         // Number of messages to fetch per read
	PendingCheckRate time.Duration // How often to check for pending messages
}

// Message represents a job message in the queue.
type Message struct {
	ID        string                 // Redis stream message ID
	JobID     string                 // Application job ID
	Payload   string                 // Job payload
	Priority  int                    // Job priority (higher = more important)
	Metadata  map[string]interface{} // Additional metadata
	Timestamp time.Time              // When message was added
	Attempts  int                    // Processing attempts
}

// Consumer represents a message consumer in a consumer group.
type Consumer struct {
	queue      *Queue
	consumerID string
	handler    MessageHandler
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup

	// Recovery
	recoveryEnabled bool
	recoveryTicker  *time.Ticker
}

// MessageHandler is called for each message consumed.
type MessageHandler func(ctx context.Context, msg *Message) error

// PublishOptions contains options for publishing messages.
type PublishOptions struct {
	MaxLen         int64  // Stream max length (MAXLEN option)
	ApproxMaxLen   bool   // Use approximate trimming (~)
	MinIDThreshold string // Minimum ID to keep (MINID option)
}

// ConsumeOptions contains options for consuming messages.
type ConsumeOptions struct {
	Count   int64         // Number of messages to read
	Block   time.Duration // Block timeout (0 = no block)
	NoAck   bool          // Don't require explicit ACK
	StartID string        // Start reading from this ID (default ">")
}

// PendingMessage represents a pending (unacknowledged) message.
type PendingMessage struct {
	ID            string
	Consumer      string
	IdleTime      time.Duration
	DeliveryCount int64
}

// StreamInfo contains information about the stream.
type StreamInfo struct {
	Length          int64
	FirstEntryID    string
	LastEntryID     string
	Groups          int64
	PendingMessages int64
}

// NewQueue creates a new distributed queue instance.
func NewQueue(ctx context.Context, client *redis.Client, config QueueConfig) (*Queue, error) {
	if client == nil {
		return nil, fmt.Errorf("redis client cannot be nil")
	}

	if config.StreamName == "" {
		config.StreamName = "jobs"
	}

	if config.GroupName == "" {
		config.GroupName = "workers"
	}

	if config.BlockTimeout == 0 {
		config.BlockTimeout = 5 * time.Second
	}

	if config.ClaimMinIdle == 0 {
		config.ClaimMinIdle = 5 * time.Minute
	}

	if config.BatchSize == 0 {
		config.BatchSize = 10
	}

	if config.PendingCheckRate == 0 {
		config.PendingCheckRate = 30 * time.Second
	}

	// Test connection
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection test failed: %w", err)
	}

	q := &Queue{
		client:    client,
		streamKey: config.StreamName,
		groupName: config.GroupName,
		config:    config,
	}

	logger.Info(ctx, "queue initialized",
		"stream", config.StreamName,
		"group", config.GroupName,
		"batch_size", config.BatchSize)

	return q, nil
}

// CreateConsumerGroup creates the consumer group if it doesn't exist.
// This should be called before starting consumers.
func (q *Queue) CreateConsumerGroup(ctx context.Context) error {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return fmt.Errorf("queue is closed")
	}
	q.mu.RUnlock()

	// Create consumer group, creating the stream if needed
	err := q.client.XGroupCreateMkStream(ctx, q.streamKey, q.groupName, "0").Err()
	if err != nil {
		// Group already exists - this is fine
		if err.Error() == "BUSYGROUP Consumer Group name already exists" {
			logger.Debug(ctx, "consumer group already exists",
				"stream", q.streamKey,
				"group", q.groupName)
			return nil
		}
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	logger.Info(ctx, "consumer group created",
		"stream", q.streamKey,
		"group", q.groupName)

	return nil
}

// Publish adds a message to the queue.
func (q *Queue) Publish(ctx context.Context, msg *Message) (string, error) {
	return q.PublishWithOptions(ctx, msg, PublishOptions{})
}

// PublishWithOptions adds a message to the queue with custom options.
func (q *Queue) PublishWithOptions(ctx context.Context, msg *Message, opts PublishOptions) (string, error) {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return "", fmt.Errorf("queue is closed")
	}
	q.mu.RUnlock()

	if msg == nil {
		return "", fmt.Errorf("message cannot be nil")
	}

	if msg.JobID == "" {
		return "", fmt.Errorf("message job_id cannot be empty")
	}

	// Prepare message fields
	fields := map[string]interface{}{
		"job_id":    msg.JobID,
		"payload":   msg.Payload,
		"priority":  msg.Priority,
		"timestamp": time.Now().Unix(),
		"attempts":  msg.Attempts,
	}

	// Add metadata fields
	if msg.Metadata != nil {
		for k, v := range msg.Metadata {
			fields[fmt.Sprintf("meta_%s", k)] = v
		}
	}

	// Build XAdd args
	args := &redis.XAddArgs{
		Stream: q.streamKey,
		Values: fields,
	}

	// Apply options
	if opts.MaxLen > 0 {
		args.MaxLen = opts.MaxLen
		args.Approx = opts.ApproxMaxLen
	} else if q.config.MaxLen > 0 {
		args.MaxLen = q.config.MaxLen
		args.Approx = true
	}

	if opts.MinIDThreshold != "" {
		args.MinID = opts.MinIDThreshold
	}

	// Publish to stream
	streamID, err := q.client.XAdd(ctx, args).Result()
	if err != nil {
		atomic.AddInt64(&q.messagesFailed, 1)
		return "", fmt.Errorf("failed to publish message: %w", err)
	}

	// Update metrics
	atomic.AddInt64(&q.messagesPublished, 1)
	q.metricsMu.Lock()
	q.lastPublishTime = time.Now()
	q.metricsMu.Unlock()

	logger.Debug(ctx, "message published",
		"stream_id", streamID,
		"job_id", msg.JobID,
		"priority", msg.Priority)

	return streamID, nil
}

// PublishBatch publishes multiple messages atomically using pipeline.
func (q *Queue) PublishBatch(ctx context.Context, messages []*Message) ([]string, error) {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return nil, fmt.Errorf("queue is closed")
	}
	q.mu.RUnlock()

	if len(messages) == 0 {
		return []string{}, nil
	}

	// Use pipeline for batch publish
	pipe := q.client.Pipeline()

	cmds := make([]*redis.StringCmd, len(messages))
	for i, msg := range messages {
		if msg.JobID == "" {
			return nil, fmt.Errorf("message[%d] job_id cannot be empty", i)
		}

		fields := map[string]interface{}{
			"job_id":    msg.JobID,
			"payload":   msg.Payload,
			"priority":  msg.Priority,
			"timestamp": time.Now().Unix(),
			"attempts":  msg.Attempts,
		}

		if msg.Metadata != nil {
			for k, v := range msg.Metadata {
				fields[fmt.Sprintf("meta_%s", k)] = v
			}
		}

		args := &redis.XAddArgs{
			Stream: q.streamKey,
			Values: fields,
		}

		if q.config.MaxLen > 0 {
			args.MaxLen = q.config.MaxLen
			args.Approx = true
		}

		cmds[i] = pipe.XAdd(ctx, args)
	}

	// Execute pipeline
	if _, err := pipe.Exec(ctx); err != nil {
		atomic.AddInt64(&q.messagesFailed, int64(len(messages)))
		return nil, fmt.Errorf("failed to publish batch: %w", err)
	}

	// Collect results
	streamIDs := make([]string, len(cmds))
	for i, cmd := range cmds {
		streamIDs[i] = cmd.Val()
	}

	// Update metrics
	atomic.AddInt64(&q.messagesPublished, int64(len(messages)))
	q.metricsMu.Lock()
	q.lastPublishTime = time.Now()
	q.metricsMu.Unlock()

	logger.Info(ctx, "batch published",
		"count", len(messages),
		"stream", q.streamKey)

	return streamIDs, nil
}

// NewConsumer creates a new consumer for this queue.
func (q *Queue) NewConsumer(ctx context.Context, consumerID string, handler MessageHandler) (*Consumer, error) {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return nil, fmt.Errorf("queue is closed")
	}
	q.mu.RUnlock()

	if consumerID == "" {
		return nil, fmt.Errorf("consumer_id cannot be empty")
	}

	if handler == nil {
		return nil, fmt.Errorf("handler cannot be nil")
	}

	consumerCtx, cancel := context.WithCancel(ctx)

	consumer := &Consumer{
		queue:           q,
		consumerID:      consumerID,
		handler:         handler,
		ctx:             consumerCtx,
		cancel:          cancel,
		recoveryEnabled: true,
	}

	logger.Info(ctx, "consumer created",
		"consumer_id", consumerID,
		"stream", q.streamKey,
		"group", q.groupName)

	return consumer, nil
}

// Start begins consuming messages. Blocks until context is cancelled.
func (c *Consumer) Start(ctx context.Context) error {
	ctx = logger.With(ctx, "consumer", c.consumerID)
	logger.Info(ctx, "consumer starting")

	// Start recovery goroutine if enabled
	if c.recoveryEnabled {
		c.recoveryTicker = time.NewTicker(c.queue.config.PendingCheckRate)
		c.wg.Add(1)
		go c.runRecovery(ctx)
	}

	// Main consumption loop
	opts := ConsumeOptions{
		Count: c.queue.config.BatchSize,
		Block: c.queue.config.BlockTimeout,
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "consumer stopping")
			return c.shutdown(ctx)
		case <-c.ctx.Done():
			logger.Info(ctx, "consumer cancelled")
			return c.shutdown(ctx)
		default:
			if err := c.consumeBatch(ctx, opts); err != nil {
				// Only log non-nil errors
				if err != redis.Nil {
					logger.Error(ctx, "consume error", err)
					// Small backoff on errors
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	}
}

func (c *Consumer) consumeBatch(ctx context.Context, opts ConsumeOptions) error {
	streams, err := c.queue.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.queue.groupName,
		Consumer: c.consumerID,
		Streams:  []string{c.queue.streamKey, ">"},
		Count:    opts.Count,
		Block:    opts.Block,
	}).Result()

	if err != nil {
		return err
	}

	for _, stream := range streams {
		for _, xmsg := range stream.Messages {
			msg, err := c.parseMessage(xmsg)
			if err != nil {
				logger.Error(ctx, "failed to parse message", err,
					"stream_id", xmsg.ID)
				// ACK malformed messages to remove them
				c.Ack(ctx, xmsg.ID)
				continue
			}

			if err := c.processMessage(ctx, msg); err != nil {
				logger.Error(ctx, "failed to process message", err,
					"job_id", msg.JobID,
					"stream_id", msg.ID)
				atomic.AddInt64(&c.queue.messagesFailed, 1)
				continue
			}

			// ACK successful processing
			if err := c.Ack(ctx, msg.ID); err != nil {
				logger.Error(ctx, "failed to ack message", err,
					"stream_id", msg.ID)
			} else {
				atomic.AddInt64(&c.queue.messagesAcknowledged, 1)
			}
		}
	}

	return nil
}

func (c *Consumer) processMessage(ctx context.Context, msg *Message) error {
	atomic.AddInt64(&c.queue.messagesConsumed, 1)
	c.queue.metricsMu.Lock()
	c.queue.lastConsumeTime = time.Now()
	c.queue.metricsMu.Unlock()

	// Call handler
	return c.handler(ctx, msg)
}

func (c *Consumer) parseMessage(xmsg redis.XMessage) (*Message, error) {
	msg := &Message{
		ID:       xmsg.ID,
		Metadata: make(map[string]interface{}),
	}

	// Extract standard fields
	if jobID, ok := xmsg.Values["job_id"].(string); ok {
		msg.JobID = jobID
	} else {
		return nil, fmt.Errorf("missing job_id field")
	}

	if payload, ok := xmsg.Values["payload"].(string); ok {
		msg.Payload = payload
	}

	if priority, ok := xmsg.Values["priority"].(string); ok {
		fmt.Sscanf(priority, "%d", &msg.Priority)
	}

	if timestamp, ok := xmsg.Values["timestamp"].(string); ok {
		var ts int64
		fmt.Sscanf(timestamp, "%d", &ts)
		msg.Timestamp = time.Unix(ts, 0)
	}

	if attempts, ok := xmsg.Values["attempts"].(string); ok {
		fmt.Sscanf(attempts, "%d", &msg.Attempts)
	}

	// Extract metadata fields
	for k, v := range xmsg.Values {
		if len(k) > 5 && k[:5] == "meta_" {
			metaKey := k[5:]
			msg.Metadata[metaKey] = v
		}
	}

	return msg, nil
}

// Ack acknowledges a message, removing it from pending.
func (c *Consumer) Ack(ctx context.Context, messageID string) error {
	return c.queue.client.XAck(ctx, c.queue.streamKey, c.queue.groupName, messageID).Err()
}

// Nack negative acknowledges a message, making it available for reprocessing.
// This is done by not ACKing - the message will be claimed by recovery.
func (c *Consumer) Nack(ctx context.Context, messageID string) error {
	// In Redis Streams, we just don't ACK the message
	logger.Debug(ctx, "message nacked", "stream_id", messageID)
	return nil
}

func (c *Consumer) runRecovery(ctx context.Context) {
	defer c.wg.Done()

	ctx = logger.With(ctx, "component", "recovery", "consumer", c.consumerID)
	logger.Info(ctx, "recovery worker started")

	for {
		select {
		case <-ctx.Done():
			c.recoveryTicker.Stop()
			logger.Info(ctx, "recovery worker stopped")
			return
		case <-c.ctx.Done():
			c.recoveryTicker.Stop()
			logger.Info(ctx, "recovery worker cancelled")
			return
		case <-c.recoveryTicker.C:
			if err := c.claimPendingMessages(ctx); err != nil {
				logger.Error(ctx, "failed to claim pending messages", err)
			}
		}
	}
}

func (c *Consumer) claimPendingMessages(ctx context.Context) error {
	// Get pending messages that are idle
	pending, err := c.queue.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: c.queue.streamKey,
		Group:  c.queue.groupName,
		Start:  "-",
		End:    "+",
		Count:  100,
		Idle:   c.queue.config.ClaimMinIdle,
	}).Result()

	if err != nil {
		return fmt.Errorf("failed to get pending messages: %w", err)
	}

	if len(pending) == 0 {
		return nil
	}

	logger.Info(ctx, "found pending messages", "count", len(pending))

	// Claim messages
	messageIDs := make([]string, len(pending))
	for i, p := range pending {
		messageIDs[i] = p.ID
	}

	claimed, err := c.queue.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   c.queue.streamKey,
		Group:    c.queue.groupName,
		Consumer: c.consumerID,
		MinIdle:  c.queue.config.ClaimMinIdle,
		Messages: messageIDs,
	}).Result()

	if err != nil {
		return fmt.Errorf("failed to claim messages: %w", err)
	}

	logger.Info(ctx, "claimed pending messages", "count", len(claimed))

	// Process claimed messages
	for _, xmsg := range claimed {
		msg, err := c.parseMessage(xmsg)
		if err != nil {
			logger.Error(ctx, "failed to parse claimed message", err)
			c.Ack(ctx, xmsg.ID) // Remove malformed message
			continue
		}

		// Increment attempts
		msg.Attempts++

		logger.Info(ctx, "processing claimed message",
			"job_id", msg.JobID,
			"attempts", msg.Attempts)

		if err := c.processMessage(ctx, msg); err != nil {
			logger.Error(ctx, "failed to process claimed message", err,
				"job_id", msg.JobID)
			atomic.AddInt64(&c.queue.messagesFailed, 1)
			continue
		}

		if err := c.Ack(ctx, msg.ID); err != nil {
			logger.Error(ctx, "failed to ack claimed message", err)
		} else {
			atomic.AddInt64(&c.queue.messagesAcknowledged, 1)
		}
	}

	return nil
}

// Stop gracefully stops the consumer.
func (c *Consumer) Stop() {
	c.cancel()
}

func (c *Consumer) shutdown(ctx context.Context) error {
	logger.Info(ctx, "consumer shutting down", "consumer_id", c.consumerID)

	// Stop recovery
	if c.recoveryTicker != nil {
		c.recoveryTicker.Stop()
	}

	// Wait for goroutines
	c.wg.Wait()

	logger.Info(ctx, "consumer shutdown complete", "consumer_id", c.consumerID)
	return nil
}

// GetPending returns information about pending messages for this consumer.
func (c *Consumer) GetPending(ctx context.Context) ([]PendingMessage, error) {
	pending, err := c.queue.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   c.queue.streamKey,
		Group:    c.queue.groupName,
		Consumer: c.consumerID,
		Start:    "-",
		End:      "+",
		Count:    100,
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to get pending: %w", err)
	}

	result := make([]PendingMessage, len(pending))
	for i, p := range pending {
		result[i] = PendingMessage{
			ID:            p.ID,
			Consumer:      p.Consumer,
			IdleTime:      p.Idle,
			DeliveryCount: p.RetryCount,
		}
	}

	return result, nil
}

// GetStreamInfo returns information about the stream.
func (q *Queue) GetStreamInfo(ctx context.Context) (*StreamInfo, error) {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return nil, fmt.Errorf("queue is closed")
	}
	q.mu.RUnlock()

	info, err := q.client.XInfoStream(ctx, q.streamKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get stream info: %w", err)
	}

	groups, err := q.client.XInfoGroups(ctx, q.streamKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get groups info: %w", err)
	}

	// Calculate total pending
	var totalPending int64
	for _, group := range groups {
		totalPending += group.Pending
	}

	streamInfo := &StreamInfo{
		Length:          info.Length,
		Groups:          int64(len(groups)),
		PendingMessages: totalPending,
	}

	if info.FirstEntry.ID != "" {
		streamInfo.FirstEntryID = info.FirstEntry.ID
	}

	if info.LastEntry.ID != "" {
		streamInfo.LastEntryID = info.LastEntry.ID
	}

	return streamInfo, nil
}

// GetMetrics returns queue metrics.
func (q *Queue) GetMetrics() map[string]interface{} {
	q.metricsMu.RLock()
	defer q.metricsMu.RUnlock()

	return map[string]interface{}{
		"messages_published":    atomic.LoadInt64(&q.messagesPublished),
		"messages_consumed":     atomic.LoadInt64(&q.messagesConsumed),
		"messages_acknowledged": atomic.LoadInt64(&q.messagesAcknowledged),
		"messages_failed":       atomic.LoadInt64(&q.messagesFailed),
		"last_publish_time":     q.lastPublishTime.Unix(),
		"last_consume_time":     q.lastConsumeTime.Unix(),
	}
}

// DeleteMessage removes a message from the stream.
func (q *Queue) DeleteMessage(ctx context.Context, messageID string) error {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return fmt.Errorf("queue is closed")
	}
	q.mu.RUnlock()

	return q.client.XDel(ctx, q.streamKey, messageID).Err()
}

// TrimStream trims the stream to the specified length.
func (q *Queue) TrimStream(ctx context.Context, maxLen int64, approx bool) (int64, error) {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return 0, fmt.Errorf("queue is closed")
	}
	q.mu.RUnlock()

	var deleted int64
	var err error

	if approx {
		deleted, err = q.client.XTrimMaxLenApprox(ctx, q.streamKey, maxLen, 0).Result()
	} else {
		deleted, err = q.client.XTrimMaxLen(ctx, q.streamKey, maxLen).Result()
	}

	if err != nil {
		return 0, fmt.Errorf("failed to trim stream: %w", err)
	}

	logger.Info(ctx, "stream trimmed",
		"stream", q.streamKey,
		"deleted", deleted,
		"max_len", maxLen)

	return deleted, nil
}

// DestroyConsumerGroup removes a consumer group.
func (q *Queue) DestroyConsumerGroup(ctx context.Context) error {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return fmt.Errorf("queue is closed")
	}
	q.mu.RUnlock()

	err := q.client.XGroupDestroy(ctx, q.streamKey, q.groupName).Err()
	if err != nil {
		return fmt.Errorf("failed to destroy consumer group: %w", err)
	}

	logger.Info(ctx, "consumer group destroyed",
		"stream", q.streamKey,
		"group", q.groupName)

	return nil
}

// Close closes the queue and releases resources.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return fmt.Errorf("queue already closed")
	}

	q.closed = true

	// Note: We don't close the Redis client as it may be shared
	logger.Info(context.Background(), "queue closed",
		"stream", q.streamKey)

	return nil
}

// Health checks the health of the queue.
func (q *Queue) Health(ctx context.Context) error {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return fmt.Errorf("queue is closed")
	}
	q.mu.RUnlock()

	// Ping Redis
	if err := q.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis health check failed: %w", err)
	}

	return nil
}

// Export exports queue metrics as JSON string.
func (q *Queue) Export() (string, error) {
	metrics := q.GetMetrics()
	data, err := json.Marshal(metrics)
	if err != nil {
		return "", fmt.Errorf("failed to marshal metrics: %w", err)
	}
	return string(data), nil
}
