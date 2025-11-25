package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/anuranpaul/task-scheduler/pkg/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Coordinator struct {
	client     *clientv3.Client
	instanceID string
	namespace  string
	mu         sync.RWMutex
}

type LeaderElection struct {
	coord      *Coordinator
	session    *concurrency.Session
	election   *concurrency.Election
	electionID string
	callbacks  LeaderCallbacks
	mu         sync.RWMutex
	isLeader   bool
}

type LeaderCallbacks struct {
	OnElected  func(ctx context.Context) error
	OnRevoked  func(ctx context.Context) error
	OnError    func(ctx context.Context, err error)
}

type DistributedLock struct {
	coord   *Coordinator
	session *concurrency.Session
	mutex   *concurrency.Mutex
	lockKey string
}

type ServiceRegistry struct {
	coord      *Coordinator
	lease      clientv3.LeaseID
	serviceKey string
	metadata   map[string]string
	ttl        int64
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

// CoordinatorConfig contains configuration for the coordinator.
type CoordinatorConfig struct {
	Endpoints   []string
	DialTimeout time.Duration
	InstanceID  string
	Namespace   string // Prefix for all etcd keys (e.g., "/task-scheduler")
}

// NewCoordinator creates a new distributed coordinator instance.
func NewCoordinator(ctx context.Context, cfg CoordinatorConfig) (*Coordinator, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	// Verify connectivity
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	
	if _, err := client.Status(timeoutCtx, cfg.Endpoints[0]); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	namespace := cfg.Namespace
	if namespace == "" {
		namespace = "/task-scheduler"
	}

	logger.Info(ctx, "coordinator initialized", 
		"instance_id", cfg.InstanceID,
		"namespace", namespace,
		"endpoints", cfg.Endpoints)

	return &Coordinator{
		client:     client,
		instanceID: cfg.InstanceID,
		namespace:  namespace,
	}, nil
}

// NewLeaderElection creates a new leader election instance.
// The session TTL determines how long the leader lease lasts without renewal.
func (c *Coordinator) NewLeaderElection(ctx context.Context, electionID string, ttl int, callbacks LeaderCallbacks) (*LeaderElection, error) {
	session, err := concurrency.NewSession(c.client, concurrency.WithTTL(ttl))
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	electionKey := fmt.Sprintf("%s/elections/%s", c.namespace, electionID)
	election := concurrency.NewElection(session, electionKey)

	logger.Info(ctx, "leader election created", 
		"election_id", electionID,
		"ttl", ttl,
		"instance_id", c.instanceID)

	return &LeaderElection{
		coord:      c,
		session:    session,
		election:   election,
		electionID: electionID,
		callbacks:  callbacks,
	}, nil
}

// Campaign starts campaigning for leadership. Blocks until elected or context cancelled.
func (le *LeaderElection) Campaign(ctx context.Context) error {
	logger.Info(ctx, "starting leadership campaign", 
		"election_id", le.electionID,
		"instance_id", le.coord.instanceID)

	if err := le.election.Campaign(ctx, le.coord.instanceID); err != nil {
		return fmt.Errorf("campaign failed: %w", err)
	}

	le.setLeader(true)
	logger.Info(ctx, "elected as leader", 
		"election_id", le.electionID,
		"instance_id", le.coord.instanceID)

	if le.callbacks.OnElected != nil {
		if err := le.callbacks.OnElected(ctx); err != nil {
			logger.Error(ctx, "OnElected callback failed", err)
			// Continue despite callback error
		}
	}

	return nil
}

// Observe watches leadership changes and handles transitions.
// This should be called in a separate goroutine after Campaign succeeds.
func (le *LeaderElection) Observe(ctx context.Context) {
	observeCh := le.election.Observe(ctx)
	
	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "leadership observation stopped", "election_id", le.electionID)
			return
		case <-le.session.Done():
			le.handleLeadershipLoss(ctx)
			return
		case resp, ok := <-observeCh:
			if !ok {
				logger.Warn(ctx, "observation channel closed", "election_id", le.electionID)
				le.handleLeadershipLoss(ctx)
				return
			}
			
			if len(resp.Kvs) > 0 {
				leaderID := string(resp.Kvs[0].Value)
				isCurrentLeader := leaderID == le.coord.instanceID
				
				if isCurrentLeader != le.IsLeader() {
					logger.Info(ctx, "leadership changed",
						"election_id", le.electionID,
						"leader_id", leaderID,
						"is_leader", isCurrentLeader)
					
					if isCurrentLeader {
						le.setLeader(true)
						if le.callbacks.OnElected != nil {
							if err := le.callbacks.OnElected(ctx); err != nil {
								logger.Error(ctx, "OnElected callback failed", err)
							}
						}
					} else {
						le.handleLeadershipLoss(ctx)
					}
				}
			}
		}
	}
}

func (le *LeaderElection) handleLeadershipLoss(ctx context.Context) {
	if !le.IsLeader() {
		return // Already handled
	}

	le.setLeader(false)
	logger.Info(ctx, "lost leadership", 
		"election_id", le.electionID,
		"instance_id", le.coord.instanceID)

	if le.callbacks.OnRevoked != nil {
		if err := le.callbacks.OnRevoked(ctx); err != nil {
			logger.Error(ctx, "OnRevoked callback failed", err)
		}
	}
}

func (le *LeaderElection) Resign(ctx context.Context) error {
	if !le.IsLeader() {
		return nil
	}

	logger.Info(ctx, "resigning from leadership", 
		"election_id", le.electionID,
		"instance_id", le.coord.instanceID)

	if err := le.election.Resign(ctx); err != nil {
		return fmt.Errorf("failed to resign: %w", err)
	}

	le.setLeader(false)
	if le.callbacks.OnRevoked != nil {
		if err := le.callbacks.OnRevoked(ctx); err != nil {
			logger.Error(ctx, "OnRevoked callback failed", err)
		}
	}

	return nil
}

func (le *LeaderElection) IsLeader() bool {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.isLeader
}

func (le *LeaderElection) setLeader(leader bool) {
	le.mu.Lock()
	defer le.mu.Unlock()
	le.isLeader = leader
}

func (le *LeaderElection) GetLeader(ctx context.Context) (string, error) {
	resp, err := le.election.Leader(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get leader: %w", err)
	}
	
	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("no leader elected")
	}
	
	return string(resp.Kvs[0].Value), nil
}

func (le *LeaderElection) Close() error {
	if le.session != nil {
		return le.session.Close()
	}
	return nil
}

func (c *Coordinator) NewDistributedLock(ctx context.Context, lockName string, ttl int) (*DistributedLock, error) {
	session, err := concurrency.NewSession(c.client, concurrency.WithTTL(ttl))
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	lockKey := fmt.Sprintf("%s/locks/%s", c.namespace, lockName)
	mutex := concurrency.NewMutex(session, lockKey)

	logger.Debug(ctx, "distributed lock created",
		"lock_name", lockName,
		"ttl", ttl,
		"instance_id", c.instanceID)

	return &DistributedLock{
		coord:   c,
		session: session,
		mutex:   mutex,
		lockKey: lockKey,
	}, nil
}

func (dl *DistributedLock) Lock(ctx context.Context) error {
	logger.Debug(ctx, "acquiring lock", "lock_key", dl.lockKey)
	
	if err := dl.mutex.Lock(ctx); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	
	logger.Debug(ctx, "lock acquired", "lock_key", dl.lockKey)
	return nil
}

func (dl *DistributedLock) TryLock(ctx context.Context) (bool, error) {
	tryCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	err := dl.mutex.Lock(tryCtx)
	if err != nil {
		if err == context.DeadlineExceeded {
			return false, nil 
		}
		return false, fmt.Errorf("failed to try lock: %w", err)
	}

	logger.Debug(ctx, "lock acquired (try)", "lock_key", dl.lockKey)
	return true, nil
}

func (dl *DistributedLock) Unlock(ctx context.Context) error {
	logger.Debug(ctx, "releasing lock", "lock_key", dl.lockKey)
	
	if err := dl.mutex.Unlock(ctx); err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	
	logger.Debug(ctx, "lock released", "lock_key", dl.lockKey)
	return nil
}

func (dl *DistributedLock) Close() error {
	if dl.session != nil {
		return dl.session.Close()
	}
	return nil
}

func (c *Coordinator) NewServiceRegistry(ctx context.Context, serviceName string, metadata map[string]string, ttl int64) (*ServiceRegistry, error) {
	lease, err := c.client.Grant(ctx, ttl)
	if err != nil {
		return nil, fmt.Errorf("failed to create lease: %w", err)
	}

	serviceKey := fmt.Sprintf("%s/services/%s/%s", c.namespace, serviceName, c.instanceID)

	sr := &ServiceRegistry{
		coord:      c,
		lease:      lease.ID,
		serviceKey: serviceKey,
		metadata:   metadata,
		ttl:        ttl,
	}

	if err := sr.register(ctx); err != nil {
		c.client.Revoke(ctx, lease.ID)
		return nil, fmt.Errorf("failed to register service: %w", err)
	}

	keepAliveCtx, cancel := context.WithCancel(context.Background())
	sr.cancelFunc = cancel

	sr.wg.Add(1)
	go sr.keepAlive(keepAliveCtx)

	logger.Info(ctx, "service registered",
		"service_name", serviceName,
		"instance_id", c.instanceID,
		"ttl", ttl)

	return sr, nil
}

func (sr *ServiceRegistry) register(ctx context.Context) error {
	metadataJSON := "{"
	first := true
	for k, v := range sr.metadata {
		if !first {
			metadataJSON += ","
		}
		metadataJSON += fmt.Sprintf(`"%s":"%s"`, k, v)
		first = false
	}
	metadataJSON += "}"

	_, err := sr.coord.client.Put(ctx, sr.serviceKey, metadataJSON, clientv3.WithLease(sr.lease))
	return err
}

func (sr *ServiceRegistry) keepAlive(ctx context.Context) {
	defer sr.wg.Done()

	keepAliveChan, err := sr.coord.client.KeepAlive(ctx, sr.lease)
	if err != nil {
		logger.Error(ctx, "failed to start keep-alive", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "service keep-alive stopped", "service_key", sr.serviceKey)
			return
		case ka, ok := <-keepAliveChan:
			if !ok {
				logger.Warn(ctx, "keep-alive channel closed", "service_key", sr.serviceKey)
				return
			}
			if ka == nil {
				logger.Warn(ctx, "keep-alive response is nil", "service_key", sr.serviceKey)
				return
			}
			logger.Debug(ctx, "keep-alive renewed", "service_key", sr.serviceKey, "ttl", ka.TTL)
		}
	}
}

func (sr *ServiceRegistry) Deregister(ctx context.Context) error {
	if sr.cancelFunc != nil {
		sr.cancelFunc()
	}

	sr.wg.Wait()

	if _, err := sr.coord.client.Revoke(ctx, sr.lease); err != nil {
		logger.Error(ctx, "failed to revoke lease", err)
	}

	if _, err := sr.coord.client.Delete(ctx, sr.serviceKey); err != nil {
		return fmt.Errorf("failed to deregister service: %w", err)
	}

	logger.Info(ctx, "service deregistered", "service_key", sr.serviceKey)
	return nil
}

func (c *Coordinator) DiscoverServices(ctx context.Context, serviceName string) ([]ServiceInstance, error) {
	prefix := fmt.Sprintf("%s/services/%s/", c.namespace, serviceName)
	
	resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to discover services: %w", err)
	}

	instances := make([]ServiceInstance, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		instanceID := key[len(prefix):]
		
		instances = append(instances, ServiceInstance{
			InstanceID: instanceID,
			Metadata:   string(kv.Value),
		})
	}

	return instances, nil
}

func (c *Coordinator) WatchServices(ctx context.Context, serviceName string, callback func(event ServiceEvent)) {
	prefix := fmt.Sprintf("%s/services/%s/", c.namespace, serviceName)
	
	watchChan := c.client.Watch(ctx, prefix, clientv3.WithPrefix())
	
	for {
		select {
		case <-ctx.Done():
			return
		case watchResp, ok := <-watchChan:
			if !ok {
				logger.Warn(ctx, "watch channel closed", "service_name", serviceName)
				return
			}
			
			for _, event := range watchResp.Events {
				key := string(event.Kv.Key)
				instanceID := key[len(prefix):]
				
				serviceEvent := ServiceEvent{
					InstanceID: instanceID,
					Metadata:   string(event.Kv.Value),
				}
				
				switch event.Type {
				case clientv3.EventTypePut:
					serviceEvent.Type = ServiceEventTypeAdded
				case clientv3.EventTypeDelete:
					serviceEvent.Type = ServiceEventTypeRemoved
				}
				
				callback(serviceEvent)
			}
		}
	}
}

type ServiceInstance struct {
	InstanceID string
	Metadata   string
}

type ServiceEvent struct {
	Type       ServiceEventType
	InstanceID string
	Metadata   string
}

type ServiceEventType int

const (
	ServiceEventTypeAdded ServiceEventType = iota
	ServiceEventTypeRemoved
)

func (c *Coordinator) GetKey(ctx context.Context, key string) (string, error) {
	fullKey := fmt.Sprintf("%s/%s", c.namespace, key)
	
	resp, err := c.client.Get(ctx, fullKey)
	if err != nil {
		return "", fmt.Errorf("failed to get key: %w", err)
	}
	
	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("key not found: %s", key)
	}
	
	return string(resp.Kvs[0].Value), nil
}

func (c *Coordinator) SetKey(ctx context.Context, key, value string) error {
	fullKey := fmt.Sprintf("%s/%s", c.namespace, key)
	
	_, err := c.client.Put(ctx, fullKey, value)
	if err != nil {
		return fmt.Errorf("failed to set key: %w", err)
	}
	
	return nil
}

func (c *Coordinator) DeleteKey(ctx context.Context, key string) error {
	fullKey := fmt.Sprintf("%s/%s", c.namespace, key)
	
	_, err := c.client.Delete(ctx, fullKey)
	if err != nil {
		return fmt.Errorf("failed to delete key: %w", err)
	}
	
	return nil
}

func (c *Coordinator) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.client != nil {
		return c.client.Close()
	}
	
	return nil
}

func (c *Coordinator) Health(ctx context.Context) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	
	_, err := c.client.Status(timeoutCtx, c.client.Endpoints()[0])
	return err
}