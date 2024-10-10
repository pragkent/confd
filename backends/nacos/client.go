package nacos

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/kelseyhightower/confd/log"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

const (
	keyPrefixNaming = "/naming/"
	keyPrefixConfig = "/config/"
)

type keySpace int

const (
	keySpaceConfig keySpace = iota
	keySpaceNaming
)

var replacer = strings.NewReplacer("/", ".")

// NacosClient provides a backend client for Nacos.
type NacosClient struct {
	mu      sync.Mutex
	group   string
	nc      naming_client.INamingClient
	cc      config_client.IConfigClient
	watches []*nacosWatch
}

// NacosConfig holds the configuration for a NacosClient.
type NacosConfig struct {
	Nodes     []string
	Namespace string
	Username  string
	Password  string
	Group     string
}

// New creates a new NacosClient.
func New(config NacosConfig) (*NacosClient, error) {
	group := config.Group
	if strings.TrimSpace(group) == "" {
		group = "DEFAULT_GROUP"
	}

	if len(config.Nodes) == 0 {
		return nil, fmt.Errorf("no nacos nodes provided")
	}

	log.Info("Nacos backend nodes: %v namespace: %s group: %s username: %s", config.Nodes, config.Namespace, group, config.Username)

	clientConfig := *constant.NewClientConfig(
		constant.WithNamespaceId(config.Namespace),
		constant.WithUsername(config.Username),
		constant.WithPassword(config.Password),
	)

	serverConfigs, err := newServerConfigs(config)
	if err != nil {
		return nil, err
	}

	namingClient, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  &clientConfig,
			ServerConfigs: serverConfigs,
		})
	if err != nil {
		return nil, err
	}

	configClient, err := clients.NewConfigClient(
		vo.NacosClientParam{
			ClientConfig:  &clientConfig,
			ServerConfigs: serverConfigs,
		})
	if err != nil {
		return nil, err
	}

	client := &NacosClient{
		group: group,
		nc:    namingClient,
		cc:    configClient,
	}

	return client, nil
}

func newServerConfigs(config NacosConfig) ([]constant.ServerConfig, error) {
	serverConfigs := make([]constant.ServerConfig, 0, len(config.Nodes))

	for _, node := range config.Nodes {
		host, port, err := net.SplitHostPort(node)
		if err != nil {
			return nil, fmt.Errorf("invalid nacos node address: %s: %v", node, err)
		}

		portNum, err := strconv.Atoi(port)
		if err != nil {
			return nil, fmt.Errorf("invalid nacos node port: %s: %v", port, err)
		}

		serverConfigs = append(serverConfigs, *constant.NewServerConfig(host, uint64(portNum)))
	}

	return serverConfigs, nil
}

func (c *NacosClient) GetValues(keys []string) (map[string]string, error) {
	values := make(map[string]string)
	for _, k := range keys {
		space, key := parseKey(k)
		value, err := c.getValue(space, key)
		if err != nil {
			return nil, err
		}

		values[k] = value
	}

	return values, nil
}

func parseKey(key string) (keySpace, string) {
	prefix := "/"
	space := keySpaceConfig
	if strings.HasPrefix(key, keyPrefixNaming) {
		prefix = keyPrefixNaming
		space = keySpaceNaming
	} else if strings.HasPrefix(key, keyPrefixConfig) {
		prefix = keyPrefixConfig
		space = keySpaceConfig
	}

	key = strings.TrimPrefix(key, prefix)
	return space, replacer.Replace(key)
}
func (c *NacosClient) getValue(space keySpace, key string) (string, error) {
	if space == keySpaceNaming {
		return c.getNaming(key)
	}

	return c.getConfig(key)
}

func (c *NacosClient) getNaming(key string) (string, error) {
	instances, err := c.nc.SelectInstances(vo.SelectInstancesParam{
		ServiceName: key,
		GroupName:   c.group,
		HealthyOnly: true,
	})
	if err != nil {
		log.Error("Select instances error. key: %v err: %v", key, err)
		return "", err
	}

	log.Info("Select instances finished. key: %v instances: %+v", key, instances)

	if len(instances) == 0 {
		return "", errors.New("no instances available")
	}

	service := nacosService{Instances: make([]nacosInstance, 0, len(instances))}
	for _, inst := range instances {
		service.Instances = append(service.Instances, nacosInstance{
			InstanceId:  inst.InstanceId,
			IP:          inst.Ip,
			Port:        int(inst.Port),
			Weight:      inst.Weight,
			Healthy:     inst.Healthy,
			Enable:      inst.Enable,
			ClusterName: inst.ClusterName,
			ServiceName: inst.ServiceName,
			Metadata:    inst.Metadata,
		})
	}

	data, err := json.Marshal(service)
	if err != nil {
		log.Error("Marshal json error. key: %v val: %v err: %v", key, instances, err)
		return "", err
	}

	return string(data), nil
}

func (c *NacosClient) getConfig(key string) (string, error) {
	data, err := c.cc.GetConfig(vo.ConfigParam{
		DataId: key,
		Group:  c.group,
	})
	if err != nil {
		log.Error("Get config error. key: %v err: %v", key, err)
		return "", err
	}

	log.Info("Get config finished. key: %v, value: %v", key, data)
	return data, nil
}

func (c *NacosClient) WatchPrefix(prefix string, keys []string, waitIndex uint64, stopChan chan bool) (uint64, error) {
	if waitIndex == 0 {
		if err := c.watch(keys, waitIndex); err != nil {
			return 0, err
		}

		return 1, nil
	}

	watch := newNacosWatch(keys)
	c.addWatch(watch)

	for {
		select {
		case <-stopChan:
			return waitIndex, nil
		case r := <-watch.ch:
			return r.waitIndex, r.err
		}
	}
}

func (c *NacosClient) watch(keys []string, waitIndex uint64) error {
	respChan := make(chan watchResponse, 1)

	for _, k := range keys {
		space, key := parseKey(k)
		if err := c.watchKey(space, key, respChan); err != nil {
			return err
		}
	}

	go func() {
		for r := range respChan {
			c.notifyWatches(r)
		}
	}()

	return nil
}

func (c *NacosClient) watchKey(space keySpace, key string, respChan chan watchResponse) error {
	if space == keySpaceNaming {
		return c.watchNaming(key, respChan)
	}

	return c.watchConfig(key, respChan)
}

func (c *NacosClient) watchNaming(key string, respChan chan watchResponse) error {
	param := &vo.SubscribeParam{
		ServiceName: key,
		GroupName:   c.group,
		SubscribeCallback: func(services []model.Instance, err error) {
			log.Info("Naming change occurred. group: %v key: %v services: %+v err: %v", c.group, key, services, err)

			if err != nil {
				respChan <- watchResponse{err: err}
				return
			}

			respChan <- watchResponse{waitIndex: 1, err: nil}
		},
	}

	err := c.nc.Subscribe(param)
	if err != nil {
		log.Error("Subscribe naming change event error. group: %v key: %v err: %v", c.group, key, err)
		return err
	}

	log.Info("Subscribe naming change event finished. group: %v key: %v", c.group, key)
	return nil
}

func (c *NacosClient) watchConfig(key string, respChan chan watchResponse) error {
	param := vo.ConfigParam{
		DataId: key,
		Group:  c.group,
		OnChange: func(namespace, group, dataId, data string) {
			log.Info("Config change occurred. namespace: %v group: %v key: %v data: %v", namespace, c.group, key, data)
			respChan <- watchResponse{waitIndex: 1, err: nil}
		},
	}

	err := c.cc.ListenConfig(param)
	if err != nil {
		log.Error("Listen config change event error. group: %v key: %v err: %v", c.group, key, err)
		return err
	}

	log.Info("Listen config change event finished. group: %v key: %v", c.group, key)
	return nil
}

func (c *NacosClient) addWatch(watch *nacosWatch) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.watches = append(c.watches, watch)
}

func (c *NacosClient) notifyWatches(r watchResponse) {
	c.mu.Lock()
	watches := c.watches
	c.watches = nil
	c.mu.Unlock()

	for _, w := range watches {
		w.ch <- r
	}
}

type watchResponse struct {
	waitIndex uint64
	err       error
}

type nacosWatch struct {
	keys []string
	ch   chan watchResponse
}

func (w *nacosWatch) HasKey(key string) bool {
	for _, k := range w.keys {
		if k == key {
			return true
		}
	}

	return false
}

func newNacosWatch(keys []string) *nacosWatch {
	return &nacosWatch{
		keys: keys,
		ch:   make(chan watchResponse, 1),
	}
}

type nacosInstance struct {
	InstanceId  string            `json:"instanceId"`
	IP          string            `json:"ip"`
	Port        int               `json:"port"`
	Weight      float64           `json:"weight"`
	Healthy     bool              `json:"healthy"`
	Enable      bool              `json:"enabled"`
	ClusterName string            `json:"clusterName"`
	ServiceName string            `json:"serviceName"`
	Metadata    map[string]string `json:"metadata"`
}

type nacosService struct {
	Instances []nacosInstance `json:"instances"`
}
