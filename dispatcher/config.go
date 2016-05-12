package dispatcher

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	DataPath string            `json:dataPath`
	Sync     bool              `json:sync`
	Host     string            `json:host`
	Port     int               `json:port`
	Rules    map[string]*Rule `json:rules`
}

type Rule struct {
	Type        string  `json:type`
	HandlerType string  `json:handlerType`
	HandlerName string `json:handlerName`
	HandlerUrl string `json:handlerUrl`
	TryCount    uint8   `json:tryCount`
	Timeout     float64 `timeout`
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, err
	}

	return NewConfig(data)
}

func NewConfig(data []byte) (*Config, error) {
	var c Config
	err := json.Unmarshal(data, &c)
	if err != nil {
		return nil, err
	}

	return &c, nil
}
