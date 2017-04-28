package punt

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"
)

type Action struct {
	Name string
	impl ActionImpl
}

type ActionConfig struct {
	Type   string                 `json:"type"`
	Config map[string]interface{} `json:"config"`
}

func (a *Action) Run(info *AlertInfo) {
	a.impl.Run(info)
}

func NewAction(state *State, name string, config ActionConfig) *Action {
	return &Action{
		impl: GetActionImplementation(config.Type, config.Config),
	}
}

type ActionImpl interface {
	Run(info *AlertInfo)
}

func GetActionImplementation(name string, config map[string]interface{}) ActionImpl {
	switch name {
	case "discord":
		return NewDiscordWebhookAction(config)
	default:
		return nil
	}
}

type MessagePayload struct {
	Embeds []Embed `json:"embeds"`
}

type Embed struct {
	Title       string       `json:"title"`
	Description string       `json:"description"`
	URL         string       `json:"url"`
	Color       uint         `json:"color"`
	Timestamp   string       `json:"timestamp,omitempty"`
	Fields      []EmbedField `json:"fields"`
}

func (e *Embed) AddField(name, value string, inline bool) {
	e.Fields = append(e.Fields, EmbedField{Name: name, Value: value, Inline: inline})
}

type EmbedField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline"`
}

type DiscordWebhookAction struct {
	URL   string
	Color uint
}

func NewDiscordWebhookAction(config map[string]interface{}) *DiscordWebhookAction {
	return &DiscordWebhookAction{
		URL:   config["url"].(string),
		Color: uint(config["color"].(float64)),
	}
}

func (dwa *DiscordWebhookAction) send(payload MessagePayload) (err error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	req, err := http.NewRequest("POST", dwa.URL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	defer resp.Body.Close()
	return err
}

func (dwa *DiscordWebhookAction) Run(info *AlertInfo) {
	embed := Embed{
		Title:       info.Title,
		Description: info.Description,
		Color:       dwa.Color,
		Timestamp:   info.Log["timestamp"].(time.Time).Format(time.RFC3339),
	}

	for name, value := range info.Fields {
		embed.AddField(name, value, false)
	}

	dwa.send(MessagePayload{Embeds: []Embed{embed}})
}
