package punt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
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

func (a *Action) Run(infos []*AlertInfo) {
	a.impl.Run(infos)
}

func NewAction(state *State, name string, config ActionConfig) *Action {
	action := Action{
		impl: GetActionImplementation(config.Type, config.Config),
	}

	if action.impl == nil {
		log.Fatalf("Invalid Action: `%s`", config.Type)
	}

	return &action
}

type ActionImpl interface {
	Run([]*AlertInfo)
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
	log.Printf("wat: %v", err)
	defer resp.Body.Close()
	return err
}

func (dwa *DiscordWebhookAction) Run(infos []*AlertInfo) {
	embed := Embed{
		Title:       infos[0].Title,
		Description: infos[0].Description,
		Color:       dwa.Color,
		Timestamp:   infos[0].Log["timestamp"].(time.Time).Format(time.RFC3339),
	}

	if len(infos) > 1 {
		embed.Title = embed.Title + fmt.Sprintf(" (%v similar events)", len(infos)-1)
	}

	for name, value := range infos[0].Fields {
		embed.AddField(name, value, false)
	}

	dwa.send(MessagePayload{Embeds: []Embed{embed}})
}
