package punt

import (
	"bytes"
	"regexp"
	"sync"
	"text/template"
	"time"
)

type AlertConfig struct {
	Type         string                 `json:"type"`
	Sources      []string               `json:"sources"`
	Action       string                 `json:"action"`
	Config       map[string]interface{} `json:"config"`
	RollupWindow int                    `json:"rollup_window"`
}

type alertWindow struct {
	infos []*AlertInfo
}

type Alert struct {
	Name   string
	Action *Action
	State  *State
	Config AlertConfig

	windowLock sync.Mutex
	window     *alertWindow

	impl AlertImpl
}

type AlertInfo struct {
	Title       string
	Description string
	Fields      map[string]string
	Log         map[string]interface{}
}

func (a *Alert) emitWindow() {
	time.Sleep(time.Duration(a.Config.RollupWindow) * time.Second)
	a.windowLock.Lock()
	defer a.windowLock.Unlock()
	a.Action.Run(a.window.infos)
	a.window = nil
}

func (a *Alert) Emit(info *AlertInfo) {
	if a.Config.RollupWindow != 0 {
		a.windowLock.Lock()
		defer a.windowLock.Unlock()

		if a.window != nil {
			a.window.infos = append(a.window.infos, info)
		} else {
			a.window = &alertWindow{infos: []*AlertInfo{info}}
			go a.emitWindow()
		}
		return
	} else {
		a.Action.Run([]*AlertInfo{info})
	}
}

func (a *Alert) Run(data map[string]interface{}) {
	alertInfo := a.impl.Check(data)
	if alertInfo == nil {
		return
	}

	alertInfo.Log = data
	a.Emit(alertInfo)
}

func NewAlert(state *State, name string, config AlertConfig) *Alert {
	return &Alert{
		Name:   name,
		Action: state.Actions[config.Action],
		Config: config,
		State:  state,
		impl:   GetAlertImplementation(config.Type, config.Config),
	}
}

type AlertImpl interface {
	Check(map[string]interface{}) *AlertInfo
}

func GetAlertImplementation(name string, config map[string]interface{}) AlertImpl {
	switch name {
	case "match":
		return NewMatchAlert(config)
	default:
		return nil
	}
}

type MatchAlert struct {
	title       *template.Template
	description *template.Template
	fields      map[string]*template.Template

	source string
	match  *regexp.Regexp
}

func (ma *MatchAlert) Check(data map[string]interface{}) *AlertInfo {
	if ma.match.Match([]byte(data[ma.source].(string))) {
		var buf bytes.Buffer

		info := AlertInfo{Fields: make(map[string]string)}
		ma.title.Execute(&buf, data)
		info.Title = buf.String()
		buf.Reset()
		ma.description.Execute(&buf, data)
		info.Description = buf.String()

		for k, v := range ma.fields {
			buf.Reset()
			v.Execute(&buf, data)
			info.Fields[k] = buf.String()
		}

		return &info
	}

	return nil
}

func NewMatchAlert(config map[string]interface{}) *MatchAlert {
	fields := make(map[string]*template.Template)

	for k, v := range config["fields"].(map[string]interface{}) {
		fields[k] = template.Must(template.New(k).Parse(v.(string)))
	}

	return &MatchAlert{
		title:       template.Must(template.New("title").Parse(config["title"].(string))),
		description: template.Must(template.New("description").Parse(config["description"].(string))),
		fields:      fields,
		source:      config["source"].(string),
		match:       regexp.MustCompile(config["match"].(string)),
	}
}
