package connectors

type (

	connector struct {
		Id    string  `json:"id"`
		Type  string  `json:"type"`
	}

	Connector interface {
		Close()
		GetId() (string)
		GetType() (string)
	}

	SourceConnector interface {
		Connector
		Publish(message interface{})
	}

)
