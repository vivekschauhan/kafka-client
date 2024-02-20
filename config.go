package main

type Config struct {
	TopicName        string `mapstructure:"topic"`
	GroupId          string `mapstructure:"group_id"`
	BootstrapServers string `mapstructure:"bootstrap_server"`
	SecurityProtocol string `mapstructure:"security_protocol"`
	SASLMechanism    string `mapstructure:"sasl_mechanism"`
	SASLUser         string `mapstructure:"sasl_user"`
	SASLPassword     string `mapstructure:"sasl_password"`
}

// User is a simple record example
type User struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// Order
type Order struct {
	ID          int64  `json:"id"`
	Amount      int64  `json:"name"`
	Description string `json:"email"`
}
