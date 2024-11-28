package config

type Source struct {
	Type             string `yaml:"type"`
	ConnectionString string `yaml:"connection_string"`
}

type Collection struct {
	Name   string `yaml:"name"`
	Source Source `yaml:"source"`
}

type Config struct {
	Collections []Collection `yaml:"collections"`
}
