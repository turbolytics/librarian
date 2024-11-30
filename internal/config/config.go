package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Logger struct {
	Level string `yaml:"level"`
}

type Global struct {
	Logger Logger `yaml:"logger"`
}

type Archiver struct {
	Name   string `yaml:"name"`
	Source Source `yaml:"source"`
}

type Source struct {
	ConnectionString string `yaml:"connection_string"`
	Schema           string `yaml:"schema"`
	Table            string `yaml:"table"`
}

type Collector struct {
	Type string `yaml:"type"`
}

type S3 struct {
	Bucket string `yaml:"bucket"`
	Region string `yaml:"region"`
}

type Repository struct {
	Bucket   string `yaml:"bucket"`
	Region   string `yaml:"region"`
	Prefix   string `yaml:"prefix"`
	Endpoint string `yaml:"endpoint"`
}

type Preserver struct {
	Type      string `yaml:"type"`
	BatchSize int    `yaml:"batch_size"`
	Schema    string `yaml:"schema"`
}

type Librarian struct {
	Global   Global   `yaml:"global"`
	Archiver Archiver `yaml:"archiver"`
}

func NewLibrarianFromFile(fpath string) (*Librarian, error) {
	bs, err := os.ReadFile(fpath)
	if err != nil {
		return nil, err
	}

	var librarian Librarian
	if err := yaml.Unmarshal(bs, &librarian); err != nil {
		return nil, err
	}

	return &librarian, nil
}
