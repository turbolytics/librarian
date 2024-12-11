package config

import (
	"os"

	"github.com/turbolytics/librarian/internal/parquet"
	"gopkg.in/yaml.v3"
)

type Logger struct {
	Level string `yaml:"level"`
}

type Global struct {
	Logger Logger `yaml:"logger"`
}

type Archiver struct {
	Name       string     `yaml:"name"`
	Source     Source     `yaml:"source"`
	Preserver  Preserver  `yaml:"preserver"`
	Repository Repository `yaml:"repository"`
}

type Source struct {
	ConnectionString string `yaml:"connection_string"`
	Schema           string `yaml:"schema"`
	Table            string `yaml:"table"`
	Query            string `yaml:"query"`
}

type Repository struct {
	Type        string `yaml:"type"`
	LocalConfig struct {
		Path string `yaml:"path"`
	} `yaml:"local"`

	S3Config struct {
		Bucket         string `yaml:"bucket"`
		Region         string `yaml:"region"`
		Prefix         string `yaml:"prefix"`
		Endpoint       string `yaml:"endpoint"`
		ForcePathStyle bool   `yaml:"force_path_style"`
	} `yaml:"s3"`
}

type Field struct {
	Name           string `yaml:"name"`
	Type           string `yaml:"type"`
	ConvertedType  string `yaml:"converted_type"`
	RepetitionType string `yaml:"repetition_type"`
	Scale          *int   `yaml:"scale"`
	Precision      *int   `yaml:"precision"`
	Length         *int   `yaml:"length"`
}

type Preserver struct {
	Type string `yaml:"type"`

	// BatchSizeNumRecords specifies the number of records
	// that will be contained in each file part.
	BatchSizeNumRecords int `yaml:"batch_size_num_records"`
	Parquet             struct {
		Schema []Field `yaml:"schema"`
	} `yaml:"parquet"`
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

func ParquetFields(fields []Field) []parquet.Field {
	parquetFields := make([]parquet.Field, len(fields))
	for i, field := range fields {
		pf := parquet.Field{
			Name:      field.Name,
			Type:      field.Type,
			Scale:     field.Scale,
			Precision: field.Precision,
			Length:    field.Length,
		}
		if field.ConvertedType != "" {
			pf.ConvertedType = field.ConvertedType
		}
		if field.RepetitionType != "" {
			pf.RepetitionType = field.RepetitionType
		}

		parquetFields[i] = pf
	}

	return parquetFields
}
