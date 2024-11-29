package config

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/librarian/internal/archiver"
	"github.com/turbolytics/librarian/internal/postgres"
)

func InitializeArchiver(librarian *Librarian) (*archiver.Archiver, error) {
	conn, err := pgx.Connect(
		context.Background(),
		librarian.Archiver.Source.ConnectionString,
	)
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, err
	}

	source := postgres.NewSource(conn,
		postgres.WithSchema(librarian.Archiver.Source.Schema),
		postgres.WithTable(librarian.Archiver.Source.Table),
	)

	return archiver.New(
		archiver.WithSource(source),
	), nil
}
