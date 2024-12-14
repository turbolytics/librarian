package schema

import (
	"database/sql"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbolytics/librarian/internal/parquet"
	"go.uber.org/zap"
	"net/url"
)

func newGenerateCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "generate",
		Short: "Generates a parquet schema from a database table",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			logger, _ := zap.NewDevelopment()
			l := logger.Named("schema.generate")
			uri := viper.GetString("uri")
			l.Info(
				"librarian schema generate!",
				// zap.String("uri", uri),
				zap.String("schema", viper.GetString("schema")),
				zap.String("table", viper.GetString("table")),
			)

			u, err := url.Parse(uri)
			if err != nil {
				return err
			}

			db, err := sql.Open("pgx", u.String())
			if err != nil {
				return err
			}

			defer db.Close()

			if err := db.PingContext(ctx); err != nil {
				return err
			}

			rows, err := db.QueryContext(
				ctx,
				`
SELECT 
	column_name, 
	data_type, 
	character_maximum_length, 
	column_default, 
	is_nullable 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE table_name = $1
ORDER BY column_name`,
				viper.GetString("table"),
			)

			if err != nil {
				return err
			}
			defer rows.Close()

			var columns []parquet.Column
			for rows.Next() {
				var c parquet.Column
				if err := rows.Scan(
					&c.Name,
					&c.DataType,
					&c.CharacterMaximumLength,
					&c.ColumnDefault,
					&c.IsNullable,
				); err != nil {
					return err
				}
				columns = append(columns, c)
			}

			s, err := parquet.ColumnsToSchema(columns)
			if err != nil {
				return err
			}

			fmt.Println(s.ToGoParquetSchema())

			return nil
		},
	}

	cmd.PersistentFlags().StringP("uri", "u", "", "URI to connect to the database")
	cmd.PersistentFlags().StringP("schema", "s", "public", "Schema to generate a parquet schema for")
	cmd.PersistentFlags().StringP("table", "t", "", "Table to generate a parquet schema for")
	cmd.PersistentFlags().StringP("strategy", "", "information_schema", "The strategy to use to generate the schema")
	viper.BindPFlag("uri", cmd.PersistentFlags().Lookup("uri"))
	viper.BindPFlag("table", cmd.PersistentFlags().Lookup("table"))
	viper.BindPFlag("schema", cmd.PersistentFlags().Lookup("schema"))
	viper.AutomaticEnv()
	viper.SetEnvPrefix("LIBRARIAN")
	return cmd
}
