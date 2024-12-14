package schema

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbolytics/librarian/internal/config"
	"github.com/turbolytics/librarian/internal/parquet"
	"github.com/xwb1989/sqlparser"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

func newGenerateCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "generate",
		Short: "Generates a parquet schema from a database table",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, _ := zap.NewDevelopment()
			l := logger.Named("schema.generate")
			l.Info(
				"librarian schema generate!",
				// zap.String("uri", uri),
				zap.String("schema", viper.GetString("schema")),
				zap.String("table", viper.GetString("table")),
			)

			switch viper.GetString("db") {
			case "postgres":
				stmt, err := sqlparser.Parse(viper.GetString("query"))
				if err != nil {
					panic(err)
				}

				var s parquet.Schema
				create := stmt.(*sqlparser.DDL)
				for _, col := range create.TableSpec.Columns {
					f, err := parquet.PostgresSQLParserColumnToField(col)
					if err != nil {
						return err
					}
					s = append(s, f)
				}

				cfg := config.SchemaToConfigFields(s)
				bs, err := yaml.Marshal(cfg)
				if err != nil {
					return err
				}

				fmt.Println(string(bs))
			default:
				return fmt.Errorf("unsupported strategy: %q", viper.GetString("strategy"))
			}

			return nil
		},
	}

	cmd.PersistentFlags().StringP("db", "", "postgres", "The database the create table statement is from")
	cmd.PersistentFlags().StringP("query", "q", "", "The query to parse to generate the schema")
	viper.BindPFlag("db", cmd.PersistentFlags().Lookup("db"))
	viper.BindPFlag("query", cmd.PersistentFlags().Lookup("query"))
	viper.AutomaticEnv()
	viper.SetEnvPrefix("LIBRARIAN")
	return cmd
}
