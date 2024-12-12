package schema

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newGenerateCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "generate",
		Short: "Generates a parquet schema from a database table",
		RunE: func(cmd *cobra.Command, args []string) error {
			uri := viper.GetString("uri")
			fmt.Println("librarian schema generate!", uri, viper.GetString("schema"), viper.GetString("table"))
			return nil
		},
	}

	cmd.PersistentFlags().StringP("uri", "u", "", "URI to connect to the database")
	cmd.PersistentFlags().StringP("schema", "s", "public", "Schema to generate a parquet schema for")
	cmd.PersistentFlags().StringP("table", "t", "", "Table to generate a parquet schema for")
	viper.BindPFlag("uri", cmd.PersistentFlags().Lookup("uri"))
	viper.BindPFlag("table", cmd.PersistentFlags().Lookup("table"))
	viper.BindPFlag("schema", cmd.PersistentFlags().Lookup("schema"))
	viper.AutomaticEnv()
	viper.SetEnvPrefix("LIBRARIAN")
	return cmd
}
