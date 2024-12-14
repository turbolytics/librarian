package schema

import (
	"fmt"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "schema",
		Short: "Utilities to assist with generating parquet schemas",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("librarian schema utilities!")
			return nil
		},
	}

	cmd.AddCommand(newGenerateCommand())

	return cmd
}
