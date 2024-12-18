package cmd

import (
	"fmt"
	"github.com/turbolytics/librarian/internal/cmd/fixtures"
	"github.com/turbolytics/librarian/internal/cmd/schema"
	"os"

	"github.com/spf13/cobra"
	"github.com/turbolytics/librarian/internal/cmd/archiver"
)

func NewRootCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "librarian",
		Short: "",
		Long:  ``,
		// The run function is called when the command is executed
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Welcome to librarian!")
		},
	}

	cmd.AddCommand(archiver.NewCommand())
	cmd.AddCommand(schema.NewCommand())
	cmd.AddCommand(fixtures.NewCommand())

	return cmd
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	cmd := NewRootCommand()
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
