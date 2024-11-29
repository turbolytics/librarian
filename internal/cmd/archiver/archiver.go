package archiver

import (
	"fmt"

	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "archiver",
		Short: "Manages the archival of data",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("welcome to librarian archiver!")
			return nil
		},
	}
	cmd.AddCommand(newStartCommand())
	cmd.AddCommand(newInvokeCommand())
	return cmd
}
