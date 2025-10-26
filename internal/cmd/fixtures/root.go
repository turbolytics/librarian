package fixtures

import "github.com/spf13/cobra"

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "fixtures",
		Short: "Manages the fixtures",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}
	cmd.AddCommand(newGenerateCommand())
	return cmd
}
