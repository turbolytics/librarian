package fixtures

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
	"log"
	"math/rand"
	"time"
)

func newGenerateCommand() *cobra.Command {
	var records int
	var table string

	var cmd = &cobra.Command{
		Use:   "generate",
		Short: "Generates fixtures for testing",
		RunE: func(cmd *cobra.Command, args []string) error {
			if table != "Consumer Complaints" && table != "property_sales" {
				return fmt.Errorf("unsupported table: %s", table)
			}

			conn, err := pgx.Connect(context.Background(), "postgresql://test:test@localhost:5432/test?sslmode=disable")
			if err != nil {
				log.Fatalf("Unable to connect to database: %v\n", err)
			}
			defer conn.Close(context.Background())

			batchSize := 1000
			rows := make([][]interface{}, 0, batchSize)

			for i := 0; i < records; i++ {
				if table == "property_sales" {
					row := []interface{}{
						i + 1,
						rand.Intn(2023),
						time.Now().Format("2006-01-02"),
						fmt.Sprintf("%d Town", i+1),
						fmt.Sprintf("%d Address", i+1),
						rand.Float64() * 1000000,
						rand.Float64() * 1000000,
						rand.Float64() * 100,
						fmt.Sprintf("%d Type", i),
						fmt.Sprintf("%d Residential", i),
						fmt.Sprintf("%d Code", i),
						fmt.Sprintf("%d Assessor Remarks", i),
						fmt.Sprintf("%d OPM Remarks", i),
						fmt.Sprintf("%d Location", i+1),
					}
					rows = append(rows, row)

					if len(rows) == batchSize {
						_, err := conn.CopyFrom(
							context.Background(),
							pgx.Identifier{"property_sales"},
							[]string{
								"serial_number",
								"list_year",
								"date_recorded",
								"town",
								"address",
								"assessed_value",
								"sale_amount",
								"sales_ratio",
								"property_type",
								"residential_type",
								"non_use_code",
								"assessor_remarks",
								"opm_remarks",
								"location",
							},
							pgx.CopyFromRows(rows),
						)
						if err != nil {
							log.Fatalf("Failed to copy data: %v\n", err)
						}
						rows = rows[:0]
					}
				}
			}

			if len(rows) > 0 {
				_, err := conn.CopyFrom(
					context.Background(),
					pgx.Identifier{"property_sales"},
					[]string{
						"serial_number", "list_year", "date_recorded", "town", "address",
						"assessed_value", "sale_amount", "sales_ratio", "property_type",
						"residential_type", "non_use_code", "assessor_remarks", "opm_remarks",
						"location",
					},
					pgx.CopyFromRows(rows),
				)
				if err != nil {
					log.Fatalf("Failed to copy data: %v\n", err)
				}
			}

			fmt.Printf("Inserted %d records into %s table\n", records, table)
			return nil
		},
	}

	cmd.Flags().IntVarP(&records, "records", "r", 10, "Number of records to generate")
	cmd.Flags().StringVarP(&table, "table", "t", "property_sales", "Table to insert records into (supports 'Consumer Complaints' and 'property_sales')")
	return cmd
}
