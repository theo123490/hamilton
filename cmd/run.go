/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/theo123490/kafka-client-cli/clients"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run a producer or consumer",
	Long:  `run a producer or consumer`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		runType := args[0]
		cfgFile, err := cmd.Flags().GetString("config-file")
		if cfgFile != "" {
			fmt.Printf("using Config file %s\n", cfgFile)
			viper.SetConfigFile(cfgFile)
		} else {
			var defaultCfgFile string = ".kafka-client-cli.env"
			fmt.Printf("config file not found, reading default config file %s\n", defaultCfgFile)
			viper.SetConfigFile(defaultCfgFile)
		}
		viper.ReadInConfig()

		if err != nil {
			fmt.Println(err)
		}
		if runType == "consumer" {
			clients.Consumer(cfgFile)
		} else if runType == "producer" {
			clients.Producer(cfgFile)
		} else {
			fmt.Printf("I don't recognize %s \n", runType)
		}
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().String("config-file", "", "config file for producer or consumer")
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// runCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// runCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
