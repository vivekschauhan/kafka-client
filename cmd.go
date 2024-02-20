package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Axway/agent-sdk/pkg/util"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var cfg = &Config{}

func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{Use: ""}
	rootCmd.AddCommand(newSubCmd("orders", "Order publisher/consumer", runOrders))
	rootCmd.AddCommand(newSubCmd("users", "User publisher/consumer", runUsers))

	return rootCmd
}

func newSubCmd(use, shortStr string, runner func(_ *cobra.Command, _ []string) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:     use,
		Short:   shortStr,
		Version: "0.0.1",
		RunE:    runner,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			v, err := initViperConfig(cmd)
			if err != nil {
				return err
			}
			err = v.Unmarshal(cfg)
			if err != nil {
				return err
			}
			return nil
		},
	}
	setupFlags(cmd)
	return cmd
}

func initViperConfig(cmd *cobra.Command) (*viper.Viper, error) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	bindFlagsToViperConfig(cmd, v)
	envFile, _ := cmd.Flags().GetString("envFile")
	if envFile != "" {
		util.LoadEnvFromFile(envFile)
	}
	return v, nil
}

func bindFlagsToViperConfig(cmd *cobra.Command, v *viper.Viper) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		name := strings.ToUpper(f.Name)
		if err := v.BindPFlag(name, f); err != nil {
			panic(err)
		}

		if !f.Changed && v.IsSet(f.Name) {
			val := v.Get(f.Name)
			err := cmd.Flags().Set(f.Name, fmt.Sprintf("%v", val))
			if err != nil {
				panic(err)
			}
		}
	})
}

func setupFlags(cmd *cobra.Command) {
	cmd.Flags().String("topic", "", "The kafka topic name")
	cmd.Flags().String("group_id", "", "The consumer group ID")
	cmd.Flags().String("bootstrap_server", "", "The kafka bootstrap servers")
	cmd.Flags().String("security_protocol", "SASL_SSL", "The SASL/SCRAM mechanism")
	cmd.Flags().String("sasl_mechanism", "SCRAM-SHA-256", "The SASL/SCRAM mechanism")
	cmd.Flags().String("sasl_user", "", "The SASL user name")
	cmd.Flags().String("sasl_password", "", "The SASL user's password")
	cmd.Flags().String("envFile", "", "Config file with environment variables")
}

func runOrders(_ *cobra.Command, _ []string) error {
	return runProducerAndConsumer(orderGenerator)
}

func runUsers(_ *cobra.Command, _ []string) error {
	return runProducerAndConsumer(userGenerator)
}

func runProducerAndConsumer(factory payloadGeneratorFactory) error {
	p := newProducer(cfg, factory)
	err := p.run()
	if err != nil {
		return err
	}
	c := newConsumer(cfg)
	err = c.run()
	if err != nil {
		return err
	}
	return nil
}

func orderGenerator() ([]byte, error) {
	value := Order{
		ID:          time.Now().UTC().UnixMilli(),
		Amount:      10,
		Description: "generated order",
	}
	return json.Marshal(value)
}

func userGenerator() ([]byte, error) {
	value := User{
		ID:    time.Now().UTC().UnixMilli(),
		Name:  "abc",
		Email: "abc@test.com",
	}
	return json.Marshal(value)
}
