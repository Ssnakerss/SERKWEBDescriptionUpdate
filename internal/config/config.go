package config

import (
	"errors"
	"flag"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

// Config хранит конфигурацию приложения.
type Config struct {
	DBConnString    string `yaml:"dbconstring" env:"DB_CONNECTION_STRING" env-required:"true"`
	GigaChatAuthKey string `yaml:"gigachatauthkey" env:"GIGACHAT_AUTH_KEY" env-required:"true"`
	Schema          string
	Database        string
	DataDescription string
}

var cfg Config

// LoadConfig загружает конфигурацию из переменных окружения.
func LoadConfig() (*Config, error) {
	configPath := fetchConfigPath()

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, errors.New("Файл конфигурации не найден")
	}

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func fetchConfigPath() string {
	var config string
	flag.StringVar(&cfg.Schema, "schema", "", "target schema")
	flag.StringVar(&cfg.Database, "database", "", "target database")
	flag.StringVar(&cfg.DataDescription, "datadesc", "", "data description")
	flag.StringVar(&config, "config", "", "path to config file")
	flag.Parse()

	if config == "" {
		config = os.Getenv("CONFIG_PATH")
	}
	if config == "" {
		return "config.yaml"
	}

	return config
}
