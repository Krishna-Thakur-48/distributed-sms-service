package config

import (
	"os"
)

type Config struct {
	ServerPort   string
	MongoURI     string
	MongoDBName  string
	KafkaBrokers string
	KafkaTopic   string
}

func getEnvOrDefault(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func LoadConfig() *Config {
	return &Config{
		ServerPort:   getEnvOrDefault("SERVER_PORT", "8081"),
		MongoURI:     getEnvOrDefault("MONGO_URI", "mongodb://localhost:27017"),
		MongoDBName:  getEnvOrDefault("MONGO_DB_NAME", "sms_db"),
		KafkaBrokers: getEnvOrDefault("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:   getEnvOrDefault("KAFKA_TOPIC", "sms-events-topic"),
	}
}
