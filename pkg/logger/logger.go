package logger

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewLogger(level string) (*zap.Logger, error) {
	logEncoder := zap.NewProductionEncoderConfig()
	logEncoder.TimeKey = "time"
	logLevel, err := zapcore.ParseLevel(level)
	if err != nil {
		return nil, fmt.Errorf("failed to parse log level: %w", err)
	}

	logger := zap.New(
		zapcore.NewCore(zapcore.NewJSONEncoder(logEncoder),
			os.Stdout,
			logLevel))
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	return logger, nil
}
