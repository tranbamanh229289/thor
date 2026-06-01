package zap

import (
	"thor/pkg/config"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New(cfg *config.ZapConfig) (*zap.Logger, error) {
	var zapCfg zap.Config
	if cfg.Development {
		zapCfg = zap.NewDevelopmentConfig()
	} else {
		zapCfg = zap.NewProductionConfig()
		zapCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	}

	if cfg.Level != "" {
		level, err := zapcore.ParseLevel(cfg.Level)
		if err != nil {
			return nil, err
		}
		zapCfg.Level = zap.NewAtomicLevelAt(level)
	}

	if cfg.Encoding != "" {
		zapCfg.Encoding = cfg.Encoding
	}

	zapCfg.DisableCaller = cfg.DisableCaller
	zapCfg.DisableStacktrace = cfg.DisableStacktrace
	zapCfg.OutputPaths = []string{"stdout"}
	zapCfg.ErrorOutputPaths = []string{"stderr"}

	if cfg.OutputPath != "" {
		zapCfg.OutputPaths = append(zapCfg.OutputPaths, cfg.OutputPath)
	}

	if cfg.ErrorOutputPath != "" {
		zapCfg.ErrorOutputPaths = append(zapCfg.ErrorOutputPaths, cfg.ErrorOutputPath)
	}

	return zapCfg.Build()
}
