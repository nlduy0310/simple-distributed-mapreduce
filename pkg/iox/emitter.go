package iox

import (
	"errors"
	"io"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/errx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/isnil"
)

type emitterConfig struct {
	trailingText string
}

func defaultEmitterConfig() emitterConfig {
	return emitterConfig{
		trailingText: "\n",
	}
}

func validateEmitterConfig(cfg emitterConfig) error {
	return nil
}

type EmitterOption func(*emitterConfig)

func WithTrailingText(text string) EmitterOption {
	return func(e *emitterConfig) {
		e.trailingText = text
	}
}

type Emitter struct {
	cfg    emitterConfig
	writer io.Writer
}

func (e *Emitter) Emit(text string) error {
	_, err := e.writer.Write([]byte(text + e.cfg.trailingText))
	return err
}

func NewEmitter(writer io.Writer, opts ...EmitterOption) (*Emitter, error) {
	if isnil.Interface(writer) {
		return nil, errors.New("nil writer")
	}

	cfg := defaultEmitterConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	if err := validateEmitterConfig(cfg); err != nil {
		return nil, errx.WithContext(err, "validate config")
	}

	return &Emitter{
		cfg:    cfg,
		writer: writer,
	}, nil
}
