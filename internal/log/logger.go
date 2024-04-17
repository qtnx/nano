// Copyright (c) nano Authors. All Rights Reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package log

import (
	"os"

	"github.com/lonng/nano/internal/env"
	"github.com/rs/zerolog"
)

// Logger represents  the log interface
type Logger interface {
	Debug(v ...interface{})
	Println(v ...interface{})
	Infof(format string, v ...interface{})
	Error(v ...interface{})
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
	Warn(v ...interface{})
}

type ZerologWrapper struct {
	logger zerolog.Logger
}

// New creates a new Logger instance using zerolog
func New(logger zerolog.Logger) Logger {
	return &ZerologWrapper{logger: logger}
}

func (z *ZerologWrapper) Debug(v ...interface{}) {
	z.logger.Debug().Msgf("%v", v)
}

func (z *ZerologWrapper) Println(v ...interface{}) {
	// Mimic Println using Print by leveraging the Sprintf method
	z.logger.Info().Msgf("%v", v)
}

func (z *ZerologWrapper) Fatal(v ...interface{}) {
	z.logger.Fatal().Msgf("%v", v)
}

func (z *ZerologWrapper) Fatalf(format string, v ...interface{}) {
	z.logger.Fatal().Msgf(format, v...)
}

func (z *ZerologWrapper) Error(v ...interface{}) {
	z.logger.Error().Msgf("%v", v)
}

func (z *ZerologWrapper) Warn(v ...interface{}) {
	z.logger.Warn().Msgf("%v", v)
}

func (z *ZerologWrapper) Infof(format string, v ...interface{}) {
	z.logger.Info().Msgf(format, v...)
}

func init() {
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout}
	// Initializes the zerolog instance for application-wide logging
	log := zerolog.New(consoleWriter).With().Timestamp().Logger()

	if env.Debug {
		log = log.Level(zerolog.DebugLevel)
	} else {
		log = log.Level(zerolog.InfoLevel)
	}

	SetLogger(New(log))
}

var (
	Debug   func(v ...interface{})
	Println func(v ...interface{})
	Infof   func(format string, v ...interface{})
	Fatal   func(v ...interface{})
	Fatalf  func(format string, v ...interface{})
	Error   func(v ...interface{})
	Warn    func(v ...interface{})
)

// SetLogger rewrites the default logger
func SetLogger(logger Logger) {
	if logger == nil {
		return
	}
	Println = logger.Println
	Fatal = logger.Fatal
	Fatalf = logger.Fatalf
	Debug = logger.Debug
	Error = logger.Error
	Warn = logger.Warn
	Infof = logger.Infof
}
