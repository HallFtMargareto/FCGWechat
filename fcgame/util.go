package fcgame

import (
	"runtime/debug"

	"go.uber.org/zap"
)

func Capture(action string) {
	if err := recover(); err != nil {
		Logger.Error(
			action,
			zap.String("stack", string(debug.Stack())),
			zap.Any("error", err),
		)
	}
}

func SafeRun(fn func()) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				Logger.Error(
					"panic",
					zap.String("stack", string(debug.Stack())),
					zap.Any("error", err),
				)
			}
		}()
		fn()
	}()
}
