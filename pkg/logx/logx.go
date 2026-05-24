package logx

import (
	"fmt"
	"log"
)

type level string

const (
	debugL level = "DEBUG"
	infoL  level = "INFO"
	warnL  level = "WARN"
	errorL level = "ERROR"
)

func Debug(msg string) {
	withLevel(debugL, msg)
}

func Debugf(format string, args ...any) {
	withLevel(debugL, formatMsg(format, args))
}

func Info(msg string) {
	withLevel(infoL, msg)
}

func Infof(format string, args ...any) {
	withLevel(infoL, formatMsg(format, args))
}

func Warn(msg string) {
	withLevel(warnL, msg)
}

func Warnf(format string, args ...any) {
	withLevel(warnL, formatMsg(format, args))
}

func Error(msg string) {
	withLevel(errorL, msg)
}

func Errorf(format string, args ...any) {
	withLevel(errorL, formatMsg(format, args))
}

func Err(err error) {
	withLevel(errorL, err.Error())
}

func withLevel(l level, msg string) {
	log.Printf("[%s] %s", l, msg)
}

func formatMsg(format string, args []any) string {
	return fmt.Sprintf(format, args...)
}
