// Package log is a wrapper around the standard log package, with all the
// settings we want applied already set
package log

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/mc0/okq/config"
)

type okqLogger struct {
	*log.Logger
}

const defaultFlags = log.LstdFlags

// This is what all logging methods should be called on. We leave this public
// and don't make wrappers around it so that the Lshortfile flag will work
// (prints out file and line number of logs)
var L okqLogger

func init() {
	var flags int
	if config.Debug {
		flags = log.LstdFlags | log.Lshortfile
	} else {
		flags = log.LstdFlags
	}
	L = okqLogger{log.New(os.Stdout, "", flags)}
}

func (l *okqLogger) Debug(v ...interface{}) {
	if config.Debug {
		format := strings.Repeat("%s", len(v))
		s := fmt.Sprintf(format, v...)
		l.Output(2, s)
	}
}

func (l *okqLogger) Debugf(format string, args ...interface{}) {
	if config.Debug {
		s := fmt.Sprintf(format, args...)
		l.Output(2, s)
	}
}
