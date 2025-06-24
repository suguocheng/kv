package log

import (
	"fmt"
	"log"
	"os"
)

var logger *log.Logger

func init() {
	// 创建带文件名和行号的 Logger
	logger = log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)
}

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		logger.Output(2, fmt.Sprintf(format, a...))
	}
}

func Printf(format string, a ...interface{}) {
	logger.Output(2, fmt.Sprintf(format, a...))
}
