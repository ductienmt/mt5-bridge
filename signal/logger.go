package signal

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mattn/go-colorable"
)

type Logger struct{}

var _out io.Writer = colorable.NewColorableStdout()

func NewLogger() *Logger {
	return &Logger{}
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.log("\x1b[36m[INFO]\x1b[0m", fmt.Sprintf(format, args...), _out)
}

func (l *Logger) Warn(format string, args ...interface{}) {
	l.log("\x1b[33m[WARN]\x1b[0m", fmt.Sprintf(format, args...), _out)
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.log("\x1b[31m[ERROR]\x1b[0m", fmt.Sprintf(format, args...), _out)
}

func (l *Logger) Order(s *Signal) {
	action := strings.ToUpper(s.Action)
	var color string
	var label string

	switch action {
	case "BUY":
		color = "\x1b[32m"
		label = "BUY"
	case "SELL":
		color = "\x1b[31m"
		label = "SELL"
	case "BUY_STOP":
		color = "\x1b[32m"
		label = "BUY_STOP"
	case "SELL_STOP":
		color = "\x1b[31m"
		label = "SELL_STOP"
	case "CLOSE":
		color = "\x1b[35m"
		label = "CLOSE"
	case "CLOSE_ALL":
		color = "\x1b[35m"
		label = "CLOSE_ALL"
	case "MODIFY":
		color = "\x1b[33m"
		label = "MODIFY"
	default:
		color = "\x1b[34m"
		label = action
	}

	fmt.Fprintf(_out,
		"\x1b[90m[%s]\x1b[0m %s%-12s\x1b[0m %-8s %s%.2f\x1b[0m @ %s%.5f\x1b[0m | SL:%s%.5f\x1b[0m | TP:%s%.5f\x1b[0m | Magic:%d | Pnl:%s%+.2f\x1b[0m | %s\n",
		time.Now().Format("15:04:05.000"),
		color, label,
		"\x1b[37m"+s.Symbol+"\x1b[0m",
		color, s.Lot,
		color, s.Price,
		color, s.SL,
		color, s.TP,
		s.Magic,
		color, s.Pnl,
		s.Comment)
}

func (l *Logger) Queue(size int) {
	if size > 0 {
		l.Info("Queue size: \x1b[33m%d\x1b[0m", size)
	}
}

func (l *Logger) log(level, msg string, out io.Writer) {
	fmt.Fprintf(out, "\x1b[90m[%s]\x1b[0m %s %s\n",
		time.Now().Format("15:04:05.000"), level, msg)
}

func Banner() {
	banner := `
` + "\x1b[36m" + `╔══════════════════════════════════════════════╗
║      MT5 Trading Bridge  —  Go Bridge       ║
║   Signal forwarder for TradingBridge EA      ║
╚══════════════════════════════════════════════╝` + "\x1b[0m" + `
`
	fmt.Print(banner)
}
