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
	side := strings.ToUpper(s.Side)
	var color, label, dir string

	switch action {
	case "OPEN":
		dir = "\x1b[36mOPEN\x1b[0m"
		switch side {
		case "BUY", "BUY_STOP", "BUY_LIMIT":
			color = "\x1b[32m"
			label = side
		case "SELL", "SELL_STOP", "SELL_LIMIT":
			color = "\x1b[31m"
			label = side
		default:
			color = "\x1b[34m"
			label = side
		}
	case "CLOSE":
		color = "\x1b[35m"
		label = ""
		dir = "\x1b[35mCLOSE\x1b[0m"
	case "EDIT":
		color = "\x1b[33m"
		label = ""
		dir = "\x1b[33mEDIT\x1b[0m"
	default:
		color = "\x1b[34m"
		label = ""
		dir = "\x1b[34m?\x1b[0m"
	}

	fmt.Fprintf(_out,
		"\x1b[90m[%s]\x1b[0m %s%-12s\x1b[0m %s %-8s %s%.2f\x1b[0m @ %s%.5f\x1b[0m | SL:%s%.5f\x1b[0m | TP:%s%.5f\x1b[0m | Magic:%d | Pnl:%s%+.2f\x1b[0m | %s\n",
		time.Now().Format("15:04:05.000"),
		color, label,
		dir,
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
