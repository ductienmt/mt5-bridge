package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Handler returns the Prometheus metrics handler
func Handler() http.Handler {
	return promhttp.Handler()
}
