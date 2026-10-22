package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

func main() {
	port := getenv("PORT", "8000")
	gradual := strings.EqualFold(getenv("GRADUAL_MIGRATION", "true"), "true")
	percent := atoi(getenv("MOVIES_MIGRATION_PERCENT", "0"))

	monolith := newProxy(getenv("MONOLITH_URL", "http://localhost:8080"))
	movies := newProxy(getenv("MOVIES_SERVICE_URL", "http://localhost:8081"))
	events := newProxy(getenv("EVENTS_SERVICE_URL", "http://localhost:8082"))

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("Strangler Fig Proxy is healthy"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/movies"):
			if toMovies(gradual, percent) {
				log.Printf("%s %s -> movies-service", r.Method, r.URL.Path)
				movies.ServeHTTP(w, r)
				return
			}
			log.Printf("%s %s -> monolith", r.Method, r.URL.Path)
			monolith.ServeHTTP(w, r)
		case strings.HasPrefix(r.URL.Path, "/api/events"):
			log.Printf("%s %s -> events-service", r.Method, r.URL.Path)
			events.ServeHTTP(w, r)
		default:
			log.Printf("%s %s -> monolith", r.Method, r.URL.Path)
			monolith.ServeHTTP(w, r)
		}
	})

	log.Printf("proxy on :%s gradual=%v movies=%d%%", port, gradual, percent)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

// toMovies: при выключенном флаге домен уже уехал целиком в movies.
// При включённом — в movies уходит только MOVIES_MIGRATION_PERCENT запросов.
func toMovies(gradual bool, percent int) bool {
	if !gradual || percent >= 100 {
		return true
	}
	if percent <= 0 {
		return false
	}
	return rand.Intn(100) < percent
}

func newProxy(raw string) *httputil.ReverseProxy {
	target, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("bad upstream %q: %v", raw, err)
	}
	p := httputil.NewSingleHostReverseProxy(target)
	p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("upstream %s: %v", r.URL.Path, err)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	return p
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}
