// Demo UI proxy for the Distributed SMS Service.
//
// Serves the static frontend (index.html) and reverse-proxies API calls so the
// browser only ever talks to ONE origin (this server). That sidesteps CORS
// entirely without touching either backend service.
//
//	/api/sender/*  ->  http://localhost:8080  (Java SMS Sender)
//	/api/store/*   ->  http://localhost:8081  (Go SMS Store)
//	/*             ->  static files in this directory (index.html)
//
// Run:  go run proxy.go        (stdlib only, no go.mod / dependencies needed)
// Then open http://localhost:3000
package main

import (
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
)

func mustURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("invalid URL %q: %v", raw, err)
	}
	return u
}

// proxyHandler strips the given prefix and forwards the request to target.
// If apiKey is non-empty, it is injected as the X-API-Key header — this is how
// the proxy acts as a trusted gateway: browser traffic never carries the secret,
// the proxy adds it on the way to the backend.
func proxyHandler(target *url.URL, stripPrefix, apiKey string) http.Handler {
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		// Backend not up yet — return 502 so the UI can show it as DOWN.
		log.Printf("upstream %s error: %v", target, err)
		http.Error(w, "upstream unavailable: "+err.Error(), http.StatusBadGateway)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = strings.TrimPrefix(r.URL.Path, stripPrefix)
		if !strings.HasPrefix(r.URL.Path, "/") {
			r.URL.Path = "/" + r.URL.Path
		}
		r.Host = target.Host
		if apiKey != "" {
			r.Header.Set("X-API-Key", apiKey)
		}
		rp.ServeHTTP(w, r)
	})
}

func main() {
	// Default the admin key from the env (kept in sync with the Java service's
	// ADMIN_API_KEY), falling back to the demo secret.
	defaultKey := os.Getenv("ADMIN_API_KEY")
	if defaultKey == "" {
		defaultKey = "demo-secret-key-change-me"
	}

	addr := flag.String("addr", ":3000", "address to listen on")
	senderURL := flag.String("sender", "http://localhost:8080", "Java SMS Sender base URL")
	storeURL := flag.String("store", "http://localhost:8081", "Go SMS Store base URL")
	apiKey := flag.String("api-key", defaultKey, "admin API key injected on sender-bound requests")
	flag.Parse()

	sender := mustURL(*senderURL)
	store := mustURL(*storeURL)

	mux := http.NewServeMux()
	// Sender traffic gets the admin key injected; store traffic does not need it.
	mux.Handle("/api/sender/", proxyHandler(sender, "/api/sender", *apiKey))
	mux.Handle("/api/store/", proxyHandler(store, "/api/store", ""))
	mux.Handle("/", http.FileServer(http.Dir(".")))

	log.Printf("SMS demo UI:   http://localhost%s", *addr)
	log.Printf("  /api/sender -> %s (injecting X-API-Key)", sender)
	log.Printf("  /api/store  -> %s", store)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
