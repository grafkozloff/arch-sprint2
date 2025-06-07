package main

import (
	"math/rand"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func main() {
    port := getEnv("PORT", "8000")
	monolithURL, err := url.Parse(getEnv("MONOLITH_URL", "http://monolith:8080"))
	if err != nil {
		log.Fatal("Cannot parse monolith URL")
	}
	moviesServiceURL, err := url.Parse(getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081"))
	if err != nil {
		log.Fatal("Cannot parse movies service URL")
	}
	eventsServiceURL, err := url.Parse(getEnv("EVENTS_SERVICE_URL", "http://events-service:8082"))
	if err != nil {
		log.Fatal("Cannot parse events service URL")
	}
	migrationPercent, err := strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "50"))
	if err != nil {
		log.Fatal("Invalid migration percent value")
	}
	gradualMigration := getEnv("GRADUAL_MIGRATION", "true") == "true"

	monolithProxy := httputil.NewSingleHostReverseProxy(monolithURL)
	moviesServiceProxy := httputil.NewSingleHostReverseProxy(moviesServiceURL)
	eventsServiceProxy := httputil.NewSingleHostReverseProxy(eventsServiceURL)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
    	w.WriteHeader(http.StatusOK)
    	w.Write([]byte("Strangler Fig Proxy is healthy"))
    })

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
	    log.Printf("Request: %s %s", r.Method, r.URL.Path)

        if strings.Contains(r.URL.Path, "/api/events") {
            eventsServiceProxy.ServeHTTP(w, r)
			return
        }

		if !gradualMigration || !strings.Contains(r.URL.Path, "/api/movies") {
			monolithProxy.ServeHTTP(w, r)
			return
		}

		if rand.Intn(100) < migrationPercent {
            moviesServiceProxy.ServeHTTP(w, r)
        } else {
    		monolithProxy.ServeHTTP(w, r)
        }
    })

    log.Printf("API Gateway is running on port %s", port)

	log.Fatal(http.ListenAndServe(":" + port, nil))
}

