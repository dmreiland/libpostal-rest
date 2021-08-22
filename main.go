package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/mux"
	expand "github.com/openvenues/gopostal/expand"
	parser "github.com/openvenues/gopostal/parser"
)

type Request struct {
	Query string `json:"query"`
}

type BulkRequest struct {
	Queries []struct {
		QueryId string `json:"query_id"`
		Query   string `json:"query"`
	} `json:"queries"`
}

type BulkParseItem struct {
	QueryId string                   `json:"query_id"`
	Parsed  []parser.ParsedComponent `json:"parsed"`
}

type BulkParseResponse struct {
	Items []BulkParseItem `json:"items"`
}

type BulkExpandItem struct {
	QueryId    string   `json:"query_id"`
	Expansions []string `json:"expansions"`
}

type BulkExpandResponse struct {
	Items []BulkExpandItem `json:"items"`
}

func main() {
	host := os.Getenv("LISTEN_HOST")
	if host == "" {
		host = "0.0.0.0"
	}
	port := os.Getenv("LISTEN_PORT")
	if port == "" {
		port = "8080"
	}
	listenSpec := fmt.Sprintf("%s:%s", host, port)

	certFile := os.Getenv("SSL_CERT_FILE")
	keyFile := os.Getenv("SSL_KEY_FILE")

	router := mux.NewRouter()
	router.HandleFunc("/health", HealthHandler).Methods("GET")
	router.HandleFunc("/expand", ExpandHandler).Methods("POST")
	router.HandleFunc("/parser", ParserHandler).Methods("POST")
	router.HandleFunc("/bulk/expand", BulkExpandHandler).Methods("POST")
	router.HandleFunc("/bulk/parser", BulkParserHandler).Methods("POST")

	s := &http.Server{Addr: listenSpec, Handler: router}
	go func() {
		if certFile != "" && keyFile != "" {
			fmt.Printf("listening on https://%s\n", listenSpec)
			s.ListenAndServeTLS(certFile, keyFile)
		} else {
			fmt.Printf("listening on http://%s\n", listenSpec)
			s.ListenAndServe()
		}
	}()

	stop := make(chan os.Signal)
	signal.Notify(stop, os.Interrupt)

	<-stop
	fmt.Println("\nShutting down the server...")
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	s.Shutdown(ctx)
	fmt.Println("Server stopped")
}

func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func ExpandHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req Request

	q, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(q, &req)

	expansions := expand.ExpandAddress(req.Query)

	expansionThing, _ := json.Marshal(expansions)
	w.Write(expansionThing)
}

func BulkExpandHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req BulkRequest
	var payload BulkExpandResponse

	q, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(q, &req)

	for idx := range req.Queries {
		var expandPayload BulkExpandItem
		expandPayload.QueryId = req.Queries[idx].QueryId
		expandPayload.Expansions = expand.ExpandAddress(req.Queries[idx].Query)
		payload.Items = append(payload.Items, expandPayload)
	}

	expansionThing, _ := json.Marshal(payload)
	w.Write(expansionThing)
}

func ParserHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req Request

	q, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(q, &req)

	parsed := parser.ParseAddress(req.Query)
	parseThing, _ := json.Marshal(parsed)
	w.Write(parseThing)
}

func BulkParserHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req BulkRequest
	var payload BulkParseResponse

	q, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(q, &req)

	for idx := range req.Queries {
		var addressPayload BulkParseItem
		addressPayload.QueryId = req.Queries[idx].QueryId
		addressPayload.Parsed = parser.ParseAddress(req.Queries[idx].Query)
		payload.Items = append(payload.Items, addressPayload)
	}
	parseThing, _ := json.Marshal(payload)
	w.Write(parseThing)
}
