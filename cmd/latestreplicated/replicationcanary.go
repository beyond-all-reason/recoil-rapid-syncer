package main

import (
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

func (s *Server) HandleUpdateReplicationCanary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		log.Printf("Got %s, not POST request for URL: %v", r.Method, r.URL)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	contents := strings.NewReader(time.Now().Format(time.RFC3339Nano))
	err := s.bunnyStorageZone.Upload(r.Context(), "replication-canary.txt", contents)
	if err != nil {
		log.Printf("Failed to upload replication canary: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "OK")
}
