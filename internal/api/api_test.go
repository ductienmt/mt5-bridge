package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAuthMiddleware_SkipHealthCheck(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	handler := AuthMiddleware(mux)

	req := httptest.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rr.Code)
	}
}

func TestCORSMiddleware(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := CORSMiddleware(mux)

	// Test preflight request
	req := httptest.NewRequest("OPTIONS", "/test", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status 200 for OPTIONS, got %d", rr.Code)
	}

	// Check CORS headers
	if rr.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("Missing Access-Control-Allow-Origin header")
	}
	if rr.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Error("Missing Access-Control-Allow-Methods header")
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/panic", func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	})

	handler := RecoveryMiddleware(mux)

	req := httptest.NewRequest("GET", "/panic", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	// Should not panic and return 500
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", rr.Code)
	}
}

func TestValidateContentType(t *testing.T) {
	tests := []struct {
		method       string
		contentType  string
		expectStatus int
	}{
		{"GET", "", http.StatusOK},
		{"DELETE", "", http.StatusOK},
		{"POST", "application/json", http.StatusOK},
		{"PUT", "application/json", http.StatusOK},
		{"POST", "text/plain", http.StatusUnsupportedMediaType},
		{"PUT", "text/html", http.StatusUnsupportedMediaType},
	}

	for _, tt := range tests {
		mux := http.NewServeMux()
		mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})

		handler := ValidateContentType(mux)

		req := httptest.NewRequest(tt.method, "/test", strings.NewReader("{}"))
		if tt.contentType != "" {
			req.Header.Set("Content-Type", tt.contentType)
		}

		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if rr.Code != tt.expectStatus {
			t.Errorf("Method=%s Content-Type=%s: expected %d, got %d",
				tt.method, tt.contentType, tt.expectStatus, rr.Code)
		}
	}
}

func TestContainsSubstring(t *testing.T) {
	tests := []struct {
		path    string
		substr  string
		matches bool
	}{
		{"/api/masters/abc/followers", "/followers", true},
		{"/api/masters/abc", "/followers", false},
		{"/api/followers/xyz/start", "/start", true},
		{"/api/followers/xyz/stop", "/stop", true},
		{"/api/followers/xyz", "/start", false},
		{"/health", "/followers", false},
	}

	for _, tt := range tests {
		result := containsSubstring(tt.path, tt.substr)
		if result != tt.matches {
			t.Errorf("containsSubstring('%s', '%s') = %v, expected %v",
				tt.path, tt.substr, result, tt.matches)
		}
	}
}

func TestWriteJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		data := map[string]string{"key": "value"}
		writeJSON(w, http.StatusOK, data)
	})

	req := httptest.NewRequest("GET", "/test", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rr.Code)
	}

	if rr.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got '%s'",
			rr.Header().Get("Content-Type"))
	}

	var result map[string]string
	json.Unmarshal(rr.Body.Bytes(), &result)
	if result["key"] != "value" {
		t.Errorf("Expected body '{\"key\":\"value\"}', got '%s'", rr.Body.String())
	}
}

func TestWriteError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusBadRequest, "Test error", "TEST_ERROR")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rr.Code)
	}

	var result map[string]interface{}
	json.Unmarshal(rr.Body.Bytes(), &result)

	if result["error"] != "Test error" {
		t.Errorf("Expected error 'Test error', got '%v'", result["error"])
	}
	if result["code"] != "TEST_ERROR" {
		t.Errorf("Expected code 'TEST_ERROR', got '%v'", result["code"])
	}
}

func TestGenerateID(t *testing.T) {
	id1 := generateID("m")
	id2 := generateID("m")

	// Should have correct prefix
	if !strings.HasPrefix(id1, "m_") {
		t.Errorf("ID should start with 'm_', got '%s'", id1)
	}

	// Should be unique
	if id1 == id2 {
		t.Error("Generated IDs should be unique")
	}

	// Should have expected length
	if len(id1) < 10 {
		t.Errorf("ID should be at least 10 chars, got %d", len(id1))
	}
}
