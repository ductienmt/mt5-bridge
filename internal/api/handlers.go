package api

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"

	"mt5-bridge/internal/cache"
	"mt5-bridge/internal/models"
	"mt5-bridge/internal/redis"
	"mt5-bridge/internal/repository"
)

const (
	// JWT settings
	jwtSecret     = "your-secret-key-change-in-production"
	tokenExpiry   = 24 * time.Hour

	// Server settings
	serverPort = 8082
)

// Server represents the API server
type Server struct {
	db              *sql.DB
	masterCache    *cache.MasterCache
	subStore       *redis.SubscriptionStore
	fastLookup     *redis.FastLookupStore
	masterRepo     *repository.MasterRepository
	followerRepo   *repository.FollowerRepository
	jwtSecret      []byte
}

// NewServer creates a new API server
func NewServer(
	db *sql.DB,
	masterCache *cache.MasterCache,
	subStore *redis.SubscriptionStore,
	fastLookup *redis.FastLookupStore,
) *Server {
	return &Server{
		db:            db,
		masterCache:   masterCache,
		subStore:      subStore,
		fastLookup:    fastLookup,
		masterRepo:    repository.NewMasterRepository(db),
		followerRepo:  repository.NewFollowerRepository(db),
		jwtSecret:     []byte(jwtSecret),
	}
}

// generateID generates a unique ID with prefix
func generateID(prefix string) string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(bytes))
}

// hashPassword hashes a password using bcrypt
func hashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), 10)
	return string(bytes), err
}

// parseAuthHeader extracts user ID from Authorization header
func parseAuthHeader(r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", fmt.Errorf("missing authorization header")
	}

	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 || parts[0] != "Bearer" {
		return "", fmt.Errorf("invalid authorization header format")
	}

	tokenString := parts[1]
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return []byte(jwtSecret), nil
	})

	if err != nil || !token.Valid {
		return "", fmt.Errorf("invalid token")
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return "", fmt.Errorf("invalid claims")
	}

	userID, ok := claims["user_id"].(string)
	if !ok {
		return "", fmt.Errorf("invalid user_id claim")
	}

	return userID, nil
}

// writeJSON writes JSON response
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// writeError writes error response
func writeError(w http.ResponseWriter, status int, errMsg string, code string) {
	resp := models.NewErrorResponse(errMsg, code, nil)
	writeJSON(w, status, resp)
}

// generateJWT generates a JWT token for a user
func (s *Server) generateJWT(userID, username, role string) (string, error) {
	claims := jwt.MapClaims{
		"user_id":  userID,
		"username": username,
		"role":     role,
		"exp":      time.Now().Add(tokenExpiry).Unix(),
		"iat":      time.Now().Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(s.jwtSecret)
}

// Login handles POST /api/auth/login
func (s *Server) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "METHOD_NOT_ALLOWED")
		return
	}

	var req models.LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request format", "INVALID_REQUEST")
		return
	}

	// Validate input
	if len(req.Username) < 3 || len(req.Username) > 50 {
		writeError(w, http.StatusBadRequest, "Username must be between 3 and 50 characters", "INVALID_USERNAME")
		return
	}
	if len(req.Password) < 6 {
		writeError(w, http.StatusBadRequest, "Password must be at least 6 characters", "INVALID_PASSWORD")
		return
	}

	// For demo purposes, accept default admin credentials
	// In production, this should query a users table
	if req.Username == "admin" && req.Password == "admin123" {
		userID := "user_admin"
		token, err := s.generateJWT(userID, req.Username, "admin")
		if err != nil {
			writeError(w, http.StatusInternalServerError, "Failed to generate token", "INTERNAL_ERROR")
			return
		}

		resp := models.LoginResponse{
			AccessToken: token,
			TokenType:  "Bearer",
			ExpiresIn:  int64(tokenExpiry.Seconds()),
			User: models.UserInfo{
				UserID:   userID,
				Username: req.Username,
				Role:     "admin",
			},
		}

		writeJSON(w, http.StatusOK, resp)
		return
	}

	// Also accept master account credentials for testing
	master, err := s.masterRepo.GetByAccountID(r.Context(), req.Username)
	if err == nil {
		// Verify password
		if err := bcrypt.CompareHashAndPassword([]byte(master.PasswordHash), []byte(req.Password)); err == nil {
			userID := master.ID
			token, err := s.generateJWT(userID, req.Username, "master")
			if err != nil {
				writeError(w, http.StatusInternalServerError, "Failed to generate token", "INTERNAL_ERROR")
				return
			}

			resp := models.LoginResponse{
				AccessToken: token,
				TokenType:  "Bearer",
				ExpiresIn:  int64(tokenExpiry.Seconds()),
				User: models.UserInfo{
					UserID:   master.ID,
					Username: req.Username,
					Role:     "master",
				},
			}

			writeJSON(w, http.StatusOK, resp)
			return
		}
	}

	// Also accept follower account credentials for testing
	follower, err := s.followerRepo.GetByAccountID(r.Context(), req.Username)
	if err == nil {
		// Verify password
		if err := bcrypt.CompareHashAndPassword([]byte(follower.PasswordHash), []byte(req.Password)); err == nil {
			userID := follower.ID
			token, err := s.generateJWT(userID, req.Username, "follower")
			if err != nil {
				writeError(w, http.StatusInternalServerError, "Failed to generate token", "INTERNAL_ERROR")
				return
			}

			resp := models.LoginResponse{
				AccessToken: token,
				TokenType:  "Bearer",
				ExpiresIn:  int64(tokenExpiry.Seconds()),
				User: models.UserInfo{
					UserID:   follower.ID,
					Username: req.Username,
					Role:     "follower",
				},
			}

			writeJSON(w, http.StatusOK, resp)
			return
		}
	}

	writeError(w, http.StatusUnauthorized, "Invalid username or password", "INVALID_CREDENTIALS")
}

// CreateMaster handles POST /api/masters
func (s *Server) CreateMaster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "METHOD_NOT_ALLOWED")
		return
	}

	var req models.CreateMasterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request format", "INVALID_REQUEST")
		return
	}

	// Validate input
	if len(req.AccountID) < 4 || len(req.AccountID) > 50 {
		writeError(w, http.StatusBadRequest, "Account ID must be between 4 and 50 characters", "INVALID_ACCOUNT_ID")
		return
	}
	if len(req.Password) < 8 {
		writeError(w, http.StatusBadRequest, "Password must be at least 8 characters", "INVALID_PASSWORD")
		return
	}
	if len(req.Server) == 0 || len(req.Server) > 100 {
		writeError(w, http.StatusBadRequest, "Server is required and must be less than 100 characters", "INVALID_SERVER")
		return
	}

	// Hash password
	passwordHash, err := hashPassword(req.Password)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to process password", "INTERNAL_ERROR")
		return
	}

	// Create master
	master := &models.Master{
		ID:           generateID("m"),
		AccountID:    req.AccountID,
		PasswordHash: passwordHash,
		Server:      req.Server,
	}

	if err := s.masterRepo.Create(r.Context(), master); err != nil {
		if err == repository.ErrDuplicateAccountID {
			writeError(w, http.StatusConflict, "Account ID already registered as master", "DUPLICATE_ACCOUNT")
			return
		}
		writeError(w, http.StatusInternalServerError, "Failed to create master", "INTERNAL_ERROR")
		return
	}

	// Update in-memory cache
	s.masterCache.Set(master.AccountID, master.ID)

	// Register in FastLookupStore for O(1) signal routing
	if s.fastLookup != nil {
		if err := s.fastLookup.RegisterMaster(r.Context(), master); err != nil {
			log.Printf("Warning: Failed to register master in FastLookupStore: %v", err)
		}
	}

	resp := models.CreateMasterResponse{
		MasterID:  master.ID,
		AccountID: master.AccountID,
		Server:    master.Server,
		CreatedAt: master.CreatedAt,
	}

	writeJSON(w, http.StatusCreated, resp)
}

// RegisterFollower handles POST /api/masters/{masterId}/followers
func (s *Server) RegisterFollower(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "METHOD_NOT_ALLOWED")
		return
	}

	// Extract masterId from path
	path := r.URL.Path
	parts := strings.Split(path, "/")
	if len(parts) < 4 {
		writeError(w, http.StatusBadRequest, "Invalid path", "INVALID_PATH")
		return
	}
	masterId := parts[3]

	// Verify master exists
	exists, err := s.masterRepo.Exists(r.Context(), masterId)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to verify master", "INTERNAL_ERROR")
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "Master not found", "MASTER_NOT_FOUND")
		return
	}

	var req models.CreateFollowerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request format", "INVALID_REQUEST")
		return
	}

	// Validate input
	if len(req.AccountID) < 4 || len(req.AccountID) > 50 {
		writeError(w, http.StatusBadRequest, "Account ID must be between 4 and 50 characters", "INVALID_ACCOUNT_ID")
		return
	}
	if len(req.Password) < 8 {
		writeError(w, http.StatusBadRequest, "Password must be at least 8 characters", "INVALID_PASSWORD")
		return
	}
	if req.LotMultiplier < 0.01 || req.LotMultiplier > 100.0 {
		writeError(w, http.StatusBadRequest, "Lot multiplier must be between 0.01 and 100.0", "INVALID_LOT_MULTIPLIER")
		return
	}

	// Hash password
	passwordHash, err := hashPassword(req.Password)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to process password", "INTERNAL_ERROR")
		return
	}

	// Create follower
	follower := &models.Follower{
		ID:            generateID("f"),
		MasterID:      masterId,
		AccountID:    req.AccountID,
		PasswordHash: passwordHash,
		Server:       req.Server,
		Status:       string(models.FollowerStatusInactive),
		LotMultiplier: req.LotMultiplier,
	}

	if err := s.followerRepo.Create(r.Context(), follower); err != nil {
		if err == repository.ErrDuplicateFollower {
			writeError(w, http.StatusConflict, "Account already registered as follower for this master", "DUPLICATE_FOLLOWER")
			return
		}
		writeError(w, http.StatusInternalServerError, "Failed to create follower", "INTERNAL_ERROR")
		return
	}

	// Register in FastLookupStore for O(1) signal routing
	if s.fastLookup != nil {
		if err := s.fastLookup.RegisterFollower(r.Context(), masterId, follower); err != nil {
			log.Printf("Warning: Failed to register follower in FastLookupStore: %v", err)
		}
	}

	resp := models.CreateFollowerResponse{
		FollowerID:   follower.ID,
		MasterID:    follower.MasterID,
		AccountID:   follower.AccountID,
		Server:      follower.Server,
		Status:      follower.Status,
		LotMultiplier: follower.LotMultiplier,
		CreatedAt:   follower.CreatedAt,
	}

	writeJSON(w, http.StatusCreated, resp)
}

// StartFollowing handles POST /api/followers/{followerId}/start
func (s *Server) StartFollowing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "METHOD_NOT_ALLOWED")
		return
	}

	// Extract followerId from path
	path := r.URL.Path
	parts := strings.Split(path, "/")
	if len(parts) < 4 {
		writeError(w, http.StatusBadRequest, "Invalid path", "INVALID_PATH")
		return
	}
	followerId := parts[2]

	// Get follower
	follower, err := s.followerRepo.GetByID(r.Context(), followerId)
	if err != nil {
		if err == repository.ErrNotFound {
			writeError(w, http.StatusNotFound, "Follower not found", "FOLLOWER_NOT_FOUND")
			return
		}
		writeError(w, http.StatusInternalServerError, "Failed to get follower", "INTERNAL_ERROR")
		return
	}

	// Update status in database
	if err := s.followerRepo.UpdateStatus(r.Context(), followerId, string(models.FollowerStatusActive)); err != nil {
		if err == repository.ErrNotFound {
			writeError(w, http.StatusNotFound, "Follower not found", "FOLLOWER_NOT_FOUND")
			return
		}
		writeError(w, http.StatusInternalServerError, "Failed to start following", "INTERNAL_ERROR")
		return
	}

	// Add to Redis subscription set
	if err := s.subStore.AddFollower(r.Context(), follower.MasterID, follower.AccountID); err != nil {
		log.Printf("Warning: Failed to add follower to Redis: %v", err)
	}

	// Update status in FastLookupStore for O(1) signal routing
	if s.fastLookup != nil {
		if err := s.fastLookup.UpdateFollowerStatus(r.Context(), followerId, string(models.FollowerStatusActive)); err != nil {
			log.Printf("Warning: Failed to update follower status in FastLookupStore: %v", err)
		}
	}

	resp := models.StartStopResponse{
		FollowerID: followerId,
		MasterID:   follower.MasterID,
		Status:     string(models.FollowerStatusActive),
		Message:    "Copy trading activated",
	}

	writeJSON(w, http.StatusOK, resp)
}

// StopFollowing handles POST /api/followers/{followerId}/stop
func (s *Server) StopFollowing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "METHOD_NOT_ALLOWED")
		return
	}

	// Extract followerId from path
	path := r.URL.Path
	parts := strings.Split(path, "/")
	if len(parts) < 4 {
		writeError(w, http.StatusBadRequest, "Invalid path", "INVALID_PATH")
		return
	}
	followerId := parts[2]

	// Get follower
	follower, err := s.followerRepo.GetByID(r.Context(), followerId)
	if err != nil {
		if err == repository.ErrNotFound {
			writeError(w, http.StatusNotFound, "Follower not found", "FOLLOWER_NOT_FOUND")
			return
		}
		writeError(w, http.StatusInternalServerError, "Failed to get follower", "INTERNAL_ERROR")
		return
	}

	// Update status in database
	if err := s.followerRepo.UpdateStatus(r.Context(), followerId, string(models.FollowerStatusInactive)); err != nil {
		if err == repository.ErrNotFound {
			writeError(w, http.StatusNotFound, "Follower not found", "FOLLOWER_NOT_FOUND")
			return
		}
		writeError(w, http.StatusInternalServerError, "Failed to stop following", "INTERNAL_ERROR")
		return
	}

	// Remove from Redis subscription set
	if err := s.subStore.RemoveFollower(r.Context(), follower.MasterID, follower.AccountID); err != nil {
		log.Printf("Warning: Failed to remove follower from Redis: %v", err)
	}

	// Update status in FastLookupStore for O(1) signal routing
	if s.fastLookup != nil {
		if err := s.fastLookup.UpdateFollowerStatus(r.Context(), followerId, string(models.FollowerStatusInactive)); err != nil {
			log.Printf("Warning: Failed to update follower status in FastLookupStore: %v", err)
		}
	}

	resp := models.StartStopResponse{
		FollowerID: followerId,
		MasterID:   follower.MasterID,
		Status:     string(models.FollowerStatusInactive),
		Message:    "Copy trading deactivated",
	}

	writeJSON(w, http.StatusOK, resp)
}

// DeleteMaster handles DELETE /api/masters/{masterId}
func (s *Server) DeleteMaster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "METHOD_NOT_ALLOWED")
		return
	}

	// Extract masterId from path
	path := r.URL.Path
	parts := strings.Split(path, "/")
	if len(parts) < 4 {
		writeError(w, http.StatusBadRequest, "Invalid path", "INVALID_PATH")
		return
	}
	masterId := parts[3]

	// Get master to remove from cache
	master, err := s.masterRepo.GetByID(r.Context(), masterId)
	if err != nil {
		if err == repository.ErrNotFound {
			writeError(w, http.StatusNotFound, "Master not found", "MASTER_NOT_FOUND")
			return
		}
		writeError(w, http.StatusInternalServerError, "Failed to get master", "INTERNAL_ERROR")
		return
	}

	// Delete master (cascades to followers)
	affectedFollowers, err := s.masterRepo.SoftDelete(r.Context(), masterId)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to delete master", "INTERNAL_ERROR")
		return
	}

	// Remove from cache
	s.masterCache.Delete(master.AccountID)

	// Clear Redis subscriptions
	if err := s.subStore.ClearMasterFollowers(r.Context(), masterId); err != nil {
		log.Printf("Warning: Failed to clear Redis followers: %v", err)
	}

	// Note: FastLookupStore entries are not deleted here
	// They remain for historical tracking. If needed, add cleanup logic here.

	resp := models.DeleteMasterResponse{
		MasterID: masterId,
		Message:  fmt.Sprintf("Master deleted, %d followers deactivated", affectedFollowers),
	}

	writeJSON(w, http.StatusOK, resp)
}

// DeleteFollower handles DELETE /api/followers/{followerId}
func (s *Server) DeleteFollower(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "METHOD_NOT_ALLOWED")
		return
	}

	// Extract followerId from path
	path := r.URL.Path
	parts := strings.Split(path, "/")
	if len(parts) < 4 {
		writeError(w, http.StatusBadRequest, "Invalid path", "INVALID_PATH")
		return
	}
	followerId := parts[2]

	// Get follower
	follower, err := s.followerRepo.GetByID(r.Context(), followerId)
	if err != nil {
		if err == repository.ErrNotFound {
			writeError(w, http.StatusNotFound, "Follower not found", "FOLLOWER_NOT_FOUND")
			return
		}
		writeError(w, http.StatusInternalServerError, "Failed to get follower", "INTERNAL_ERROR")
		return
	}

	// Delete follower
	if err := s.followerRepo.SoftDelete(r.Context(), followerId); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to delete follower", "INTERNAL_ERROR")
		return
	}

	// Remove from Redis if was active
	if follower.Status == string(models.FollowerStatusActive) {
		if err := s.subStore.RemoveFollower(r.Context(), follower.MasterID, follower.AccountID); err != nil {
			log.Printf("Warning: Failed to remove follower from Redis: %v", err)
		}
	}

	// Unregister from FastLookupStore
	if s.fastLookup != nil {
		if err := s.fastLookup.UnregisterFollower(r.Context(), followerId); err != nil {
			log.Printf("Warning: Failed to unregister follower from FastLookupStore: %v", err)
		}
	}

	resp := models.DeleteFollowerResponse{
		FollowerID: followerId,
		Message:    "Follower deleted",
	}

	writeJSON(w, http.StatusOK, resp)
}

// GetMasterFollowers handles GET /api/masters/{masterId}/followers
func (s *Server) GetMasterFollowers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "METHOD_NOT_ALLOWED")
		return
	}

	// Extract masterId from path
	path := r.URL.Path
	parts := strings.Split(path, "/")
	if len(parts) < 4 {
		writeError(w, http.StatusBadRequest, "Invalid path", "INVALID_PATH")
		return
	}
	masterId := parts[3]

	// Verify master exists
	exists, err := s.masterRepo.Exists(r.Context(), masterId)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to verify master", "INTERNAL_ERROR")
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "Master not found", "MASTER_NOT_FOUND")
		return
	}

	// Get counts
	activeCount, err := s.followerRepo.CountActiveByMaster(r.Context(), masterId)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to get follower count", "INTERNAL_ERROR")
		return
	}

	totalCount, err := s.followerRepo.CountTotalByMaster(r.Context(), masterId)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to get follower count", "INTERNAL_ERROR")
		return
	}

	resp := models.MasterStats{
		MasterID:        masterId,
		ActiveFollowers: activeCount,
		TotalFollowers:  totalCount,
	}

	writeJSON(w, http.StatusOK, resp)
}

// HealthCheck handles GET /health
func (s *Server) HealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "METHOD_NOT_ALLOWED")
		return
	}

	components := make(map[string]string)
	stats := models.HealthStats{}

	// Check database
	if err := s.db.PingContext(r.Context()); err != nil {
		components["database"] = "disconnected"
	} else {
		components["database"] = "connected"
		stats.ActiveMasters, _ = s.masterRepo.CountActive(r.Context())
		stats.ActiveFollowers, _ = s.followerRepo.CountActive(r.Context())
	}

	// Check Redis
	if err := s.subStore.HealthCheck(r.Context()); err != nil {
		components["redis"] = "disconnected"
	} else {
		components["redis"] = "connected"
	}

	// Check master cache
	if s.masterCache.Size() > 0 {
		components["masterCache"] = "loaded"
	} else {
		components["masterCache"] = "empty"
	}

	status := "running"
	if components["database"] != "connected" || components["redis"] != "connected" {
		status = "degraded"
	}

	resp := models.HealthResponse{
		Status:     status,
		Version:    "2.0.0",
		Components: components,
		Stats:      stats,
	}

	writeJSON(w, http.StatusOK, resp)
}
