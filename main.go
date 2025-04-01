package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
	Server struct {
		Port         string        `json:"port"`
		WriteTimeout time.Duration `json:"write_timeout"`
		ReadTimeout  time.Duration `json:"read_timeout"`
	} `json:"server"`
	MongoDB struct {
		URI        string `json:"uri"`
		Database   string `json:"database"`
		Collection string `json:"collection"`
	} `json:"mongodb"`
	Cache struct {
		FlushInterval time.Duration `json:"flush_interval"`
		MaxCacheAge   time.Duration `json:"max_cache_age"`
	} `json:"cache"`
}

type AgentData struct {
	LocalAddr  NetworkAddr `json:"localAddr"`
	RemoteAddr NetworkAddr `json:"remoteAddr"`
	Relation   Relation    `json:"relation"`
	Options    Options     `json:"options"`
}

type NetworkAddr struct {
	IP   string `json:"ip"`
	Name string `json:"name"`
	Port int    `json:"port"`
}

type Relation struct {
	Mode     string `json:"mode"`
	Type     string `json:"type"`
	Result   int    `json:"result"`
	Response int    `json:"response"`
	Trace    int    `json:"trace"`
}

type Options struct {
	Service     string `json:"service"`
	Status      string `json:"status"`
	Timeout     int    `json:"timeout"`
	MaxRespTime int    `json:"maxRespTime"`
	AccountID   int    `json:"accountID"`
}

type Request struct {
	Data []AgentData `json:"data"`
}

type CacheItem struct {
	Data      AgentData
	Timestamp int64
	HashID    string
	Dirty     bool
}

type Server struct {
	config      *Config
	cache       map[string]*CacheItem
	cacheMutex  sync.RWMutex
	mongoClient *mongo.Client
	httpServer  *http.Server
	logger      *log.Logger
	lastFlush   time.Time
}

func NewServer(config *Config) *Server {
	return &Server{
		config: config,
		cache:  make(map[string]*CacheItem),
		logger: log.New(os.Stdout, "[SERVER] ", log.LstdFlags|log.Lmsgprefix),
	}
}

func (s *Server) Initialize() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(s.config.MongoDB.URI))
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to ping MongoDB: %v", err)
	}

	s.mongoClient = client

	if err := s.loadInitialCache(); err != nil {
		return fmt.Errorf("failed to load initial cache: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/data", s.handleData)
	mux.HandleFunc("/api/v1/cache", s.handleGetCache)
	mux.HandleFunc("/api/v1/cache/stats", s.handleCacheStats)

	s.httpServer = &http.Server{
		Addr:         ":" + s.config.Server.Port,
		Handler:      mux,
		WriteTimeout: s.config.Server.WriteTimeout * time.Second,
		ReadTimeout:  s.config.Server.ReadTimeout * time.Second,
	}

	s.logger.Printf("Server initialized with %d items in cache", len(s.cache))
	return nil
}

func (s *Server) loadInitialCache() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collection := s.mongoClient.Database(s.config.MongoDB.Database).Collection(s.config.MongoDB.Collection)

	filter := bson.M{
		"timestamp": bson.M{
			"$gt": time.Now().Add(-s.config.Cache.MaxCacheAge).Unix(),
		},
	}

	cur, err := collection.Find(ctx, filter)
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	s.cacheMutex.Lock()
	defer s.cacheMutex.Unlock()

	count := 0
	for cur.Next(ctx) {
		var doc struct {
			LocalAddr  NetworkAddr `bson:"local_addr"`
			RemoteAddr NetworkAddr `bson:"remote_addr"`
			Relation   Relation    `bson:"relation"`
			Options    Options     `bson:"options"`
			HashID     string      `bson:"hash_id"`
			Timestamp  int64       `bson:"timestamp"`
		}

		if err := cur.Decode(&doc); err != nil {
			s.logger.Printf("Error decoding document: %v", err)
			continue
		}

		s.cache[doc.HashID] = &CacheItem{
			Data: AgentData{
				LocalAddr:  doc.LocalAddr,
				RemoteAddr: doc.RemoteAddr,
				Relation:   doc.Relation,
				Options:    doc.Options,
			},
			Timestamp: doc.Timestamp,
			HashID:    doc.HashID,
			Dirty:     false,
		}
		count++
	}

	s.logger.Printf("Loaded %d items from MongoDB to cache", count)
	return nil
}

func (s *Server) Run() error {
	go s.flushCachePeriodically()
	s.logger.Printf("Starting server on port %s", s.config.Server.Port)
	return s.httpServer.ListenAndServe()
}

func (s *Server) handleData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Printf("Error decoding request: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	now := time.Now().Unix()
	added := 0
	updated := 0

	s.cacheMutex.Lock()
	defer s.cacheMutex.Unlock()

	for _, data := range req.Data {
		hashID := generateHashID(data)
		item, exists := s.cache[hashID]

		if !exists {
			s.cache[hashID] = &CacheItem{
				Data:      data,
				Timestamp: now,
				HashID:    hashID,
				Dirty:     true,
			}
			added++
		} else {
			item.Timestamp = now
			item.Dirty = true
			updated++
		}
	}

	s.logger.Printf("Processed POST request: added %d, updated %d items", added, updated)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"added":   added,
		"updated": updated,
		"total":   len(s.cache),
	})
}

func (s *Server) handleGetCache(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	s.logger.Printf("GET /api/v1/cache request. Query name='%s'", name)

	s.cacheMutex.RLock()
	defer s.cacheMutex.RUnlock()

	var result []AgentData
	now := time.Now().Unix()

	for _, item := range s.cache {
		if name != "" && item.Data.LocalAddr.Name != name {
			continue
		}
		if now-item.Timestamp <= int64(s.config.Cache.MaxCacheAge.Seconds()) {
			result = append(result, item.Data)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		s.logger.Printf("Error encoding response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func (s *Server) handleCacheStats(w http.ResponseWriter, r *http.Request) {
	s.cacheMutex.RLock()
	defer s.cacheMutex.RUnlock()

	stats := struct {
		Status     string `json:"status"`
		TotalItems int    `json:"total_items"`
		DirtyItems int    `json:"dirty_items"`
		LastFlush  string `json:"last_flush,omitempty"`
	}{
		Status:     "ok",
		TotalItems: len(s.cache),
	}

	if !s.lastFlush.IsZero() {
		stats.LastFlush = s.lastFlush.Format(time.RFC3339)
	}

	for _, item := range s.cache {
		if item.Dirty {
			stats.DirtyItems++
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) flushCachePeriodically() {
	ticker := time.NewTicker(s.config.Cache.FlushInterval)
	defer ticker.Stop()

	for range ticker.C {
		if err := s.flushDirtyItems(); err != nil {
			s.logger.Printf("Flush error: %v", err)
		}
	}
}

func (s *Server) flushDirtyItems() error {
	s.cacheMutex.Lock()
	defer s.cacheMutex.Unlock()

	if len(s.cache) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collection := s.mongoClient.Database(s.config.MongoDB.Database).Collection(s.config.MongoDB.Collection)
	var operations []mongo.WriteModel
	now := time.Now()
	flushCutoff := now.Add(-5 * time.Minute).Unix()

	for _, item := range s.cache {
		if item.Dirty && item.Timestamp >= flushCutoff {
			filter := bson.M{"hash_id": item.HashID}
			update := bson.M{
				"$setOnInsert": bson.M{
					"local_addr": bson.M{
						"ip":   item.Data.LocalAddr.IP,
						"name": item.Data.LocalAddr.Name,
						"port": item.Data.LocalAddr.Port,
					},
					"remote_addr": bson.M{
						"ip":   item.Data.RemoteAddr.IP,
						"name": item.Data.RemoteAddr.Name,
						"port": item.Data.RemoteAddr.Port,
					},
					"relation": bson.M{
						"mode":     item.Data.Relation.Mode,
						"type":     item.Data.Relation.Type,
						"result":   item.Data.Relation.Result,
						"response": item.Data.Relation.Response,
						"trace":    item.Data.Relation.Trace,
					},
					"options": bson.M{
						"service":       item.Data.Options.Service,
						"status":        item.Data.Options.Status,
						"timeout":       item.Data.Options.Timeout,
						"max_resp_time": item.Data.Options.MaxRespTime,
						"account_id":    item.Data.Options.AccountID,
					},
					"hash_id":    item.HashID,
					"timestamp":  item.Timestamp,
					"created_at": now.Unix(),
				},
			}

			operations = append(operations, mongo.NewUpdateOneModel().
				SetFilter(filter).
				SetUpdate(update).
				SetUpsert(true))
		}
	}

	if len(operations) == 0 {
		// s.logger.Println("No dirty items to flush")
		return nil
	}

	res, err := collection.BulkWrite(ctx, operations)
	if err != nil {
		return fmt.Errorf("mongo bulk write failed: %v", err)
	}

	for _, item := range s.cache {
		if item.Dirty && item.Timestamp >= flushCutoff {
			item.Dirty = false
		}
	}

	s.lastFlush = now
	s.logger.Printf("Flushed %d items (inserted: %d, modified: %d)",
		len(operations), res.InsertedCount, res.ModifiedCount)
	return nil
}

func generateHashID(data AgentData) string {
	hashInput := fmt.Sprintf("%s:%d:%s:%d",
		data.LocalAddr.IP,
		data.LocalAddr.Port,
		data.RemoteAddr.IP,
		data.RemoteAddr.Port)

	hash := sha256.Sum256([]byte(hashInput))
	return hex.EncodeToString(hash[:])
}

func main() {
	configFile, err := os.Open("config.json")
	if err != nil {
		log.Fatalf("Failed to open config file: %v", err)
	}
	defer configFile.Close()

	var config Config
	if err := json.NewDecoder(configFile).Decode(&config); err != nil {
		log.Fatalf("Failed to decode config: %v", err)
	}

	if config.Cache.FlushInterval == 0 {
		config.Cache.FlushInterval = time.Minute
	}
	if config.Cache.MaxCacheAge == 0 {
		config.Cache.MaxCacheAge = 24 * time.Hour
	}

	server := NewServer(&config)
	if err := server.Initialize(); err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}

	if err := server.Run(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}
