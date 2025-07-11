package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const htmlTemplate = `
<!DOCTYPE html>
<html>
<head>
    <title>Agent Data Monitor</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { display: flex; justify-content: space-between; margin-bottom: 20px; }
        .stats { background: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
        .stat-item { margin: 5px 0; }
        .buttons { margin-bottom: 20px; display: flex; gap: 10px; flex-wrap: wrap; }
        button { padding: 8px 15px; cursor: pointer; }
        .primary { background-color: #4CAF50; color: white; border: none; }
        .danger { background-color: #f44336; color: white; border: none; }
        .secondary { background-color: #008CBA; color: white; border: none; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background-color: #f2f2f2; }
        tr:hover { background-color: #f5f5f5; }
        .dirty { color: #d32f2f; font-weight: bold; }
        .clean { color: #388e3c; }
        .loading { display: none; margin: 20px 0; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Agent Data Monitor</h1>
            <div>
                <span id="time">{{.Uptime}}</span> uptime
            </div>
        </div>

        <div class="stats">
            <div class="stat-item"><strong>Total items in cache:</strong> {{.TotalItems}}</div>
            <div class="stat-item"><strong>Dirty items:</strong> {{.DirtyItems}}</div>
            <div class="stat-item"><strong>Last flush:</strong> {{.LastFlush}}</div>
            <div class="stat-item"><strong>MongoDB:</strong> {{.MongoStatus}}</div>
            <div class="stat-item"><strong>Source collection:</strong> {{.SourceCollection}}</div>
            <div class="stat-item"><strong>Processed collection:</strong> {{.ProcessedCollection}}</div>
        </div>

        <div class="buttons">
            <button onclick="flushCache()" class="primary">Flush Cache to DB</button>
            <button onclick="dropCache()" class="danger">Drop Cache</button>
            <button onclick="processData()" class="secondary">Transform Data</button>
            <button onclick="showProcessedData()" class="secondary">Show Processed Data</button>
            <button onclick="refreshData()">Refresh</button>
        </div>

        <div id="loading" class="loading">
            <p>Processing data, please wait...</p>
            <progress id="progressBar" value="0" max="100"></progress>
        </div>

        <div id="dataTables">
            {{if .ShowProcessedData}}
            <h2>Processed Connections ({{len .ProcessedConnections}} items)</h2>
            <table>
                <thead>
                    <tr>
                        <th>Local Address</th>
                        <th>Remote Address</th>
                        <th>Relation</th>
                        <th>Service</th>
                        <th>Status</th>
                        <th>Timestamp</th>
                    </tr>
                </thead>
                <tbody>
                    {{range .ProcessedConnections}}
                    <tr>
                        <td>{{.LocalAddr.IP}} ({{.LocalAddr.Name}})</td>
                        <td>{{.RemoteAddr.IP}} ({{.RemoteAddr.Name}})</td>
                        <td>{{.Relation.Mode}}:{{.Relation.Port}}</td>
                        <td>{{.Options.Service}}</td>
                        <td>{{.Options.Status}}</td>
                        <td>{{.Timestamp | formatTime}}</td>
                    </tr>
                    {{end}}
                </tbody>
            </table>
            {{else}}
            <h2>Current Cache ({{len .CacheItems}} items)</h2>
            <table>
                <thead>
                    <tr>
                        <th>Local Address</th>
                        <th>Remote Address</th>
                        <th>Relation</th>
                        <th>Service</th>
                        <th>Status</th>
                        <th>Timestamp</th>
                        <th>State</th>
                    </tr>
                </thead>
                <tbody>
                    {{range .CacheItems}}
                    <tr>
                        <td>{{.Local}}</td>
                        <td>{{.Remote}}</td>
                        <td>{{.Relation}}</td>
                        <td>{{.Service}}</td>
                        <td>{{.Status}}</td>
                        <td>{{.Timestamp.Format "2006-01-02 15:04:05"}}</td>
                        <td class="{{if .Dirty}}dirty{{else}}clean{{end}}">
                            {{if .Dirty}}DIRTY{{else}}clean{{end}}
                        </td>
                    </tr>
                    {{end}}
                </tbody>
            </table>
            {{end}}
        </div>
    </div>

    <script>
        function showLoading(show) {
            document.getElementById('loading').style.display = show ? 'block' : 'none';
        }

        function processData() {
            showLoading(true);
            fetch('/api/v1/process', { method: 'POST' })
                .then(response => response.json())
                .then(data => {
                    showLoading(false);
                    if(data.status === 'started') {
                        alert('Data transformation started successfully');
                        setTimeout(refreshData, 3000);
                    } else {
                        alert('Error: ' + (data.error || 'Unknown error'));
                    }
                })
                .catch(err => {
                    showLoading(false);
                    alert('Error: ' + err);
                });
        }

        function showProcessedData() {
            window.location.href = '/?show=processed';
        }

        function flushCache() {
            fetch('/api/v1/cache/flush', { method: 'POST' })
                .then(response => response.json())
                .then(data => {
                    if(data.status === 'success') {
                        alert('Cache flushed successfully! Dirty items: ' + data.dirty_items);
                        refreshData();
                    } else {
                        alert('Error: ' + (data.error || 'Unknown error'));
                    }
                })
                .catch(err => alert('Error: ' + err));
        }

        function dropCache() {
            if(confirm('Are you sure you want to DROP ALL CACHE? This cannot be undone!')) {
                fetch('/api/v1/cache/drop', { method: 'POST' })
                    .then(response => response.json())
                    .then(data => {
                        if(data.status === 'success') {
                            alert('Cache dropped successfully! Removed ' + data.deleted + ' items');
                            refreshData();
                        } else {
                            alert('Error: ' + (data.error || 'Unknown error'));
                        }
                    })
                    .catch(err => alert('Error: ' + err));
            }
        }

        function refreshData() {
            location.reload();
        }

        // Auto-refresh every 30 seconds
        setTimeout(refreshData, 30000);
    </script>
</body>
</html>`

type Config struct {
	Server struct {
		Host         string        `json:"host"`
		Port         string        `json:"port"`
		WriteTimeout time.Duration `json:"write_timeout"`
		ReadTimeout  time.Duration `json:"read_timeout"`
	} `json:"server"`
	MongoDB struct {
		URI           string `json:"uri"`
		Database      string `json:"database"`
		Collection    string `json:"collection"`
		NewCollection string `json:"new_collection"`
	} `json:"mongodb"`
	Cache struct {
		FlushInterval        time.Duration `json:"flush_interval"`
		LoadDataOlderThan    time.Duration `json:"load_data_older_than"`
		InitialLoadBatchSize int           `json:"initial_load_batch_size"`
	} `json:"cache"`
}

type NetworkAddr struct {
	IP   string `bson:"ip" json:"ip"`
	Name string `bson:"name" json:"name"`
	Port int    `bson:"port" json:"port"`
}

type Relation struct {
	Mode     string `bson:"mode" json:"mode"`
	Type     string `bson:"type" json:"type"`
	Result   int    `bson:"result" json:"result"`
	Response int    `bson:"response" json:"response"`
	Trace    int    `bson:"trace" json:"trace"`
	Port     int    `bson:"port" json:"port"`
}

type Options struct {
	Service     string `bson:"service" json:"service"`
	Status      string `bson:"status" json:"status"`
	Timeout     int    `bson:"timeout" json:"timeout"`
	MaxRespTime int    `bson:"max_resp_time" json:"maxRespTime"`
	AccountID   int    `bson:"account_id" json:"accountID"`
}

type AgentData struct {
	LocalAddr  NetworkAddr `bson:"localAddr" json:"localAddr"`
	RemoteAddr NetworkAddr `bson:"remoteAddr" json:"remoteAddr"`
	Relation   Relation    `bson:"relation" json:"relation"`
	Options    Options     `bson:"options" json:"options"`
	Timestamp  int64       `bson:"timestamp" json:"timestamp"`
	HashID     string      `bson:"hash_id" json:"hash_id"`
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

type CacheView struct {
	HashID    string
	Local     string
	Remote    string
	Relation  string
	Service   string
	Status    string
	Timestamp time.Time
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
	startTime   time.Time
}

func NewServer(config *Config) *Server {
	return &Server{
		config:    config,
		cache:     make(map[string]*CacheItem),
		logger:    log.New(os.Stdout, "[SERVER] ", log.LstdFlags|log.Lmsgprefix),
		startTime: time.Now(),
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
	mux.HandleFunc("/health", s.handleHealthCheck)
	mux.HandleFunc("/api/v1/cache/flush", s.handleFlushCache)
	mux.HandleFunc("/api/v1/cache/drop", s.handleDropCache)
	mux.HandleFunc("/api/v1/process", s.handleProcessData)
	mux.HandleFunc("/", s.handleWeb)

	addr := s.config.Server.Host + ":" + s.config.Server.Port
	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      mux,
		WriteTimeout: s.config.Server.WriteTimeout * time.Second,
		ReadTimeout:  s.config.Server.ReadTimeout * time.Second,
	}

	s.logger.Printf("Server initialized with %d items in cache", len(s.cache))
	return nil
}

func (s *Server) Run() error {
	go s.flushCachePeriodically()

	addr := s.httpServer.Addr
	s.logger.Printf("Starting server on http://%s", addr)
	s.logger.Printf("Available endpoints:")
	s.logger.Printf("  - GET  http://%s/", addr)
	s.logger.Printf("  - GET  http://%s/health", addr)
	s.logger.Printf("  - POST http://%s/api/v1/data", addr)
	s.logger.Printf("  - GET  http://%s/api/v1/cache", addr)
	s.logger.Printf("  - GET  http://%s/api/v1/cache/stats", addr)
	s.logger.Printf("  - POST http://%s/api/v1/cache/flush", addr)
	s.logger.Printf("  - POST http://%s/api/v1/cache/drop", addr)
	s.logger.Printf("  - POST http://%s/api/v1/process", addr)

	return s.httpServer.ListenAndServe()
}

func (s *Server) transformData(rawData bson.M) (bson.M, error) {
	localPort, lok := rawData["local_addr"].(bson.M)["port"].(int32)
	remotePort, rok := rawData["remote_addr"].(bson.M)["port"].(int32)
	if !lok || !rok {
		return nil, fmt.Errorf("invalid port types")
	}

	isVirtualPort := func(port int32) bool {
		return port >= 32768 && port <= 60999
	}

	var relationPort int32
	switch {
	case !isVirtualPort(localPort) && !isVirtualPort(remotePort):

		relationPort = remotePort
	case isVirtualPort(localPort) && !isVirtualPort(remotePort):

		relationPort = 5000
	case !isVirtualPort(localPort) && isVirtualPort(remotePort):

		relationPort = 0
	default:

		relationPort = 0
	}

	localIP := rawData["local_addr"].(bson.M)["ip"].(string)
	localName := rawData["local_addr"].(bson.M)["name"].(string)
	remoteIP := rawData["remote_addr"].(bson.M)["ip"].(string)
	remoteName := rawData["remote_addr"].(bson.M)["name"].(string)
	mode := rawData["relation"].(bson.M)["mode"].(string)

	if localName == "" {
		localName = localIP
	}
	if remoteName == "" {
		remoteName = remoteIP
	}

	return bson.M{
		"localAddr": bson.M{
			"ip":   localIP,
			"name": localName,
		},
		"remoteAddr": bson.M{
			"ip":   remoteIP,
			"name": remoteName,
		},
		"relation": bson.M{
			"mode":     mode,
			"port":     relationPort,
			"result":   rawData["relation"].(bson.M)["result"],
			"response": rawData["relation"].(bson.M)["response"],
			"trace":    rawData["relation"].(bson.M)["trace"],
		},
		"options": bson.M{
			"service":     rawData["options"].(bson.M)["service"],
			"status":      rawData["options"].(bson.M)["status"],
			"timeout":     rawData["options"].(bson.M)["timeout"],
			"maxRespTime": rawData["options"].(bson.M)["max_resp_time"],
			"accountID":   rawData["options"].(bson.M)["account_id"],
		},
		"timestamp": rawData["timestamp"],
		"hash_id":   rawData["hash_id"],
	}, nil
}

func (s *Server) ProcessRawCollection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	rawCollection := s.mongoClient.Database(s.config.MongoDB.Database).Collection(s.config.MongoDB.Collection)
	processedCollection := s.mongoClient.Database(s.config.MongoDB.Database).Collection(s.config.MongoDB.NewCollection)

	filter := bson.M{
		"timestamp": bson.M{
			"$gt": time.Now().Add(-time.Hour * time.Duration(s.config.Cache.LoadDataOlderThan)).Unix(),
		},
	}

	cur, err := rawCollection.Find(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to find raw data: %v", err)
	}
	defer cur.Close(ctx)

	processed := 0
	failed := 0

	for cur.Next(ctx) {
		var rawData bson.M
		if err := cur.Decode(&rawData); err != nil {
			s.logger.Printf("Error decoding raw document: %v", err)
			failed++
			continue
		}

		transformed, err := s.transformData(rawData)
		if err != nil {
			s.logger.Printf("Error transforming data: %v", err)
			failed++
			continue
		}

		findFilter := bson.M{
			"localAddr.ip":   transformed["localAddr"].(bson.M)["ip"],
			"localAddr.name": transformed["localAddr"].(bson.M)["name"],
			"remoteAddr.ip":  transformed["remoteAddr"].(bson.M)["ip"],
			"relation.mode":  transformed["relation"].(bson.M)["mode"],
			"relation.port":  transformed["relation"].(bson.M)["port"],
		}

		var existingDoc bson.M
		err = processedCollection.FindOne(ctx, findFilter).Decode(&existingDoc)
		if err == nil {

			update := bson.M{
				"$set": bson.M{
					"timestamp": transformed["timestamp"],
					"updatedAt": time.Now().Unix(),
				},
			}
			_, err = processedCollection.UpdateOne(ctx, findFilter, update)
			if err != nil {
				s.logger.Printf("Error updating document: %v", err)
				failed++
				continue
			}
		} else if err == mongo.ErrNoDocuments {

			transformed["createdAt"] = time.Now().Unix()
			transformed["updatedAt"] = time.Now().Unix()
			_, err = processedCollection.InsertOne(ctx, transformed)
			if err != nil {
				s.logger.Printf("Error inserting document: %v", err)
				failed++
				continue
			}
		} else {

			s.logger.Printf("Error finding document: %v", err)
			failed++
			continue
		}

		processed++
		if processed%1000 == 0 {
			s.logger.Printf("Processed %d documents (%d failed)", processed, failed)
		}
	}

	s.logger.Printf("Processing completed. Total: %d, Processed: %d, Failed: %d",
		processed+failed, processed, failed)
	return nil
}

func (s *Server) handleProcessData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	go func() {
		startTime := time.Now()
		s.logger.Printf("Starting data processing from %s to %s",
			s.config.MongoDB.Collection, s.config.MongoDB.NewCollection)

		if err := s.ProcessRawCollection(); err != nil {
			s.logger.Printf("Data processing failed: %v", err)
		} else {
			s.logger.Printf("Data processing completed in %v", time.Since(startTime))
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "started",
		"message": fmt.Sprintf("Processing data from %s to %s",
			s.config.MongoDB.Collection, s.config.MongoDB.NewCollection),
	})
}

func (s *Server) getProcessedData() ([]AgentData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	collection := s.mongoClient.Database(s.config.MongoDB.Database).Collection(s.config.MongoDB.NewCollection)

	cur, err := collection.Find(ctx, bson.M{}, options.Find().SetLimit(1000).SetSort(bson.M{"timestamp": -1}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var results []AgentData
	for cur.Next(ctx) {
		var item AgentData
		if err := cur.Decode(&item); err != nil {
			s.logger.Printf("Error decoding processed data: %v", err)
			continue
		}
		results = append(results, item)
	}

	return results, nil
}

func (s *Server) handleWeb(w http.ResponseWriter, r *http.Request) {
	showProcessed := r.URL.Query().Get("show") == "processed"

	data := struct {
		CacheItems           []CacheView
		ProcessedConnections []AgentData
		TotalItems           int
		DirtyItems           int
		LastFlush            string
		Uptime               string
		MongoStatus          string
		SourceCollection     string
		ProcessedCollection  string
		ShowProcessedData    bool
	}{
		SourceCollection:    s.config.MongoDB.Collection,
		ProcessedCollection: s.config.MongoDB.NewCollection,
		ShowProcessedData:   showProcessed,
	}

	if showProcessed {
		processedData, err := s.getProcessedData()
		if err != nil {
			s.logger.Printf("Error getting processed data: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		data.ProcessedConnections = processedData
	} else {
		s.cacheMutex.RLock()
		data.CacheItems = s.prepareCacheView()
		data.TotalItems = len(s.cache)
		data.DirtyItems = countDirtyItems(s.cache)
		s.cacheMutex.RUnlock()
	}

	data.LastFlush = s.lastFlush.Format("2006-01-02 15:04:05")
	data.Uptime = time.Since(s.startTime).Round(time.Second).String()
	data.MongoStatus = s.checkMongoStatus()

	tmpl := template.Must(template.New("status").Funcs(template.FuncMap{
		"formatTime": func(t int64) string {
			return time.Unix(t, 0).Format("2006-01-02 15:04:05")
		},
	}).Parse(htmlTemplate))

	if err := tmpl.Execute(w, data); err != nil {
		s.logger.Printf("Error executing template: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func (s *Server) prepareCacheView() []CacheView {
	var cacheView []CacheView
	for _, item := range s.cache {
		cacheView = append(cacheView, CacheView{
			HashID:    item.HashID[:8] + "...",
			Local:     fmt.Sprintf("%s:%d", item.Data.LocalAddr.IP, item.Data.LocalAddr.Port),
			Remote:    fmt.Sprintf("%s:%d", item.Data.RemoteAddr.IP, item.Data.RemoteAddr.Port),
			Relation:  fmt.Sprintf("%s/%s", item.Data.Relation.Mode, item.Data.Relation.Type),
			Service:   item.Data.Options.Service,
			Status:    item.Data.Options.Status,
			Timestamp: time.Unix(item.Timestamp, 0),
			Dirty:     item.Dirty,
		})
	}

	sort.Slice(cacheView, func(i, j int) bool {
		return cacheView[i].Timestamp.After(cacheView[j].Timestamp)
	})

	return cacheView
}

func countDirtyItems(cache map[string]*CacheItem) int {
	count := 0
	for _, item := range cache {
		if item.Dirty {
			count++
		}
	}
	return count
}

func (s *Server) checkMongoStatus() string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := s.mongoClient.Ping(ctx, nil); err != nil {
		return "disconnected"
	}
	return "connected"
}

func (s *Server) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	status := "ok"
	if err := s.mongoClient.Ping(ctx, nil); err != nil {
		status = "mongo_unavailable"
		s.logger.Printf("Health check failed: MongoDB ping error: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  status,
		"version": "1.0",
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) loadInitialCache() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collection := s.mongoClient.Database(s.config.MongoDB.Database).Collection(s.config.MongoDB.Collection)

	filter := bson.M{
		"timestamp": bson.M{
			"$gt": time.Now().Add(-time.Hour * time.Duration(s.config.Cache.LoadDataOlderThan)).Unix(),
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

func generateHashID(data AgentData) string {
	hashInput := fmt.Sprintf("%s:%d:%s:%d",
		data.LocalAddr.IP,
		data.LocalAddr.Port,
		data.RemoteAddr.IP,
		data.RemoteAddr.Port)

	hash := sha256.Sum256([]byte(hashInput))
	return hex.EncodeToString(hash[:])
}

func (s *Server) handleGetCache(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	s.logger.Printf("GET /api/v1/cache request. Query name='%s'", name)

	s.cacheMutex.RLock()
	defer s.cacheMutex.RUnlock()

	var result []AgentData
	now := time.Now().Unix()
	maxAgeSeconds := int64(s.config.Cache.LoadDataOlderThan) * 3600

	for _, item := range s.cache {
		if name != "" && item.Data.LocalAddr.Name != name {
			continue
		}
		if now-item.Timestamp <= maxAgeSeconds {
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
			s.logger.Printf("Flushing item: %s -> %s:%d to %s:%d (service: %s)",
				item.HashID[:8],
				item.Data.LocalAddr.IP, item.Data.LocalAddr.Port,
				item.Data.RemoteAddr.IP, item.Data.RemoteAddr.Port,
				item.Data.Options.Service)

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

func (s *Server) handleFlushCache(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.logger.Println("Received manual cache flush request")

	err := s.flushDirtyItems()
	if err != nil {
		s.logger.Printf("Manual cache flush failed: %v", err)
		http.Error(w, fmt.Sprintf("Flush failed: %v", err), http.StatusInternalServerError)
		return
	}

	s.cacheMutex.Lock()
	dirtyCount := countDirtyItems(s.cache)
	s.cacheMutex.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "success",
		"flushed":     true,
		"dirty_items": dirtyCount,
		"total_items": len(s.cache),
	})
}

func (s *Server) handleDropCache(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.cacheMutex.Lock()
	deleted := len(s.cache)
	s.cache = make(map[string]*CacheItem)
	s.cacheMutex.Unlock()

	s.logger.Printf("Cache dropped manually, deleted %d items", deleted)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"deleted": deleted,
	})
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

	if config.Server.Host == "" {
		config.Server.Host = "127.0.0.1"
	}
	if config.Server.Port == "" {
		config.Server.Port = "8080"
	}
	if config.Cache.FlushInterval == 0 {
		config.Cache.FlushInterval = time.Minute
	}
	if config.Cache.LoadDataOlderThan == 0 {
		config.Cache.LoadDataOlderThan = 24
	}
	if config.Cache.InitialLoadBatchSize == 0 {
		config.Cache.InitialLoadBatchSize = 1000
	}
	if config.MongoDB.NewCollection == "" {
		config.MongoDB.NewCollection = "all_connections"
	}

	log.Printf("Starting server with configuration:")
	log.Printf("  - Server: %s:%s", config.Server.Host, config.Server.Port)
	log.Printf("  - MongoDB: %s/%s", config.MongoDB.URI, config.MongoDB.Database)
	log.Printf("  - Source collection: %s", config.MongoDB.Collection)
	log.Printf("  - Processed collection: %s", config.MongoDB.NewCollection)
	log.Printf("  - Cache flush interval: %v", config.Cache.FlushInterval)
	log.Printf("  - Load data older than: %v hours", config.Cache.LoadDataOlderThan)

	server := NewServer(&config)
	if err := server.Initialize(); err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}

	if err := server.Run(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}
