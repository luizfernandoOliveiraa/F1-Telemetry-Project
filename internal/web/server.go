package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"f1-telemetry/internal/config"
)

type ServerStatus struct {
	Capturing        bool      `json:"capturing"`
	SessionUID       uint64    `json:"session_uid"`
	TrackName        string    `json:"track_name"`
	SessionTypeName  string    `json:"session_type_name"`
	TotalLaps        uint8     `json:"total_laps"`
	LapsCompleted    uint8     `json:"laps_completed"`
	BestLapTime      string    `json:"best_lap_time"`
	BestSectors      string    `json:"best_sectors"`
	PacketsReceived  uint64    `json:"packets_received"`
	KafkaConnected   bool      `json:"kafka_connected"`
	StartTime        time.Time `json:"start_time"`
	VehicleName      string    `json:"vehicle_name"`
	UDPPacketStats   []uint64  `json:"udp_packet_stats"`
}

type WebServer struct {
	addr        string
	configPath  string
	cfg         *config.Config
	upgrader    websocket.Upgrader
	clients     map[*websocket.Conn]bool
	clientsMu   sync.Mutex
	broadcast   chan interface{}
	status      ServerStatus
	statusMu    sync.RWMutex
	
	// Callbacks to orchestrate UDP/Kafka starting/stopping
	OnStartCapture func(cfg *config.Config) error
	OnStopCapture  func()
}

func NewWebServer(addr string, configPath string, cfg *config.Config) *WebServer {
	return &WebServer{
		addr:       addr,
		configPath: configPath,
		cfg:        cfg,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all connections
			},
		},
		clients:   make(map[*websocket.Conn]bool),
		broadcast: make(chan interface{}, 100),
		status: ServerStatus{
			Capturing:       false,
			TrackName:       "Aguardando Jogo...",
			SessionTypeName: "Aguardando...",
			BestLapTime:     "--:--.---",
			BestSectors:     "S1: --.--- | S2: --.--- | S3: --.---",
			VehicleName:     "Carro Padrão",
			UDPPacketStats:  make([]uint64, 16),
		},
	}
}

// Start launches the HTTP server
func (s *WebServer) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	
	// Serve static files
	mux.Handle("/", http.FileServer(http.Dir("./web/static")))
	
	// Endpoints
	mux.HandleFunc("/ws", s.handleWebSocket)
	mux.HandleFunc("/api/config", s.handleConfig)
	mux.HandleFunc("/api/capture/start", s.handleStartCapture)
	mux.HandleFunc("/api/capture/stop", s.handleStopCapture)
	mux.HandleFunc("/api/capture/status", s.handleStatus)

	server := &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	go s.broadcastLoop(ctx)

	fmt.Printf("Web server starting on http://localhost%s\n", s.addr)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Broadcast sends a message to all connected WebSocket clients
func (s *WebServer) Broadcast(msg interface{}) {
	select {
	case s.broadcast <- msg:
	default:
		// Drop if buffer is full to prevent blocker
	}
}

// UpdateStatus allows updating telemetry stats shown on UI
func (s *WebServer) UpdateStatus(updater func(*ServerStatus)) {
	s.statusMu.Lock()
	updater(&s.status)
	statusCopy := s.status
	s.statusMu.Unlock()
	s.Broadcast(map[string]interface{}{
		"type":   "status_update",
		"status": statusCopy,
	})
}

func (s *WebServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Printf("WebSocket upgrade error: %v\n", err)
		return
	}

	s.clientsMu.Lock()
	s.clients[conn] = true
	s.clientsMu.Unlock()

	// Send initial status
	s.statusMu.RLock()
	initialStatus := s.status
	s.statusMu.RUnlock()
	
	_ = conn.WriteJSON(map[string]interface{}{
		"type":   "status_update",
		"status": initialStatus,
	})

	// Keep-alive/Read loop
	go func() {
		defer func() {
			s.clientsMu.Lock()
			delete(s.clients, conn)
			s.clientsMu.Unlock()
			conn.Close()
		}()

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	}()
}

func (s *WebServer) broadcastLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-s.broadcast:
			s.clientsMu.Lock()
			for client := range s.clients {
				err := client.WriteJSON(msg)
				if err != nil {
					client.Close()
					delete(s.clients, client)
				}
			}
			s.clientsMu.Unlock()
		}
	}
}

func (s *WebServer) handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodGet {
		s.statusMu.RLock()
		json.NewEncoder(w).Encode(s.cfg)
		s.statusMu.RUnlock()
		return
	}

	if r.Method == http.MethodPost {
		var newCfg config.Config
		if err := json.NewDecoder(r.Body).Decode(&newCfg); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		s.statusMu.Lock()
		*s.cfg = newCfg
		s.statusMu.Unlock()

		if err := config.SaveConfig(s.configPath, s.cfg); err != nil {
			http.Error(w, fmt.Sprintf("Failed to save config: %v", err), http.StatusInternalServerError)
			return
		}

		json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "Configuração salva com sucesso!"})
		return
	}

	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}

func (s *WebServer) handleStartCapture(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.statusMu.Lock()
	if s.status.Capturing {
		s.statusMu.Unlock()
		json.NewEncoder(w).Encode(map[string]string{"status": "error", "message": "A captura já está rodando!"})
		return
	}
	s.statusMu.Unlock()

	if s.OnStartCapture != nil {
		s.statusMu.RLock()
		cfgCopy := *s.cfg
		s.statusMu.RUnlock()

		err := s.OnStartCapture(&cfgCopy)
		if err != nil {
			http.Error(w, fmt.Sprintf("Erro ao iniciar captura: %v", err), http.StatusInternalServerError)
			return
		}
	}

	s.UpdateStatus(func(st *ServerStatus) {
		st.Capturing = true
		st.StartTime = time.Now()
		st.PacketsReceived = 0
	})

	json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "Captura iniciada!"})
}

func (s *WebServer) handleStopCapture(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.statusMu.Lock()
	if !s.status.Capturing {
		s.statusMu.Unlock()
		json.NewEncoder(w).Encode(map[string]string{"status": "error", "message": "A captura não está em execução!"})
		return
	}
	s.statusMu.Unlock()

	if s.OnStopCapture != nil {
		s.OnStopCapture()
	}

	s.UpdateStatus(func(st *ServerStatus) {
		st.Capturing = false
	})

	json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "Captura parada!"})
}

func (s *WebServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	s.statusMu.RLock()
	json.NewEncoder(w).Encode(s.status)
	s.statusMu.RUnlock()
}

func (s *WebServer) IsCapturing() bool {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.status.Capturing
}


// BroadcastLiveTelemetry helper to broadcast telemetry packets to frontend for gauge rendering
func (s *WebServer) BroadcastLiveTelemetry(packetId uint8, payload interface{}) {
	s.Broadcast(map[string]interface{}{
		"type":      "telemetry_live",
		"packet_id": packetId,
		"data":      payload,
	})
}

// Resolvers to show human readable text for TrackIDs and SessionTypes
var TrackNames = map[int8]string{
	0: "Melbourne (Austrália)", 1: "Paul Ricard (França)", 2: "Xangai (China)", 3: "Sakhir (Bahrein)",
	4: "Barcelona (Espanha)", 5: "Mônaco", 6: "Montreal (Canadá)", 7: "Silverstone (Reino Unido)",
	8: "Hockenheim (Alemanha)", 9: "Hungaroring (Hungria)", 10: "Spa-Francorchamps (Bélgica)",
	11: "Monza (Itália)", 12: "Singapura", 13: "Suzuka (Japão)", 14: "Yas Marina (Abu Dhabi)",
	15: "Austin (EUA)", 16: "Interlagos (Brasil)", 17: "Red Bull Ring (Áustria)", 18: "Sochi (Rússia)",
	19: "Hermanos Rodríguez (México)", 20: "Baku (Azerbaijão)", 21: "Sakhir Short", 22: "Silverstone Short",
	23: "Austin Short", 24: "Suzuka Short", 25: "Hanói (Vietnã)", 26: "Zandvoort (Holanda)",
	27: "Ímola (Itália)", 28: "Portimão (Portugal)", 29: "Jeddah (Arábia Saudita)", 30: "Miami (EUA)",
	31: "Las Vegas (EUA)", 32: "Losail (Catar)",
}

var SessionTypeNames = map[uint8]string{
	0: "Desconhecido", 1: "Treino Livre 1", 2: "Treino Livre 2", 3: "Treino Livre 3", 4: "Treino Livre Curto",
	5: "Qualificação 1", 6: "Qualificação 2", 7: "Qualificação 3", 8: "Qualificação Curta", 9: "Qualificação One-Shot",
	10: "Sprint Shootout 1", 11: "Sprint Shootout 2", 12: "Sprint Shootout 3", 13: "Short Sprint Shootout",
	14: "One-Shot Sprint Shootout", 15: "Corrida", 16: "Corrida 2", 17: "Corrida 3", 18: "Treino Contra o Tempo",
}

var TeamNames = map[uint8]string{
	0: "Mercedes", 1: "Ferrari", 2: "Red Bull Racing", 3: "Williams", 4: "Aston Martin",
	5: "Alpine", 6: "RB", 7: "Haas", 8: "McLaren", 9: "Sauber", 41: "F1 Genérico",
	104: "Equipe Customizada",
}
func GetTrackName(id int8) string {
	if name, ok := TrackNames[id]; ok {
		return name
	}
	return "Pista Desconhecida"
}

func GetSessionTypeName(id uint8) string {
	if name, ok := SessionTypeNames[id]; ok {
		return name
	}
	return "Sessão Desconhecida"
}

func GetTeamName(id uint8) string {
	if name, ok := TeamNames[id]; ok {
		return name
	}
	return "Carro Padrão"
}
