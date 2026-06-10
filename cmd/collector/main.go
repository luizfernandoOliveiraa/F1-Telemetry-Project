package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"f1-telemetry/internal/config"
	"f1-telemetry/internal/models"
	"f1-telemetry/internal/network"
	"f1-telemetry/internal/queue"
	"f1-telemetry/internal/web"
)

const configPath = "config.json"

func main() {
	// 1. Load config
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	// 2. Initialize Web Server
	webServer := web.NewWebServer(":8080", configPath, cfg)

	// Context for graceful shutdown of background routines
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		udpListener *network.UDPListener
		producer    *queue.KafkaProducer
		packetChan  chan network.UDPPacket
		wg          sync.WaitGroup
		captureCtx  context.Context
		captureCancel context.CancelFunc
	)

	// State tracking for session metadata to show on the dashboard
	var (
		lastLapNum      uint8
		bestLapTimeMs   uint32
		bestSector1Ms   uint16
		bestSector1Min  uint8
		bestSector2Ms   uint16
		bestSector2Min  uint8
		packetsReceived uint64
		udpPacketStats  [16]uint64
		stateMu         sync.Mutex
	)

	// Reset state when starting a new capture session
	resetState := func() {
		stateMu.Lock()
		lastLapNum = 0
		bestLapTimeMs = 0
		bestSector1Ms = 0
		bestSector1Min = 0
		bestSector2Ms = 0
		bestSector2Min = 0
		packetsReceived = 0
		for i := 0; i < 16; i++ {
			udpPacketStats[i] = 0
		}
		stateMu.Unlock()
	}

	// Helper to format sector info
	formatSectors := func() string {
		stateMu.Lock()
		defer stateMu.Unlock()
		s1 := "--.---"
		if bestSector1Ms > 0 || bestSector1Min > 0 {
			s1 = fmt.Sprintf("%.3f", float64(bestSector1Min)*60.0 + float64(bestSector1Ms)/1000.0)
		}
		s2 := "--.---"
		if bestSector2Ms > 0 || bestSector2Min > 0 {
			s2 = fmt.Sprintf("%.3f", float64(bestSector2Min)*60.0 + float64(bestSector2Ms)/1000.0)
		}
		return fmt.Sprintf("S1: %s | S2: %s", s1, s2)
	}

	formatLapTime := func(ms uint32) string {
		if ms == 0 {
			return "--:--.---"
		}
		mins := ms / 60000
		secs := float64(ms%60000) / 1000.0
		return fmt.Sprintf("%d:%06.3f", mins, secs)
	}

	// Callback: Start Capture
	webServer.OnStartCapture = func(c *config.Config) error {
		resetState()
		fmt.Printf("Starting capture. UDP Port: %d, Kafka Broker: %s\n", c.UDPPort, c.KafkaBroker)

		// Initialize Kafka Producer
		producer = queue.NewKafkaProducer(c.KafkaBroker, c.KafkaTopic)
		if err := producer.Start(ctx); err != nil {
			fmt.Printf("Kafka initialization warning: %v (data will still broadcast to UI)\n", err)
		}

		// Create channel for UDP packet flow
		packetChan = make(chan network.UDPPacket, 5000)
		udpListener = network.NewUDPListener(c.UDPPort, packetChan)

		captureCtx, captureCancel = context.WithCancel(ctx)

		// Start UDP Ingestion
		if err := udpListener.Start(captureCtx); err != nil {
			producer.Stop()
			captureCancel()
			return fmt.Errorf("failed to start UDP listener: %w", err)
		}

		// Start Packet processing worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			fmt.Println("Telemetry packet processing worker started")

			for {
				select {
				case <-captureCtx.Done():
					return
				case pkt, ok := <-packetChan:
					if !ok {
						return
					}

					// Raw packet processing
					rawBytes := pkt.Data[:pkt.Length]
					
					// Parse Header
					header := models.ParsePacketHeader(rawBytes)

					stateMu.Lock()
					packetsReceived++
					pktsCount := packetsReceived
					if header.PacketId < 16 {
						udpPacketStats[header.PacketId]++
					}
					statsCopy := make([]uint64, 16)
					copy(statsCopy, udpPacketStats[:])
					stateMu.Unlock()

					// Update UI packet counter
					webServer.UpdateStatus(func(st *web.ServerStatus) {
						st.PacketsReceived = pktsCount
						st.UDPPacketStats = statsCopy
						if header.SessionUID != 0 {
							st.SessionUID = header.SessionUID
						}
					})

					// Forward raw packet payload to Kafka
					if producer != nil {
						// Send raw packet bytes as JSON string or parsed JSON depending on type
						// To ingest all packets, we parse and produce them
						switch header.PacketId {
						case models.PacketIDMotion:
							motion := models.ParsePacketMotionData(rawBytes, header)
							_ = producer.Publish(header.SessionUID, header.PacketId, motion)
							// Broadcast live to UI
							webServer.BroadcastLiveTelemetry(header.PacketId, motion)

						case models.PacketIDSession:
							session := models.ParsePacketSessionData(rawBytes, header)
							_ = producer.Publish(header.SessionUID, header.PacketId, session)
							webServer.UpdateStatus(func(st *web.ServerStatus) {
								st.TrackName = web.GetTrackName(session.TrackId)
								st.SessionTypeName = web.GetSessionTypeName(session.SessionType)
								st.TotalLaps = session.TotalLaps
							})

						case models.PacketIDLapData:
							laps := models.ParsePacketLapData(rawBytes, header)
							_ = producer.Publish(header.SessionUID, header.PacketId, laps)
							webServer.BroadcastLiveTelemetry(header.PacketId, laps)

							// Update lap stats on UI
							playerIdx := header.PlayerCarIndex
							if int(playerIdx) < len(laps.LapData) {
								pLap := laps.LapData[playerIdx]
								
								stateMu.Lock()
								if pLap.CurrentLapNum != lastLapNum {
									lastLapNum = pLap.CurrentLapNum
								}
								// Track best lap times
								if pLap.LastLapTimeInMS > 0 && (bestLapTimeMs == 0 || pLap.LastLapTimeInMS < bestLapTimeMs) {
									bestLapTimeMs = pLap.LastLapTimeInMS
								}
								// Track best sectors
								if pLap.Sector1TimeMSPart > 0 && (bestSector1Ms == 0 || pLap.Sector1TimeMSPart < bestSector1Ms) {
									bestSector1Ms = pLap.Sector1TimeMSPart
									bestSector1Min = pLap.Sector1TimeMinutesPart
								}
								if pLap.Sector2TimeMSPart > 0 && (bestSector2Ms == 0 || pLap.Sector2TimeMSPart < bestSector2Ms) {
									bestSector2Ms = pLap.Sector2TimeMSPart
									bestSector2Min = pLap.Sector2TimeMinutesPart
								}
								stateMu.Unlock()

								webServer.UpdateStatus(func(st *web.ServerStatus) {
									st.LapsCompleted = pLap.CurrentLapNum - 1
									if bestLapTimeMs > 0 {
										st.BestLapTime = formatLapTime(bestLapTimeMs)
									}
									st.BestSectors = formatSectors()
								})
							}

						case models.PacketIDParticipants:
							participants := models.ParsePacketParticipantsData(rawBytes, header)
							_ = producer.Publish(header.SessionUID, header.PacketId, participants)
							
							playerIdx := header.PlayerCarIndex
							if int(playerIdx) < len(participants.Participants) {
								player := participants.Participants[playerIdx]
								webServer.UpdateStatus(func(st *web.ServerStatus) {
									st.VehicleName = fmt.Sprintf("%s (%s)", player.Name, web.GetTeamName(player.TeamId))
								})
							}

						case models.PacketIDCarTelemetry:
							telemetry := models.ParsePacketCarTelemetryData(rawBytes, header)
							_ = producer.Publish(header.SessionUID, header.PacketId, telemetry)
							// Live telemetry broadcast to UI for HUD rendering
							webServer.BroadcastLiveTelemetry(header.PacketId, telemetry)
						case models.PacketIDCarStatus:
							status := models.ParsePacketCarStatusData(rawBytes, header)
							_ = producer.Publish(header.SessionUID, header.PacketId, status)
							webServer.BroadcastLiveTelemetry(header.PacketId, status)

						case models.PacketIDEvent:
							evt := models.ParsePacketEventData(rawBytes, header)
							_ = producer.Publish(header.SessionUID, header.PacketId, evt)

						case models.PacketIDCarSetups:
							setups := models.ParsePacketCarSetupData(rawBytes, header)
							_ = producer.Publish(header.SessionUID, header.PacketId, setups)

						case models.PacketIDFinalClassification:
							classification := models.ParsePacketFinalClassificationData(rawBytes, header)
							_ = producer.Publish(header.SessionUID, header.PacketId, classification)

						case models.PacketIDCarDamage:
							damage := models.ParsePacketCarDamageData(rawBytes, header)
							_ = producer.Publish(header.SessionUID, header.PacketId, damage)

						case models.PacketIDSessionHistory:
							history := models.ParsePacketSessionHistoryData(rawBytes, header)
							_ = producer.Publish(header.SessionUID, header.PacketId, history)

						case models.PacketIDTyreSets:
							tyreSets := models.ParsePacketTyreSetsData(rawBytes, header)
							_ = producer.Publish(header.SessionUID, header.PacketId, tyreSets)

						case models.PacketIDMotionEx:
							motionEx := models.ParsePacketMotionExData(rawBytes, header)
							_ = producer.Publish(header.SessionUID, header.PacketId, motionEx)

						default:
							// For other packets, send basic header + raw payload to Kafka
							type GenericPacket struct {
								Header models.PacketHeader `json:"header"`
								Raw    []byte               `json:"raw"`
							}
							_ = producer.Publish(header.SessionUID, header.PacketId, GenericPacket{
								Header: header,
								Raw:    rawBytes[29:],
							})
						}
					}

					// Return buffer back to pool
					udpListener.Recycle(pkt.Data)
				}
			}
		}()

		return nil
	}

	// Callback: Stop Capture
	webServer.OnStopCapture = func() {
		fmt.Println("Stopping capture...")
		if udpListener != nil {
			udpListener.Stop()
		}
		if captureCancel != nil {
			captureCancel()
		}
		if packetChan != nil {
			close(packetChan)
		}
		wg.Wait()

		if producer != nil {
			producer.Stop()
		}
		fmt.Println("Capture stopped successfully.")
	}

	// 3. Graceful shutdown handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutdown signal received. Exiting...")
		if webServer.IsCapturing() {
			webServer.OnStopCapture()
		}
		cancel()
		time.Sleep(500 * time.Millisecond)
		os.Exit(0)
	}()

	// 4. Start HTTP Web UI Server (blocks)
	if err := webServer.Start(ctx); err != nil {
		fmt.Printf("HTTP Server error: %v\n", err)
	}
}
