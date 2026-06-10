package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"f1-telemetry/internal/config"
	"f1-telemetry/internal/models"
	"f1-telemetry/internal/storage"
)

const configPath = "config.json"

type ParquetBuffers struct {
	mu                  sync.Mutex
	motion              []models.MotionParquet
	lap                 []models.LapParquet
	telemetry           []models.TelemetryParquet
	status              []models.StatusParquet
	session             []models.SessionParquet
	event               []models.EventParquet
	participants        []models.ParticipantParquet
	carSetups           []models.CarSetupParquet
	finalClassification []models.FinalClassificationParquet
	carDamage           []models.CarDamageParquet
	sessionHistory      []models.SessionHistoryParquet
	tyreSets            []models.TyreSetParquet
	motionEx            []models.MotionExParquet
	
	lastFlushMotion              time.Time
	lastFlushLap                 time.Time
	lastFlushTelemetry           time.Time
	lastFlushStatus              time.Time
	lastFlushSession             time.Time
	lastFlushEvent               time.Time
	lastFlushParticipants        time.Time
	lastFlushCarSetups           time.Time
	lastFlushFinalClassification time.Time
	lastFlushCarDamage           time.Time
	lastFlushSessionHistory      time.Time
	lastFlushTyreSets            time.Time
	lastFlushMotionEx            time.Time

	activeSession       uint64
}

func main() {
	fmt.Println("Starting F1 Telemetry Storage Sink...")

	// 1. Load config
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Printf("Error loading config: %v. Running with defaults.\n", err)
		cfg = &config.Config{
			KafkaBroker:        "localhost:9092",
			KafkaTopic:         "f1-telemetry",
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Connect to Azure ADLS Gen2 (or local fallback)
	adlsSink := storage.NewAzureADLSSink(
		cfg.AzureStorageAccount,
		cfg.AzureAccessKey,
		cfg.AzureContainer,
		cfg.AzureDirectory,
	)

	// 4. In-memory Parquet Batching Buffers
	nowTime := time.Now()
	buffers := &ParquetBuffers{
		motion:                       make([]models.MotionParquet, 0, 15000),
		lap:                          make([]models.LapParquet, 0, 15000),
		telemetry:                    make([]models.TelemetryParquet, 0, 15000),
		status:                       make([]models.StatusParquet, 0, 15000),
		session:                      make([]models.SessionParquet, 0, 1000),
		event:                        make([]models.EventParquet, 0, 1000),
		participants:                 make([]models.ParticipantParquet, 0, 1000),
		carSetups:                    make([]models.CarSetupParquet, 0, 1000),
		finalClassification:          make([]models.FinalClassificationParquet, 0, 1000),
		carDamage:                    make([]models.CarDamageParquet, 0, 1000),
		sessionHistory:               make([]models.SessionHistoryParquet, 0, 1000),
		tyreSets:                     make([]models.TyreSetParquet, 0, 1000),
		motionEx:                     make([]models.MotionExParquet, 0, 15000),
		lastFlushMotion:              nowTime,
		lastFlushLap:                 nowTime,
		lastFlushTelemetry:           nowTime,
		lastFlushStatus:              nowTime,
		lastFlushSession:             nowTime,
		lastFlushEvent:               nowTime,
		lastFlushParticipants:        nowTime,
		lastFlushCarSetups:           nowTime,
		lastFlushFinalClassification: nowTime,
		lastFlushCarDamage:           nowTime,
		lastFlushSessionHistory:      nowTime,
		lastFlushTyreSets:            nowTime,
		lastFlushMotionEx:            nowTime,
	}

	// Flush Parquet buffers worker
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				flushAllBuffers(context.Background(), buffers, adlsSink)
				return
			case <-ticker.C:
				flushBuffersOnInterval(ctx, buffers, adlsSink)
			}
		}
	}()

	// 5. Connect Kafka Consumer
	fmt.Printf("Connecting to Kafka broker: %s on topic: %s...\n", cfg.KafkaBroker, cfg.KafkaTopic)
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.KafkaBroker),
		kgo.ConsumeTopics(cfg.KafkaTopic),
		kgo.ConsumerGroup("f1-telemetry-sink"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		fmt.Printf("Failed to create Kafka consumer client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// Verify connection
	pingCtx, pingCancel := context.WithTimeout(ctx, 3*time.Second)
	if err := client.Ping(pingCtx); err != nil {
		fmt.Printf("Warning: Kafka broker not reachable yet: %v. Retrying in background.\n", err)
	}
	pingCancel()

	// Signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nShutdown signal received in Sink. Exiting and flushing buffers...")
		cancel()
	}()

	// 6. Consumer Loop
	fmt.Println("Listening for Kafka telemetry messages...")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			fetches := client.PollRecords(ctx, 500)
			if fetches.IsClientClosed() {
				return
			}

			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					fmt.Printf("Kafka consumer poll warning: %v\n", err)
				}
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()

				// Identify PacketID from Kafka headers
				var packetID uint8
				var found bool
				for _, h := range record.Headers {
					if h.Key == "packet_id" && len(h.Value) > 0 {
						packetID = h.Value[0]
						found = true
						break
					}
				}

				if !found {
					continue
				}

				// Process packet type
				switch packetID {
				case models.PacketIDMotion:
					var p models.PacketMotionData
					if err := json.Unmarshal(record.Value, &p); err == nil {
						recs := models.MapToMotionParquet(&p)
						buffers.mu.Lock()
						buffers.motion = append(buffers.motion, recs...)
						buffers.activeSession = p.Header.SessionUID
						checkBufferLimits(ctx, buffers, adlsSink)
						buffers.mu.Unlock()
					}

				case models.PacketIDSession:
					var p models.PacketSessionData
					if err := json.Unmarshal(record.Value, &p); err == nil {
						recs := models.MapToSessionParquet(&p)
						buffers.mu.Lock()
						buffers.session = append(buffers.session, recs...)
						buffers.activeSession = p.Header.SessionUID
						checkBufferLimits(ctx, buffers, adlsSink)
						buffers.mu.Unlock()
					}

				case models.PacketIDLapData:
					var p models.PacketLapData
					if err := json.Unmarshal(record.Value, &p); err == nil {
						recs := models.MapToLapParquet(&p)
						// Buffer for Parquet lake
						buffers.mu.Lock()
						buffers.lap = append(buffers.lap, recs...)
						buffers.activeSession = p.Header.SessionUID
						checkBufferLimits(ctx, buffers, adlsSink)
						buffers.mu.Unlock()
					}

				case models.PacketIDCarTelemetry:
					var p models.PacketCarTelemetryData
					if err := json.Unmarshal(record.Value, &p); err == nil {
						recs := models.MapToTelemetryParquet(&p)
						// Buffer for Parquet lake
						buffers.mu.Lock()
						buffers.telemetry = append(buffers.telemetry, recs...)
						buffers.activeSession = p.Header.SessionUID
						checkBufferLimits(ctx, buffers, adlsSink)
						buffers.mu.Unlock()
					}

				case models.PacketIDCarStatus:
					var p models.PacketCarStatusData
					if err := json.Unmarshal(record.Value, &p); err == nil {
						recs := models.MapToStatusParquet(&p)
						// Buffer for Parquet lake
						buffers.mu.Lock()
						buffers.status = append(buffers.status, recs...)
						buffers.activeSession = p.Header.SessionUID
						checkBufferLimits(ctx, buffers, adlsSink)
						buffers.mu.Unlock()
					}

				case models.PacketIDEvent:
					var p models.PacketEventData
					if err := json.Unmarshal(record.Value, &p); err == nil {
						recs := models.MapToEventParquet(&p)
						buffers.mu.Lock()
						buffers.event = append(buffers.event, recs...)
						buffers.activeSession = p.Header.SessionUID
						checkBufferLimits(ctx, buffers, adlsSink)
						buffers.mu.Unlock()
					}

				case models.PacketIDParticipants:
					var p models.PacketParticipantsData
					if err := json.Unmarshal(record.Value, &p); err == nil {
						recs := models.MapToParticipantsParquet(&p)
						buffers.mu.Lock()
						buffers.participants = append(buffers.participants, recs...)
						buffers.activeSession = p.Header.SessionUID
						checkBufferLimits(ctx, buffers, adlsSink)
						buffers.mu.Unlock()
					}

				case models.PacketIDCarSetups:
					var p models.PacketCarSetupData
					if err := json.Unmarshal(record.Value, &p); err == nil {
						recs := models.MapToCarSetupParquet(&p)
						buffers.mu.Lock()
						buffers.carSetups = append(buffers.carSetups, recs...)
						buffers.activeSession = p.Header.SessionUID
						checkBufferLimits(ctx, buffers, adlsSink)
						buffers.mu.Unlock()
					}

				case models.PacketIDFinalClassification:
					var p models.PacketFinalClassificationData
					if err := json.Unmarshal(record.Value, &p); err == nil {
						recs := models.MapToFinalClassificationParquet(&p)
						buffers.mu.Lock()
						buffers.finalClassification = append(buffers.finalClassification, recs...)
						buffers.activeSession = p.Header.SessionUID
						checkBufferLimits(ctx, buffers, adlsSink)
						buffers.mu.Unlock()
					}

				case models.PacketIDCarDamage:
					var p models.PacketCarDamageData
					if err := json.Unmarshal(record.Value, &p); err == nil {
						recs := models.MapToCarDamageParquet(&p)
						buffers.mu.Lock()
						buffers.carDamage = append(buffers.carDamage, recs...)
						buffers.activeSession = p.Header.SessionUID
						checkBufferLimits(ctx, buffers, adlsSink)
						buffers.mu.Unlock()
					}

				case models.PacketIDSessionHistory:
					var p models.PacketSessionHistoryData
					if err := json.Unmarshal(record.Value, &p); err == nil {
						recs := models.MapToSessionHistoryParquet(&p)
						buffers.mu.Lock()
						buffers.sessionHistory = append(buffers.sessionHistory, recs...)
						buffers.activeSession = p.Header.SessionUID
						checkBufferLimits(ctx, buffers, adlsSink)
						buffers.mu.Unlock()
					}

				case models.PacketIDTyreSets:
					var p models.PacketTyreSetsData
					if err := json.Unmarshal(record.Value, &p); err == nil {
						recs := models.MapToTyreSetParquet(&p)
						buffers.mu.Lock()
						buffers.tyreSets = append(buffers.tyreSets, recs...)
						buffers.activeSession = p.Header.SessionUID
						checkBufferLimits(ctx, buffers, adlsSink)
						buffers.mu.Unlock()
					}

				case models.PacketIDMotionEx:
					var p models.PacketMotionExData
					if err := json.Unmarshal(record.Value, &p); err == nil {
						recs := models.MapToMotionExParquet(&p)
						buffers.mu.Lock()
						buffers.motionEx = append(buffers.motionEx, recs...)
						buffers.activeSession = p.Header.SessionUID
						checkBufferLimits(ctx, buffers, adlsSink)
						buffers.mu.Unlock()
					}
				}
			}
		}
	}
}

func checkBufferLimits(ctx context.Context, b *ParquetBuffers, sink *storage.AzureADLSSink) {
	session := b.activeSession
	if session == 0 {
		return
	}

	now := time.Now()

	// High frequency packets: limit 15,000 records
	if len(b.motion) >= 15000 {
		recs := b.motion
		b.motion = make([]models.MotionParquet, 0, 15000)
		b.lastFlushMotion = now
		go func() {
			_ = sink.SaveMotionRecords(context.Background(), session, recs)
		}()
	}
	if len(b.lap) >= 15000 {
		recs := b.lap
		b.lap = make([]models.LapParquet, 0, 15000)
		b.lastFlushLap = now
		go func() {
			_ = sink.SaveLapRecords(context.Background(), session, recs)
		}()
	}
	if len(b.telemetry) >= 15000 {
		recs := b.telemetry
		b.telemetry = make([]models.TelemetryParquet, 0, 15000)
		b.lastFlushTelemetry = now
		go func() {
			_ = sink.SaveTelemetryRecords(context.Background(), session, recs)
		}()
	}
	if len(b.status) >= 15000 {
		recs := b.status
		b.status = make([]models.StatusParquet, 0, 15000)
		b.lastFlushStatus = now
		go func() {
			_ = sink.SaveStatusRecords(context.Background(), session, recs)
		}()
	}
	if len(b.motionEx) >= 15000 {
		recs := b.motionEx
		b.motionEx = make([]models.MotionExParquet, 0, 15000)
		b.lastFlushMotionEx = now
		go func() {
			_ = sink.SaveMotionExRecords(context.Background(), session, recs)
		}()
	}

	// Low frequency packets: limit 1,000 records
	if len(b.session) >= 1000 {
		recs := b.session
		b.session = make([]models.SessionParquet, 0, 1000)
		b.lastFlushSession = now
		go func() {
			_ = sink.SaveSessionRecords(context.Background(), session, recs)
		}()
	}
	if len(b.event) >= 1000 {
		recs := b.event
		b.event = make([]models.EventParquet, 0, 1000)
		b.lastFlushEvent = now
		go func() {
			_ = sink.SaveEventRecords(context.Background(), session, recs)
		}()
	}
	if len(b.participants) >= 1000 {
		recs := b.participants
		b.participants = make([]models.ParticipantParquet, 0, 1000)
		b.lastFlushParticipants = now
		go func() {
			_ = sink.SaveParticipantsRecords(context.Background(), session, recs)
		}()
	}
	if len(b.carSetups) >= 1000 {
		recs := b.carSetups
		b.carSetups = make([]models.CarSetupParquet, 0, 1000)
		b.lastFlushCarSetups = now
		go func() {
			_ = sink.SaveCarSetupRecords(context.Background(), session, recs)
		}()
	}
	if len(b.finalClassification) >= 1000 {
		recs := b.finalClassification
		b.finalClassification = make([]models.FinalClassificationParquet, 0, 1000)
		b.lastFlushFinalClassification = now
		go func() {
			_ = sink.SaveFinalClassificationRecords(context.Background(), session, recs)
		}()
	}
	if len(b.carDamage) >= 1000 {
		recs := b.carDamage
		b.carDamage = make([]models.CarDamageParquet, 0, 1000)
		b.lastFlushCarDamage = now
		go func() {
			_ = sink.SaveCarDamageRecords(context.Background(), session, recs)
		}()
	}
	if len(b.sessionHistory) >= 1000 {
		recs := b.sessionHistory
		b.sessionHistory = make([]models.SessionHistoryParquet, 0, 1000)
		b.lastFlushSessionHistory = now
		go func() {
			_ = sink.SaveSessionHistoryRecords(context.Background(), session, recs)
		}()
	}
	if len(b.tyreSets) >= 1000 {
		recs := b.tyreSets
		b.tyreSets = make([]models.TyreSetParquet, 0, 1000)
		b.lastFlushTyreSets = now
		go func() {
			_ = sink.SaveTyreSetRecords(context.Background(), session, recs)
		}()
	}
}

func flushBuffersOnInterval(ctx context.Context, b *ParquetBuffers, sink *storage.AzureADLSSink) {
	b.mu.Lock()
	session := b.activeSession
	if session == 0 {
		b.mu.Unlock()
		return
	}

	highFreqLimit := 2 * time.Minute
	lowFreqLimit := 10 * time.Minute
	now := time.Now()

	// 1. Motion
	if len(b.motion) > 0 && now.Sub(b.lastFlushMotion) >= highFreqLimit {
		recs := b.motion
		b.motion = make([]models.MotionParquet, 0, 15000)
		b.lastFlushMotion = now
		go func() {
			_ = sink.SaveMotionRecords(context.Background(), session, recs)
		}()
	}
	// 2. Lap
	if len(b.lap) > 0 && now.Sub(b.lastFlushLap) >= highFreqLimit {
		recs := b.lap
		b.lap = make([]models.LapParquet, 0, 15000)
		b.lastFlushLap = now
		go func() {
			_ = sink.SaveLapRecords(context.Background(), session, recs)
		}()
	}
	// 3. Telemetry
	if len(b.telemetry) > 0 && now.Sub(b.lastFlushTelemetry) >= highFreqLimit {
		recs := b.telemetry
		b.telemetry = make([]models.TelemetryParquet, 0, 15000)
		b.lastFlushTelemetry = now
		go func() {
			_ = sink.SaveTelemetryRecords(context.Background(), session, recs)
		}()
	}
	// 4. Status
	if len(b.status) > 0 && now.Sub(b.lastFlushStatus) >= highFreqLimit {
		recs := b.status
		b.status = make([]models.StatusParquet, 0, 15000)
		b.lastFlushStatus = now
		go func() {
			_ = sink.SaveStatusRecords(context.Background(), session, recs)
		}()
	}
	// 5. Session
	if len(b.session) > 0 && now.Sub(b.lastFlushSession) >= lowFreqLimit {
		recs := b.session
		b.session = make([]models.SessionParquet, 0, 1000)
		b.lastFlushSession = now
		go func() {
			_ = sink.SaveSessionRecords(context.Background(), session, recs)
		}()
	}
	// 6. Event
	if len(b.event) > 0 && now.Sub(b.lastFlushEvent) >= lowFreqLimit {
		recs := b.event
		b.event = make([]models.EventParquet, 0, 1000)
		b.lastFlushEvent = now
		go func() {
			_ = sink.SaveEventRecords(context.Background(), session, recs)
		}()
	}
	// 7. Participants
	if len(b.participants) > 0 && now.Sub(b.lastFlushParticipants) >= lowFreqLimit {
		recs := b.participants
		b.participants = make([]models.ParticipantParquet, 0, 1000)
		b.lastFlushParticipants = now
		go func() {
			_ = sink.SaveParticipantsRecords(context.Background(), session, recs)
		}()
	}
	// 8. CarSetups
	if len(b.carSetups) > 0 && now.Sub(b.lastFlushCarSetups) >= lowFreqLimit {
		recs := b.carSetups
		b.carSetups = make([]models.CarSetupParquet, 0, 1000)
		b.lastFlushCarSetups = now
		go func() {
			_ = sink.SaveCarSetupRecords(context.Background(), session, recs)
		}()
	}
	// 9. FinalClassification
	if len(b.finalClassification) > 0 && now.Sub(b.lastFlushFinalClassification) >= lowFreqLimit {
		recs := b.finalClassification
		b.finalClassification = make([]models.FinalClassificationParquet, 0, 1000)
		b.lastFlushFinalClassification = now
		go func() {
			_ = sink.SaveFinalClassificationRecords(context.Background(), session, recs)
		}()
	}
	// 10. CarDamage
	if len(b.carDamage) > 0 && now.Sub(b.lastFlushCarDamage) >= lowFreqLimit {
		recs := b.carDamage
		b.carDamage = make([]models.CarDamageParquet, 0, 1000)
		b.lastFlushCarDamage = now
		go func() {
			_ = sink.SaveCarDamageRecords(context.Background(), session, recs)
		}()
	}
	// 11. SessionHistory
	if len(b.sessionHistory) > 0 && now.Sub(b.lastFlushSessionHistory) >= lowFreqLimit {
		recs := b.sessionHistory
		b.sessionHistory = make([]models.SessionHistoryParquet, 0, 1000)
		b.lastFlushSessionHistory = now
		go func() {
			_ = sink.SaveSessionHistoryRecords(context.Background(), session, recs)
		}()
	}
	// 12. TyreSets
	if len(b.tyreSets) > 0 && now.Sub(b.lastFlushTyreSets) >= lowFreqLimit {
		recs := b.tyreSets
		b.tyreSets = make([]models.TyreSetParquet, 0, 1000)
		b.lastFlushTyreSets = now
		go func() {
			_ = sink.SaveTyreSetRecords(context.Background(), session, recs)
		}()
	}
	// 13. MotionEx
	if len(b.motionEx) > 0 && now.Sub(b.lastFlushMotionEx) >= highFreqLimit {
		recs := b.motionEx
		b.motionEx = make([]models.MotionExParquet, 0, 15000)
		b.lastFlushMotionEx = now
		go func() {
			_ = sink.SaveMotionExRecords(context.Background(), session, recs)
		}()
	}

	b.mu.Unlock()
}

func flushAllBuffers(ctx context.Context, b *ParquetBuffers, sink *storage.AzureADLSSink) {
	b.mu.Lock()
	defer b.mu.Unlock()

	session := b.activeSession
	if session == 0 {
		return
	}

	fmt.Println("Gracefully flushing all remaining Parquet buffers to storage on exit...")

	motionRecs := b.motion
	b.motion = make([]models.MotionParquet, 0, 15000)

	lapRecs := b.lap
	b.lap = make([]models.LapParquet, 0, 15000)

	telemRecs := b.telemetry
	b.telemetry = make([]models.TelemetryParquet, 0, 15000)

	statusRecs := b.status
	b.status = make([]models.StatusParquet, 0, 15000)

	sessionRecs := b.session
	b.session = make([]models.SessionParquet, 0, 1000)

	eventRecs := b.event
	b.event = make([]models.EventParquet, 0, 1000)

	participantsRecs := b.participants
	b.participants = make([]models.ParticipantParquet, 0, 1000)

	setupRecs := b.carSetups
	b.carSetups = make([]models.CarSetupParquet, 0, 1000)

	classRecs := b.finalClassification
	b.finalClassification = make([]models.FinalClassificationParquet, 0, 1000)

	damageRecs := b.carDamage
	b.carDamage = make([]models.CarDamageParquet, 0, 1000)

	historyRecs := b.sessionHistory
	b.sessionHistory = make([]models.SessionHistoryParquet, 0, 1000)

	tyreRecs := b.tyreSets
	b.tyreSets = make([]models.TyreSetParquet, 0, 1000)

	motionExRecs := b.motionEx
	b.motionEx = make([]models.MotionExParquet, 0, 15000)

	var wg sync.WaitGroup

	if len(motionRecs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sink.SaveMotionRecords(ctx, session, motionRecs)
		}()
	}
	if len(lapRecs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sink.SaveLapRecords(ctx, session, lapRecs)
		}()
	}
	if len(telemRecs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sink.SaveTelemetryRecords(ctx, session, telemRecs)
		}()
	}
	if len(statusRecs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sink.SaveStatusRecords(ctx, session, statusRecs)
		}()
	}
	if len(sessionRecs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sink.SaveSessionRecords(ctx, session, sessionRecs)
		}()
	}
	if len(eventRecs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sink.SaveEventRecords(ctx, session, eventRecs)
		}()
	}
	if len(participantsRecs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sink.SaveParticipantsRecords(ctx, session, participantsRecs)
		}()
	}
	if len(setupRecs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sink.SaveCarSetupRecords(ctx, session, setupRecs)
		}()
	}
	if len(classRecs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sink.SaveFinalClassificationRecords(ctx, session, classRecs)
		}()
	}
	if len(damageRecs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sink.SaveCarDamageRecords(ctx, session, damageRecs)
		}()
	}
	if len(historyRecs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sink.SaveSessionHistoryRecords(ctx, session, historyRecs)
		}()
	}
	if len(tyreRecs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sink.SaveTyreSetRecords(ctx, session, tyreRecs)
		}()
	}
	if len(motionExRecs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sink.SaveMotionExRecords(ctx, session, motionExRecs)
		}()
	}

	wg.Wait()
	fmt.Println("All Parquet buffers flushed successfully.")
}
