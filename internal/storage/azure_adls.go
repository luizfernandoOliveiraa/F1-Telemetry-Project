package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/parquet-go/parquet-go"
	"f1-telemetry/internal/models"
)

type AzureADLSSink struct {
	accountName string
	accountKey  string
	container   string
	directory   string
	client      *azblob.Client
	mu          sync.Mutex
	enabled     bool
}

func NewAzureADLSSink(accountName, accountKey, container, directory string) *AzureADLSSink {
	sink := &AzureADLSSink{
		accountName: accountName,
		accountKey:  accountKey,
		container:   container,
		directory:   directory,
	}

	if accountName != "" && accountKey != "" && container != "" {
		url := fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
		cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err == nil {
			client, err := azblob.NewClientWithSharedKeyCredential(url, cred, nil)
			if err == nil {
				sink.client = client
				sink.enabled = true
				fmt.Printf("Azure ADLS Gen2 Storage Sink initialized for account: %s, container: %s\n", accountName, container)
			} else {
				fmt.Printf("Azure SDK connection failed: %v. Fallback to local Parquet files.\n", err)
			}
		} else {
			fmt.Printf("Azure credentials error: %v. Fallback to local Parquet files.\n", err)
		}
	} else {
		fmt.Println("Azure credentials not fully configured. Running in Local Parquet Fallback mode.")
	}

	return sink
}

func (s *AzureADLSSink) IsEnabled() bool {
	return s.enabled
}

// SaveMotionRecords serializes and uploads motion data to Azure/Local Parquet
func (s *AzureADLSSink) SaveMotionRecords(ctx context.Context, sessionUID uint64, records []models.MotionParquet) error {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[models.MotionParquet](&buf)
	if _, err := writer.Write(records); err != nil {
		return fmt.Errorf("failed to write motion records to parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close motion parquet writer: %w", err)
	}

	return s.saveParquet(ctx, sessionUID, "motion", buf.Bytes())
}

// SaveLapRecords serializes and uploads lap data to Azure/Local Parquet
func (s *AzureADLSSink) SaveLapRecords(ctx context.Context, sessionUID uint64, records []models.LapParquet) error {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[models.LapParquet](&buf)
	if _, err := writer.Write(records); err != nil {
		return fmt.Errorf("failed to write lap records to parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close lap parquet writer: %w", err)
	}

	return s.saveParquet(ctx, sessionUID, "lap_data", buf.Bytes())
}

// SaveTelemetryRecords serializes and uploads car telemetry to Azure/Local Parquet
func (s *AzureADLSSink) SaveTelemetryRecords(ctx context.Context, sessionUID uint64, records []models.TelemetryParquet) error {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[models.TelemetryParquet](&buf)
	if _, err := writer.Write(records); err != nil {
		return fmt.Errorf("failed to write telemetry records to parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close telemetry parquet writer: %w", err)
	}

	return s.saveParquet(ctx, sessionUID, "car_telemetry", buf.Bytes())
}

// SaveStatusRecords serializes and uploads car status to Azure/Local Parquet
func (s *AzureADLSSink) SaveStatusRecords(ctx context.Context, sessionUID uint64, records []models.StatusParquet) error {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[models.StatusParquet](&buf)
	if _, err := writer.Write(records); err != nil {
		return fmt.Errorf("failed to write status records to parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close status parquet writer: %w", err)
	}

	return s.saveParquet(ctx, sessionUID, "car_status", buf.Bytes())
}

// SaveSessionRecords serializes and uploads session records to Azure/Local Parquet
func (s *AzureADLSSink) SaveSessionRecords(ctx context.Context, sessionUID uint64, records []models.SessionParquet) error {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[models.SessionParquet](&buf)
	if _, err := writer.Write(records); err != nil {
		return fmt.Errorf("failed to write session records to parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close session parquet writer: %w", err)
	}

	return s.saveParquet(ctx, sessionUID, "session", buf.Bytes())
}

// SaveEventRecords serializes and uploads event records to Azure/Local Parquet
func (s *AzureADLSSink) SaveEventRecords(ctx context.Context, sessionUID uint64, records []models.EventParquet) error {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[models.EventParquet](&buf)
	if _, err := writer.Write(records); err != nil {
		return fmt.Errorf("failed to write event records to parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close event parquet writer: %w", err)
	}

	return s.saveParquet(ctx, sessionUID, "event", buf.Bytes())
}

// SaveParticipantsRecords serializes and uploads participants records to Azure/Local Parquet
func (s *AzureADLSSink) SaveParticipantsRecords(ctx context.Context, sessionUID uint64, records []models.ParticipantParquet) error {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[models.ParticipantParquet](&buf)
	if _, err := writer.Write(records); err != nil {
		return fmt.Errorf("failed to write participants records to parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close participants parquet writer: %w", err)
	}

	return s.saveParquet(ctx, sessionUID, "participants", buf.Bytes())
}

// SaveCarSetupRecords serializes and uploads car setups records to Azure/Local Parquet
func (s *AzureADLSSink) SaveCarSetupRecords(ctx context.Context, sessionUID uint64, records []models.CarSetupParquet) error {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[models.CarSetupParquet](&buf)
	if _, err := writer.Write(records); err != nil {
		return fmt.Errorf("failed to write setups records to parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close setups parquet writer: %w", err)
	}

	return s.saveParquet(ctx, sessionUID, "car_setups", buf.Bytes())
}

// SaveFinalClassificationRecords serializes and uploads final classification records to Azure/Local Parquet
func (s *AzureADLSSink) SaveFinalClassificationRecords(ctx context.Context, sessionUID uint64, records []models.FinalClassificationParquet) error {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[models.FinalClassificationParquet](&buf)
	if _, err := writer.Write(records); err != nil {
		return fmt.Errorf("failed to write classification records to parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close classification parquet writer: %w", err)
	}

	return s.saveParquet(ctx, sessionUID, "final_classification", buf.Bytes())
}

// SaveCarDamageRecords serializes and uploads car damage records to Azure/Local Parquet
func (s *AzureADLSSink) SaveCarDamageRecords(ctx context.Context, sessionUID uint64, records []models.CarDamageParquet) error {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[models.CarDamageParquet](&buf)
	if _, err := writer.Write(records); err != nil {
		return fmt.Errorf("failed to write damage records to parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close damage parquet writer: %w", err)
	}

	return s.saveParquet(ctx, sessionUID, "car_damage", buf.Bytes())
}

// SaveSessionHistoryRecords serializes and uploads session history records to Azure/Local Parquet
func (s *AzureADLSSink) SaveSessionHistoryRecords(ctx context.Context, sessionUID uint64, records []models.SessionHistoryParquet) error {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[models.SessionHistoryParquet](&buf)
	if _, err := writer.Write(records); err != nil {
		return fmt.Errorf("failed to write history records to parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close history parquet writer: %w", err)
	}

	return s.saveParquet(ctx, sessionUID, "session_history", buf.Bytes())
}

// SaveTyreSetRecords serializes and uploads tyre sets records to Azure/Local Parquet
func (s *AzureADLSSink) SaveTyreSetRecords(ctx context.Context, sessionUID uint64, records []models.TyreSetParquet) error {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[models.TyreSetParquet](&buf)
	if _, err := writer.Write(records); err != nil {
		return fmt.Errorf("failed to write tyre sets records to parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close tyre sets parquet writer: %w", err)
	}

	return s.saveParquet(ctx, sessionUID, "tyre_sets", buf.Bytes())
}

// SaveMotionExRecords serializes and uploads motion ex records to Azure/Local Parquet
func (s *AzureADLSSink) SaveMotionExRecords(ctx context.Context, sessionUID uint64, records []models.MotionExParquet) error {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[models.MotionExParquet](&buf)
	if _, err := writer.Write(records); err != nil {
		return fmt.Errorf("failed to write motion ex records to parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close motion ex parquet writer: %w", err)
	}

	return s.saveParquet(ctx, sessionUID, "motion_ex", buf.Bytes())
}

func (s *AzureADLSSink) saveParquet(ctx context.Context, sessionUID uint64, packetType string, data []byte) error {
	now := time.Now()
	dateStr := now.Format("2006-01-02")
	fileName := fmt.Sprintf("batch_%d.parquet", now.UnixNano())
	
	// Create partitioned path
	var path string
	if s.directory != "" {
		path = fmt.Sprintf("%s/%d/%s/dt=%s/%s", s.directory, sessionUID, packetType, dateStr, fileName)
	} else {
		path = fmt.Sprintf("%d/%s/dt=%s/%s", sessionUID, packetType, dateStr, fileName)
	}

	if s.enabled && s.client != nil {
		// Try uploading to Azure ADLS Gen2
		s.mu.Lock()
		client := s.client
		s.mu.Unlock()

		_, err := client.UploadBuffer(ctx, s.container, path, data, nil)
		if err == nil {
			fmt.Printf("[Azure ADLS] Uploaded %s size=%d bytes\n", path, len(data))
			return nil
		}
		fmt.Printf("[Azure ADLS] Upload failed (%v). Falling back to local disk storage.\n", err)
	}

	// Local Fallback writing
	localDir := filepath.Join("data", "parquet", fmt.Sprintf("%d", sessionUID), packetType, "dt="+dateStr)
	if err := os.MkdirAll(localDir, 0755); err != nil {
		return fmt.Errorf("failed to create local storage directory: %w", err)
	}

	localPath := filepath.Join(localDir, fileName)
	if err := os.WriteFile(localPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write local parquet file: %w", err)
	}

	fmt.Printf("[Local Disk] Saved fallback Parquet file: %s size=%d bytes\n", localPath, len(data))
	return nil
}
