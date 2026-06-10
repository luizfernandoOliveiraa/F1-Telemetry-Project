package models

import "time"

// MotionParquet represents a single car's motion data row in a Parquet file.
type MotionParquet struct {
	SessionUID         int64     `parquet:"session_uid,type=INT64"`
	SessionTime        float32   `parquet:"session_time,type=FLOAT"`
	FrameIdentifier    int64     `parquet:"frame_identifier,type=INT64"`
	CarIndex           int32     `parquet:"car_index,type=INT32"`
	IsPlayerCar        bool      `parquet:"is_player_car,type=BOOLEAN"`
	WorldPositionX     float32   `parquet:"world_position_x,type=FLOAT"`
	WorldPositionY     float32   `parquet:"world_position_y,type=FLOAT"`
	WorldPositionZ     float32   `parquet:"world_position_z,type=FLOAT"`
	WorldVelocityX     float32   `parquet:"world_velocity_x,type=FLOAT"`
	WorldVelocityY     float32   `parquet:"world_velocity_y,type=FLOAT"`
	WorldVelocityZ     float32   `parquet:"world_velocity_z,type=FLOAT"`
	GForceLateral      float32   `parquet:"g_force_lateral,type=FLOAT"`
	GForceLongitudinal float32   `parquet:"g_force_longitudinal,type=FLOAT"`
	GForceVertical     float32   `parquet:"g_force_vertical,type=FLOAT"`
	Yaw                float32   `parquet:"yaw,type=FLOAT"`
	Pitch              float32   `parquet:"pitch,type=FLOAT"`
	Roll               float32   `parquet:"roll,type=FLOAT"`
	Timestamp          int64     `parquet:"timestamp,type=INT64"`
}

// LapParquet represents a single car's lap data row in a Parquet file.
type LapParquet struct {
	SessionUID             int64   `parquet:"session_uid,type=INT64"`
	SessionTime            float32 `parquet:"session_time,type=FLOAT"`
	FrameIdentifier        int64   `parquet:"frame_identifier,type=INT64"`
	CarIndex               int32   `parquet:"car_index,type=INT32"`
	IsPlayerCar            bool    `parquet:"is_player_car,type=BOOLEAN"`
	LastLapTimeInMS        int32   `parquet:"last_lap_time_in_ms,type=INT32"`
	CurrentLapTimeInMS     int32   `parquet:"current_lap_time_in_ms,type=INT32"`
	Sector1TimeMS          int32   `parquet:"sector_1_time_ms,type=INT32"`
	Sector2TimeMS          int32   `parquet:"sector_2_time_ms,type=INT32"`
	CarPosition            int32   `parquet:"car_position,type=INT32"`
	CurrentLapNum          int32   `parquet:"current_lap_num,type=INT32"`
	PitStatus              int32   `parquet:"pit_status,type=INT32"`
	Sector                 int32   `parquet:"sector,type=INT32"`
	Penalties              int32   `parquet:"penalties,type=INT32"`
	DriverStatus           int32   `parquet:"driver_status,type=INT32"`
	ResultStatus           int32   `parquet:"result_status,type=INT32"`
	Timestamp              int64   `parquet:"timestamp,type=INT64"`
}

// TelemetryParquet represents a single car's telemetry data row in a Parquet file.
type TelemetryParquet struct {
	SessionUID        int64     `parquet:"session_uid,type=INT64"`
	SessionTime       float32   `parquet:"session_time,type=FLOAT"`
	FrameIdentifier   int64     `parquet:"frame_identifier,type=INT64"`
	CarIndex          int32     `parquet:"car_index,type=INT32"`
	IsPlayerCar       bool      `parquet:"is_player_car,type=BOOLEAN"`
	Speed             int32     `parquet:"speed,type=INT32"`
	Throttle          float32   `parquet:"throttle,type=FLOAT"`
	Steer             float32   `parquet:"steer,type=FLOAT"`
	Brake             float32   `parquet:"brake,type=FLOAT"`
	Clutch            int32     `parquet:"clutch,type=INT32"`
	Gear              int32     `parquet:"gear,type=INT32"`
	EngineRPM         int32     `parquet:"engine_rpm,type=INT32"`
	Drs               int32     `parquet:"drs,type=INT32"`
	EngineTemperature int32     `parquet:"engine_temperature,type=INT32"`
	TyresTempRL       int32     `parquet:"tyres_temp_rl,type=INT32"`
	TyresTempRR       int32     `parquet:"tyres_temp_rr,type=INT32"`
	TyresTempFL       int32     `parquet:"tyres_temp_fl,type=INT32"`
	TyresTempFR       int32     `parquet:"tyres_temp_fr,type=INT32"`
	Timestamp         int64     `parquet:"timestamp,type=INT64"`
}

// StatusParquet represents a single car's status data row in a Parquet file.
type StatusParquet struct {
	SessionUID        int64   `parquet:"session_uid,type=INT64"`
	SessionTime       float32 `parquet:"session_time,type=FLOAT"`
	FrameIdentifier   int64   `parquet:"frame_identifier,type=INT64"`
	CarIndex          int32   `parquet:"car_index,type=INT32"`
	IsPlayerCar       bool    `parquet:"is_player_car,type=BOOLEAN"`
	FuelInTank        float32 `parquet:"fuel_in_tank,type=FLOAT"`
	FuelCapacity      float32 `parquet:"fuel_capacity,type=FLOAT"`
	FuelRemainingLaps float32 `parquet:"fuel_remaining_laps,type=FLOAT"`
	MaxRPM            int32   `parquet:"max_rpm,type=INT32"`
	IdleRPM           int32   `parquet:"idle_rpm,type=INT32"`
	MaxGears          int32   `parquet:"max_gears,type=INT32"`
	DrsAllowed        int32   `parquet:"drs_allowed,type=INT32"`
	ActualTyreCompound int32  `parquet:"actual_tyre_compound,type=INT32"`
	VisualTyreCompound int32  `parquet:"visual_tyre_compound,type=INT32"`
	TyresAgeLaps      int32   `parquet:"tyres_age_laps,type=INT32"`
	ErsStoreEnergy    float32 `parquet:"ers_store_energy,type=FLOAT"`
	ErsDeployMode     int32   `parquet:"ers_deploy_mode,type=INT32"`
	Timestamp         int64   `parquet:"timestamp,type=INT64"`
}

// MapToMotionParquet converts parsed Motion Packet to flat Parquet structures
func MapToMotionParquet(p *PacketMotionData) []MotionParquet {
	records := make([]MotionParquet, 0, 22)
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < 22; i++ {
		c := p.CarMotionData[i]
		// Filter out cars with zero coordinates to save space
		if c.WorldPositionX == 0 && c.WorldPositionY == 0 && c.WorldPositionZ == 0 {
			continue
		}
		records = append(records, MotionParquet{
			SessionUID:         int64(p.Header.SessionUID),
			SessionTime:        p.Header.SessionTime,
			FrameIdentifier:    int64(p.Header.FrameIdentifier),
			CarIndex:           int32(i),
			IsPlayerCar:        i == int(p.Header.PlayerCarIndex),
			WorldPositionX:     c.WorldPositionX,
			WorldPositionY:     c.WorldPositionY,
			WorldPositionZ:     c.WorldPositionZ,
			WorldVelocityX:     c.WorldVelocityX,
			WorldVelocityY:     c.WorldVelocityY,
			WorldVelocityZ:     c.WorldVelocityZ,
			GForceLateral:      c.GForceLateral,
			GForceLongitudinal: c.GForceLongitudinal,
			GForceVertical:     c.GForceVertical,
			Yaw:                c.Yaw,
			Pitch:              c.Pitch,
			Roll:               c.Roll,
			Timestamp:          now,
		})
	}
	return records
}

// MapToLapParquet converts parsed Lap Data Packet to flat Parquet structures
func MapToLapParquet(p *PacketLapData) []LapParquet {
	records := make([]LapParquet, 0, 22)
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < 22; i++ {
		l := p.LapData[i]
		if l.ResultStatus == 0 { // Invalid status
			continue
		}
		s1 := int32(l.Sector1TimeMinutesPart)*60000 + int32(l.Sector1TimeMSPart)
		s2 := int32(l.Sector2TimeMinutesPart)*60000 + int32(l.Sector2TimeMSPart)

		records = append(records, LapParquet{
			SessionUID:         int64(p.Header.SessionUID),
			SessionTime:        p.Header.SessionTime,
			FrameIdentifier:    int64(p.Header.FrameIdentifier),
			CarIndex:           int32(i),
			IsPlayerCar:        i == int(p.Header.PlayerCarIndex),
			LastLapTimeInMS:    int32(l.LastLapTimeInMS),
			CurrentLapTimeInMS: int32(l.CurrentLapTimeInMS),
			Sector1TimeMS:      s1,
			Sector2TimeMS:      s2,
			CarPosition:        int32(l.CarPosition),
			CurrentLapNum:      int32(l.CurrentLapNum),
			PitStatus:          int32(l.PitStatus),
			Sector:             int32(l.Sector),
			Penalties:          int32(l.Penalties),
			DriverStatus:       int32(l.DriverStatus),
			ResultStatus:       int32(l.ResultStatus),
			Timestamp:          now,
		})
	}
	return records
}

// MapToTelemetryParquet converts parsed Telemetry Packet to flat Parquet structures
func MapToTelemetryParquet(p *PacketCarTelemetryData) []TelemetryParquet {
	records := make([]TelemetryParquet, 0, 22)
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < 22; i++ {
		t := p.CarTelemetryData[i]
		if t.Speed == 0 && t.EngineRPM == 0 && t.Throttle == 0 {
			continue
		}
		records = append(records, TelemetryParquet{
			SessionUID:        int64(p.Header.SessionUID),
			SessionTime:       p.Header.SessionTime,
			FrameIdentifier:   int64(p.Header.FrameIdentifier),
			CarIndex:          int32(i),
			IsPlayerCar:       i == int(p.Header.PlayerCarIndex),
			Speed:             int32(t.Speed),
			Throttle:          t.Throttle,
			Steer:             t.Steer,
			Brake:             t.Brake,
			Clutch:            int32(t.Clutch),
			Gear:              int32(t.Gear),
			EngineRPM:         int32(t.EngineRPM),
			Drs:               int32(t.Drs),
			EngineTemperature: int32(t.EngineTemperature),
			TyresTempRL:       int32(t.TyresSurfaceTemperature[0]),
			TyresTempRR:       int32(t.TyresSurfaceTemperature[1]),
			TyresTempFL:       int32(t.TyresSurfaceTemperature[2]),
			TyresTempFR:       int32(t.TyresSurfaceTemperature[3]),
			Timestamp:         now,
		})
	}
	return records
}

// MapToStatusParquet converts parsed Status Packet to flat Parquet structures
func MapToStatusParquet(p *PacketCarStatusData) []StatusParquet {
	records := make([]StatusParquet, 0, 22)
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < 22; i++ {
		s := p.CarStatusData[i]
		if s.FuelCapacity == 0 {
			continue
		}
		records = append(records, StatusParquet{
			SessionUID:         int64(p.Header.SessionUID),
			SessionTime:        p.Header.SessionTime,
			FrameIdentifier:    int64(p.Header.FrameIdentifier),
			CarIndex:           int32(i),
			IsPlayerCar:        i == int(p.Header.PlayerCarIndex),
			FuelInTank:         s.FuelInTank,
			FuelCapacity:       s.FuelCapacity,
			FuelRemainingLaps:  s.FuelRemainingLaps,
			MaxRPM:             int32(s.MaxRPM),
			IdleRPM:            int32(s.IdleRPM),
			MaxGears:           int32(s.MaxGears),
			DrsAllowed:         int32(s.DrsAllowed),
			ActualTyreCompound: int32(s.ActualTyreCompound),
			VisualTyreCompound: int32(s.VisualTyreCompound),
			TyresAgeLaps:       int32(s.TyresAgeLaps),
			ErsStoreEnergy:     s.ErsStoreEnergy,
			ErsDeployMode:      int32(s.ErsDeployMode),
			Timestamp:          now,
		})
	}
	return records
}

// SessionParquet represents a session data row in a Parquet file.
type SessionParquet struct {
	SessionUID      int64   `parquet:"session_uid,type=INT64"`
	SessionTime     float32 `parquet:"session_time,type=FLOAT"`
	FrameIdentifier int64   `parquet:"frame_identifier,type=INT64"`
	Weather         int32   `parquet:"weather,type=INT32"`
	TrackTemp       int32   `parquet:"track_temp,type=INT32"`
	AirTemp         int32   `parquet:"air_temp,type=INT32"`
	TotalLaps       int32   `parquet:"total_laps,type=INT32"`
	TrackLength     int32   `parquet:"track_length,type=INT32"`
	SessionType     int32   `parquet:"session_type,type=INT32"`
	TrackId         int32   `parquet:"track_id,type=INT32"`
	Timestamp       int64   `parquet:"timestamp,type=INT64"`
}

// EventParquet represents an event data row in a Parquet file.
type EventParquet struct {
	SessionUID        int64   `parquet:"session_uid,type=INT64"`
	SessionTime       float32 `parquet:"session_time,type=FLOAT"`
	FrameIdentifier   int64   `parquet:"frame_identifier,type=INT64"`
	EventCode         string  `parquet:"event_code,type=BYTE_ARRAY,convertedtype=UTF8"`
	VehicleIdx        int32   `parquet:"vehicle_idx,type=INT32"`
	LapTime           float32 `parquet:"lap_time,type=FLOAT"`
	PenaltyType       int32   `parquet:"penalty_type,type=INT32"`
	InfringementType  int32   `parquet:"infringement_type,type=INT32"`
	PenaltyTime       int32   `parquet:"penalty_time,type=INT32"`
	PlacesGained      int32   `parquet:"places_gained,type=INT32"`
	Speed             float32 `parquet:"speed,type=FLOAT"`
	NumLights         int32   `parquet:"num_lights,type=INT32"`
	FlashbackFrame    int64   `parquet:"flashback_frame,type=INT64"`
	FlashbackTime     float32 `parquet:"flashback_time,type=FLOAT"`
	OvertakingIdx     int32   `parquet:"overtaking_idx,type=INT32"`
	BeingOvertakenIdx int32   `parquet:"being_overtaken_idx,type=INT32"`
	SafetyCarType     int32   `parquet:"safety_car_type,type=INT32"`
	SafetyCarEvent    int32   `parquet:"safety_car_event,type=INT32"`
	Vehicle1Idx       int32   `parquet:"vehicle_1_idx,type=INT32"`
	Vehicle2Idx       int32   `parquet:"vehicle_2_idx,type=INT32"`
	Timestamp         int64   `parquet:"timestamp,type=INT64"`
}

// ParticipantParquet represents a participant's details row in a Parquet file.
type ParticipantParquet struct {
	SessionUID      int64  `parquet:"session_uid,type=INT64"`
	SessionTime     float32 `parquet:"session_time,type=FLOAT"`
	FrameIdentifier int64  `parquet:"frame_identifier,type=INT64"`
	CarIndex        int32  `parquet:"car_index,type=INT32"`
	AiControlled    bool   `parquet:"ai_controlled,type=BOOLEAN"`
	DriverId        int32  `parquet:"driver_id,type=INT32"`
	TeamId          int32  `parquet:"team_id,type=INT32"`
	RaceNumber      int32  `parquet:"race_number,type=INT32"`
	Nationality     int32  `parquet:"nationality,type=INT32"`
	Name            string `parquet:"name,type=BYTE_ARRAY,convertedtype=UTF8"`
	Platform        int32  `parquet:"platform,type=INT32"`
	Timestamp       int64  `parquet:"timestamp,type=INT64"`
}

// CarSetupParquet represents a single car's setup row in a Parquet file.
type CarSetupParquet struct {
	SessionUID             int64   `parquet:"session_uid,type=INT64"`
	SessionTime            float32 `parquet:"session_time,type=FLOAT"`
	FrameIdentifier        int64   `parquet:"frame_identifier,type=INT64"`
	CarIndex               int32   `parquet:"car_index,type=INT32"`
	IsPlayerCar            bool    `parquet:"is_player_car,type=BOOLEAN"`
	FrontWing              int32   `parquet:"front_wing,type=INT32"`
	RearWing               int32   `parquet:"rear_wing,type=INT32"`
	OnThrottle             int32   `parquet:"on_throttle,type=INT32"`
	OffThrottle            int32   `parquet:"off_throttle,type=INT32"`
	FrontCamber            float32 `parquet:"front_camber,type=FLOAT"`
	RearCamber             float32 `parquet:"rear_camber,type=FLOAT"`
	FrontToe               float32 `parquet:"front_toe,type=FLOAT"`
	RearToe                float32 `parquet:"rear_toe,type=FLOAT"`
	FrontSuspension        int32   `parquet:"front_suspension,type=INT32"`
	RearSuspension         int32   `parquet:"rear_suspension,type=INT32"`
	FrontAntiRollBar       int32   `parquet:"front_anti_roll_bar,type=INT32"`
	RearAntiRollBar        int32   `parquet:"rear_anti_roll_bar,type=INT32"`
	FrontSuspensionHeight  int32   `parquet:"front_suspension_height,type=INT32"`
	RearSuspensionHeight   int32   `parquet:"rear_suspension_height,type=INT32"`
	BrakePressure          int32   `parquet:"brake_pressure,type=INT32"`
	BrakeBias              int32   `parquet:"brake_bias,type=INT32"`
	EngineBraking          int32   `parquet:"engine_braking,type=INT32"`
	RearLeftTyrePressure   float32 `parquet:"rear_left_tyre_pressure,type=FLOAT"`
	RearRightTyrePressure  float32 `parquet:"rear_right_tyre_pressure,type=FLOAT"`
	FrontLeftTyrePressure  float32 `parquet:"front_left_tyre_pressure,type=FLOAT"`
	FrontRightTyrePressure float32 `parquet:"front_right_tyre_pressure,type=FLOAT"`
	Ballast                int32   `parquet:"ballast,type=INT32"`
	FuelLoad               float32 `parquet:"fuel_load,type=FLOAT"`
	Timestamp              int64   `parquet:"timestamp,type=INT64"`
}

// FinalClassificationParquet represents a single car's final classification row in a Parquet file.
type FinalClassificationParquet struct {
	SessionUID      int64   `parquet:"session_uid,type=INT64"`
	SessionTime     float32 `parquet:"session_time,type=FLOAT"`
	FrameIdentifier int64   `parquet:"frame_identifier,type=INT64"`
	CarIndex        int32   `parquet:"car_index,type=INT32"`
	Position        int32   `parquet:"position,type=INT32"`
	NumLaps         int32   `parquet:"num_laps,type=INT32"`
	GridPosition    int32   `parquet:"grid_position,type=INT32"`
	Points          int32   `parquet:"points,type=INT32"`
	NumPitStops     int32   `parquet:"num_pit_stops,type=INT32"`
	ResultStatus    int32   `parquet:"result_status,type=INT32"`
	BestLapTimeInMS int32   `parquet:"best_lap_time_in_ms,type=INT32"`
	TotalRaceTime   float64 `parquet:"total_race_time,type=DOUBLE"`
	PenaltiesTime   int32   `parquet:"penalties_time,type=INT32"`
	NumPenalties    int32   `parquet:"num_penalties,type=INT32"`
	NumTyreStints   int32   `parquet:"num_tyre_stints,type=INT32"`
	Timestamp       int64   `parquet:"timestamp,type=INT64"`
}

// CarDamageParquet represents a single car's damage row in a Parquet file.
type CarDamageParquet struct {
	SessionUID           int64   `parquet:"session_uid,type=INT64"`
	SessionTime          float32 `parquet:"session_time,type=FLOAT"`
	FrameIdentifier      int64   `parquet:"frame_identifier,type=INT64"`
	CarIndex             int32   `parquet:"car_index,type=INT32"`
	IsPlayerCar          bool    `parquet:"is_player_car,type=BOOLEAN"`
	TyresWearRL          float32 `parquet:"tyres_wear_rl,type=FLOAT"`
	TyresWearRR          float32 `parquet:"tyres_wear_rr,type=FLOAT"`
	TyresWearFL          float32 `parquet:"tyres_wear_fl,type=FLOAT"`
	TyresWearFR          float32 `parquet:"tyres_wear_fr,type=FLOAT"`
	TyresDamageRL        int32   `parquet:"tyres_damage_rl,type=INT32"`
	TyresDamageRR        int32   `parquet:"tyres_damage_rr,type=INT32"`
	TyresDamageFL        int32   `parquet:"tyres_damage_fl,type=INT32"`
	TyresDamageFR        int32   `parquet:"tyres_damage_fr,type=INT32"`
	FrontLeftWingDamage  int32   `parquet:"front_left_wing_damage,type=INT32"`
	FrontRightWingDamage int32   `parquet:"front_right_wing_damage,type=INT32"`
	RearWingDamage       int32   `parquet:"rear_wing_damage,type=INT32"`
	FloorDamage          int32   `parquet:"floor_damage,type=INT32"`
	DiffuserDamage       int32   `parquet:"diffuser_damage,type=INT32"`
	SidepodDamage        int32   `parquet:"sidepod_damage,type=INT32"`
	DrsFault             int32   `parquet:"drs_fault,type=INT32"`
	ErsFault             int32   `parquet:"ers_fault,type=INT32"`
	GearBoxDamage        int32   `parquet:"gear_box_damage,type=INT32"`
	EngineDamage         int32   `parquet:"engine_damage,type=INT32"`
	EngineMGUHWear       int32   `parquet:"engine_mguh_wear,type=INT32"`
	EngineESWear         int32   `parquet:"engine_es_wear,type=INT32"`
	EngineCEWear         int32   `parquet:"engine_ce_wear,type=INT32"`
	EngineICEWear        int32   `parquet:"engine_ice_wear,type=INT32"`
	EngineMGUKWear       int32   `parquet:"engine_mguk_wear,type=INT32"`
	EngineTCWear         int32   `parquet:"engine_tc_wear,type=INT32"`
	EngineBlown          int32   `parquet:"engine_blown,type=INT32"`
	EngineSeized         int32   `parquet:"engine_seized,type=INT32"`
	Timestamp            int64   `parquet:"timestamp,type=INT64"`
}

// SessionHistoryParquet represents a single lap's historical data row in a Parquet file.
type SessionHistoryParquet struct {
	SessionUID             int64   `parquet:"session_uid,type=INT64"`
	SessionTime            float32 `parquet:"session_time,type=FLOAT"`
	FrameIdentifier        int64   `parquet:"frame_identifier,type=INT64"`
	CarIndex               int32   `parquet:"car_index,type=INT32"`
	LapNumber              int32   `parquet:"lap_number,type=INT32"`
	LapTimeInMS            int32   `parquet:"lap_time_in_ms,type=INT32"`
	Sector1TimeMS          int32   `parquet:"sector_1_time_ms,type=INT32"`
	Sector2TimeMS          int32   `parquet:"sector_2_time_ms,type=INT32"`
	Sector3TimeMS          int32   `parquet:"sector_3_time_ms,type=INT32"`
	LapValidBitFlags       int32   `parquet:"lap_valid_bit_flags,type=INT32"`
	BestLapTimeLapNum      int32   `parquet:"best_lap_time_lap_num,type=INT32"`
	BestSector1LapNum      int32   `parquet:"best_sector_1_lap_num,type=INT32"`
	BestSector2LapNum      int32   `parquet:"best_sector_2_lap_num,type=INT32"`
	BestSector3LapNum      int32   `parquet:"best_sector_3_lap_num,type=INT32"`
	Timestamp              int64   `parquet:"timestamp,type=INT64"`
}

// TyreSetParquet represents a single tyre set row in a Parquet file.
type TyreSetParquet struct {
	SessionUID         int64   `parquet:"session_uid,type=INT64"`
	SessionTime        float32 `parquet:"session_time,type=FLOAT"`
	FrameIdentifier    int64   `parquet:"frame_identifier,type=INT64"`
	CarIndex           int32   `parquet:"car_index,type=INT32"`
	TyreSetIndex       int32   `parquet:"tyre_set_index,type=INT32"`
	ActualTyreCompound int32   `parquet:"actual_tyre_compound,type=INT32"`
	VisualTyreCompound int32   `parquet:"visual_tyre_compound,type=INT32"`
	Wear               int32   `parquet:"wear,type=INT32"`
	Available          int32   `parquet:"available,type=INT32"`
	LifeSpan           int32   `parquet:"life_span,type=INT32"`
	UsableLife         int32   `parquet:"usable_life,type=INT32"`
	LapDeltaTime       int32   `parquet:"lap_delta_time,type=INT32"`
	Fitted             bool    `parquet:"fitted,type=BOOLEAN"`
	FittedIdx          int32   `parquet:"fitted_idx,type=INT32"`
	Timestamp          int64   `parquet:"timestamp,type=INT64"`
}

// MotionExParquet represents player-only extended motion dynamics row in a Parquet file.
type MotionExParquet struct {
	SessionUID             int64   `parquet:"session_uid,type=INT64"`
	SessionTime            float32 `parquet:"session_time,type=FLOAT"`
	FrameIdentifier        int64   `parquet:"frame_identifier,type=INT64"`
	SuspensionPositionRL   float32 `parquet:"suspension_position_rl,type=FLOAT"`
	SuspensionPositionRR   float32 `parquet:"suspension_position_rr,type=FLOAT"`
	SuspensionPositionFL   float32 `parquet:"suspension_position_fl,type=FLOAT"`
	SuspensionPositionFR   float32 `parquet:"suspension_position_fr,type=FLOAT"`
	SuspensionVelocityRL   float32 `parquet:"suspension_velocity_rl,type=FLOAT"`
	SuspensionVelocityRR   float32 `parquet:"suspension_velocity_rr,type=FLOAT"`
	SuspensionVelocityFL   float32 `parquet:"suspension_velocity_fl,type=FLOAT"`
	SuspensionVelocityFR   float32 `parquet:"suspension_velocity_fr,type=FLOAT"`
	WheelSpeedRL           float32 `parquet:"wheel_speed_rl,type=FLOAT"`
	WheelSpeedRR           float32 `parquet:"wheel_speed_rr,type=FLOAT"`
	WheelSpeedFL           float32 `parquet:"wheel_speed_fl,type=FLOAT"`
	WheelSpeedFR           float32 `parquet:"wheel_speed_fr,type=FLOAT"`
	WheelSlipRatioRL       float32 `parquet:"wheel_slip_ratio_rl,type=FLOAT"`
	WheelSlipRatioRR       float32 `parquet:"wheel_slip_ratio_rr,type=FLOAT"`
	WheelSlipRatioFL       float32 `parquet:"wheel_slip_ratio_fl,type=FLOAT"`
	WheelSlipRatioFR       float32 `parquet:"wheel_slip_ratio_fr,type=FLOAT"`
	HeightOfCOGAboveGround float32 `parquet:"height_of_cog_above_ground,type=FLOAT"`
	LocalVelocityX         float32 `parquet:"local_velocity_x,type=FLOAT"`
	LocalVelocityY         float32 `parquet:"local_velocity_y,type=FLOAT"`
	LocalVelocityZ         float32 `parquet:"local_velocity_z,type=FLOAT"`
	AngularVelocityX       float32 `parquet:"angular_velocity_x,type=FLOAT"`
	AngularVelocityY       float32 `parquet:"angular_velocity_y,type=FLOAT"`
	AngularVelocityZ       float32 `parquet:"angular_velocity_z,type=FLOAT"`
	FrontWheelsAngle       float32 `parquet:"front_wheels_angle,type=FLOAT"`
	ChassisYaw             float32 `parquet:"chassis_yaw,type=FLOAT"`
	Timestamp              int64   `parquet:"timestamp,type=INT64"`
}

func MapToSessionParquet(p *PacketSessionData) []SessionParquet {
	now := time.Now().UnixNano() / int64(time.Millisecond)
	return []SessionParquet{{
		SessionUID:      int64(p.Header.SessionUID),
		SessionTime:     p.Header.SessionTime,
		FrameIdentifier: int64(p.Header.FrameIdentifier),
		Weather:         int32(p.Weather),
		TrackTemp:       int32(p.TrackTemp),
		AirTemp:         int32(p.AirTemp),
		TotalLaps:       int32(p.TotalLaps),
		TrackLength:     int32(p.TrackLength),
		SessionType:     int32(p.SessionType),
		TrackId:         int32(p.TrackId),
		Timestamp:       now,
	}}
}

func MapToEventParquet(p *PacketEventData) []EventParquet {
	now := time.Now().UnixNano() / int64(time.Millisecond)
	return []EventParquet{{
		SessionUID:        int64(p.Header.SessionUID),
		SessionTime:       p.Header.SessionTime,
		FrameIdentifier:   int64(p.Header.FrameIdentifier),
		EventCode:         p.EventCode,
		VehicleIdx:        int32(p.VehicleIdx),
		LapTime:           p.LapTime,
		PenaltyType:       int32(p.PenaltyType),
		InfringementType:  int32(p.InfringementType),
		PenaltyTime:       int32(p.PenaltyTime),
		PlacesGained:      int32(p.PlacesGained),
		Speed:             p.Speed,
		NumLights:         int32(p.NumLights),
		FlashbackFrame:    int64(p.FlashbackFrame),
		FlashbackTime:     p.FlashbackTime,
		OvertakingIdx:     int32(p.OvertakingIdx),
		BeingOvertakenIdx: int32(p.BeingOvertakenIdx),
		SafetyCarType:     int32(p.SafetyCarType),
		SafetyCarEvent:    int32(p.SafetyCarEvent),
		Vehicle1Idx:       int32(p.Vehicle1Idx),
		Vehicle2Idx:       int32(p.Vehicle2Idx),
		Timestamp:         now,
	}}
}

func MapToParticipantsParquet(p *PacketParticipantsData) []ParticipantParquet {
	records := make([]ParticipantParquet, 0, int(p.NumActiveCars))
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < 22; i++ {
		part := p.Participants[i]
		if part.Name == "" {
			continue
		}
		records = append(records, ParticipantParquet{
			SessionUID:      int64(p.Header.SessionUID),
			SessionTime:     p.Header.SessionTime,
			FrameIdentifier: int64(p.Header.FrameIdentifier),
			CarIndex:        int32(i),
			AiControlled:    part.AiControlled == 1,
			DriverId:        int32(part.DriverId),
			TeamId:          int32(part.TeamId),
			RaceNumber:      int32(part.RaceNumber),
			Nationality:     int32(part.Nationality),
			Name:            part.Name,
			Platform:        int32(part.Platform),
			Timestamp:       now,
		})
	}
	return records
}

func MapToCarSetupParquet(p *PacketCarSetupData) []CarSetupParquet {
	records := make([]CarSetupParquet, 0, 22)
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < 22; i++ {
		setup := p.CarSetups[i]
		if setup.FrontWing == 0 && setup.RearWing == 0 && setup.FuelLoad == 0 {
			continue
		}
		records = append(records, CarSetupParquet{
			SessionUID:             int64(p.Header.SessionUID),
			SessionTime:            p.Header.SessionTime,
			FrameIdentifier:        int64(p.Header.FrameIdentifier),
			CarIndex:               int32(i),
			IsPlayerCar:            i == int(p.Header.PlayerCarIndex),
			FrontWing:              int32(setup.FrontWing),
			RearWing:               int32(setup.RearWing),
			OnThrottle:             int32(setup.OnThrottle),
			OffThrottle:            int32(setup.OffThrottle),
			FrontCamber:            setup.FrontCamber,
			RearCamber:             setup.RearCamber,
			FrontToe:               setup.FrontToe,
			RearToe:                setup.RearToe,
			FrontSuspension:        int32(setup.FrontSuspension),
			RearSuspension:         int32(setup.RearSuspension),
			FrontAntiRollBar:       int32(setup.FrontAntiRollBar),
			RearAntiRollBar:        int32(setup.RearAntiRollBar),
			FrontSuspensionHeight:  int32(setup.FrontSuspensionHeight),
			RearSuspensionHeight:   int32(setup.RearSuspensionHeight),
			BrakePressure:          int32(setup.BrakePressure),
			BrakeBias:              int32(setup.BrakeBias),
			EngineBraking:          int32(setup.EngineBraking),
			RearLeftTyrePressure:   setup.RearLeftTyrePressure,
			RearRightTyrePressure:  setup.RearRightTyrePressure,
			FrontLeftTyrePressure:  setup.FrontLeftTyrePressure,
			FrontRightTyrePressure: setup.FrontRightTyrePressure,
			Ballast:                int32(setup.Ballast),
			FuelLoad:               setup.FuelLoad,
			Timestamp:              now,
		})
	}
	return records
}

func MapToFinalClassificationParquet(p *PacketFinalClassificationData) []FinalClassificationParquet {
	records := make([]FinalClassificationParquet, 0, int(p.NumCars))
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < 22; i++ {
		class := p.ClassificationData[i]
		if class.Position == 0 {
			continue
		}
		records = append(records, FinalClassificationParquet{
			SessionUID:      int64(p.Header.SessionUID),
			SessionTime:     p.Header.SessionTime,
			FrameIdentifier: int64(p.Header.FrameIdentifier),
			CarIndex:        int32(i),
			Position:        int32(class.Position),
			NumLaps:         int32(class.NumLaps),
			GridPosition:    int32(class.GridPosition),
			Points:          int32(class.Points),
			NumPitStops:     int32(class.NumPitStops),
			ResultStatus:    int32(class.ResultStatus),
			BestLapTimeInMS: int32(class.BestLapTimeInMS),
			TotalRaceTime:   class.TotalRaceTime,
			PenaltiesTime:   int32(class.PenaltiesTime),
			NumPenalties:    int32(class.NumPenalties),
			NumTyreStints:   int32(class.NumTyreStints),
			Timestamp:       now,
		})
	}
	return records
}

func MapToCarDamageParquet(p *PacketCarDamageData) []CarDamageParquet {
	records := make([]CarDamageParquet, 0, 22)
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < 22; i++ {
		dmg := p.CarDamageData[i]
		if dmg.FrontLeftWingDamage == 0 && dmg.FrontRightWingDamage == 0 && dmg.GearBoxDamage == 0 && dmg.EngineDamage == 0 {
			if i != int(p.Header.PlayerCarIndex) {
				continue
			}
		}
		records = append(records, CarDamageParquet{
			SessionUID:           int64(p.Header.SessionUID),
			SessionTime:          p.Header.SessionTime,
			FrameIdentifier:      int64(p.Header.FrameIdentifier),
			CarIndex:             int32(i),
			IsPlayerCar:          i == int(p.Header.PlayerCarIndex),
			TyresWearRL:          dmg.TyresWear[0],
			TyresWearRR:          dmg.TyresWear[1],
			TyresWearFL:          dmg.TyresWear[2],
			TyresWearFR:          dmg.TyresWear[3],
			TyresDamageRL:        int32(dmg.TyresDamage[0]),
			TyresDamageRR:        int32(dmg.TyresDamage[1]),
			TyresDamageFL:        int32(dmg.TyresDamage[2]),
			TyresDamageFR:        int32(dmg.TyresDamage[3]),
			FrontLeftWingDamage:  int32(dmg.FrontLeftWingDamage),
			FrontRightWingDamage: int32(dmg.FrontRightWingDamage),
			RearWingDamage:       int32(dmg.RearWingDamage),
			FloorDamage:          int32(dmg.FloorDamage),
			DiffuserDamage:       int32(dmg.DiffuserDamage),
			SidepodDamage:        int32(dmg.SidepodDamage),
			DrsFault:             int32(dmg.DrsFault),
			ErsFault:             int32(dmg.ErsFault),
			GearBoxDamage:        int32(dmg.GearBoxDamage),
			EngineDamage:         int32(dmg.EngineDamage),
			EngineMGUHWear:       int32(dmg.EngineMGUHWear),
			EngineESWear:         int32(dmg.EngineESWear),
			EngineCEWear:         int32(dmg.EngineCEWear),
			EngineICEWear:        int32(dmg.EngineICEWear),
			EngineMGUKWear:       int32(dmg.EngineMGUKWear),
			EngineTCWear:         int32(dmg.EngineTCWear),
			EngineBlown:          int32(dmg.EngineBlown),
			EngineSeized:         int32(dmg.EngineSeized),
			Timestamp:            now,
		})
	}
	return records
}

func MapToSessionHistoryParquet(p *PacketSessionHistoryData) []SessionHistoryParquet {
	records := make([]SessionHistoryParquet, 0, int(p.NumLaps))
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < int(p.NumLaps); i++ {
		lap := p.LapHistoryData[i]
		if lap.LapTimeInMS == 0 {
			continue
		}
		s1 := int32(lap.Sector1TimeMinutesPart)*60000 + int32(lap.Sector1TimeMSPart)
		s2 := int32(lap.Sector2TimeMinutesPart)*60000 + int32(lap.Sector2TimeMSPart)
		s3 := int32(lap.Sector3TimeMinutesPart)*60000 + int32(lap.Sector3TimeMSPart)

		records = append(records, SessionHistoryParquet{
			SessionUID:        int64(p.Header.SessionUID),
			SessionTime:       p.Header.SessionTime,
			FrameIdentifier:   int64(p.Header.FrameIdentifier),
			CarIndex:          int32(p.CarIdx),
			LapNumber:         int32(i + 1),
			LapTimeInMS:       int32(lap.LapTimeInMS),
			Sector1TimeMS:     s1,
			Sector2TimeMS:     s2,
			Sector3TimeMS:     s3,
			LapValidBitFlags:  int32(lap.LapValidBitFlags),
			BestLapTimeLapNum: int32(p.BestLapTimeLapNum),
			BestSector1LapNum: int32(p.BestSector1LapNum),
			BestSector2LapNum: int32(p.BestSector2LapNum),
			BestSector3LapNum: int32(p.BestSector3LapNum),
			Timestamp:         now,
		})
	}
	return records
}

func MapToTyreSetParquet(p *PacketTyreSetsData) []TyreSetParquet {
	records := make([]TyreSetParquet, 0, 20)
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < 20; i++ {
		set := p.TyreSetData[i]
		if set.LifeSpan == 0 && set.UsableLife == 0 {
			continue
		}
		records = append(records, TyreSetParquet{
			SessionUID:         int64(p.Header.SessionUID),
			SessionTime:        p.Header.SessionTime,
			FrameIdentifier:    int64(p.Header.FrameIdentifier),
			CarIndex:           int32(p.CarIdx),
			TyreSetIndex:       int32(i),
			ActualTyreCompound: int32(set.ActualTyreCompound),
			VisualTyreCompound: int32(set.VisualTyreCompound),
			Wear:               int32(set.Wear),
			Available:          int32(set.Available),
			LifeSpan:           int32(set.LifeSpan),
			UsableLife:         int32(set.UsableLife),
			LapDeltaTime:       int32(set.LapDeltaTime),
			Fitted:             set.Fitted == 1,
			FittedIdx:          int32(p.FittedIdx),
			Timestamp:          now,
		})
	}
	return records
}

func MapToMotionExParquet(p *PacketMotionExData) []MotionExParquet {
	now := time.Now().UnixNano() / int64(time.Millisecond)
	return []MotionExParquet{{
		SessionUID:             int64(p.Header.SessionUID),
		SessionTime:            p.Header.SessionTime,
		FrameIdentifier:        int64(p.Header.FrameIdentifier),
		SuspensionPositionRL:   p.SuspensionPosition[0],
		SuspensionPositionRR:   p.SuspensionPosition[1],
		SuspensionPositionFL:   p.SuspensionPosition[2],
		SuspensionPositionFR:   p.SuspensionPosition[3],
		SuspensionVelocityRL:   p.SuspensionVelocity[0],
		SuspensionVelocityRR:   p.SuspensionVelocity[1],
		SuspensionVelocityFL:   p.SuspensionVelocity[2],
		SuspensionVelocityFR:   p.SuspensionVelocity[3],
		WheelSpeedRL:           p.WheelSpeed[0],
		WheelSpeedRR:           p.WheelSpeed[1],
		WheelSpeedFL:           p.WheelSpeed[2],
		WheelSpeedFR:           p.WheelSpeed[3],
		WheelSlipRatioRL:       p.WheelSlipRatio[0],
		WheelSlipRatioRR:       p.WheelSlipRatio[1],
		WheelSlipRatioFL:       p.WheelSlipRatio[2],
		WheelSlipRatioFR:       p.WheelSlipRatio[3],
		HeightOfCOGAboveGround: p.HeightOfCOGAboveGround,
		LocalVelocityX:         p.LocalVelocityX,
		LocalVelocityY:         p.LocalVelocityY,
		LocalVelocityZ:         p.LocalVelocityZ,
		AngularVelocityX:       p.AngularVelocityX,
		AngularVelocityY:       p.AngularVelocityY,
		AngularVelocityZ:       p.AngularVelocityZ,
		FrontWheelsAngle:       p.FrontWheelsAngle,
		ChassisYaw:             p.ChassisYaw,
		Timestamp:              now,
	}}
}

