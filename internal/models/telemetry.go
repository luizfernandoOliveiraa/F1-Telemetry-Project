package models

import (
	"encoding/binary"
	"math"
)

// Packet IDs
const (
	PacketIDMotion              = 0
	PacketIDSession             = 1
	PacketIDLapData             = 2
	PacketIDEvent               = 3
	PacketIDParticipants        = 4
	PacketIDCarSetups           = 5
	PacketIDCarTelemetry        = 6
	PacketIDCarStatus           = 7
	PacketIDFinalClassification = 8
	PacketIDLobbyInfo           = 9
	PacketIDCarDamage           = 10
	PacketIDSessionHistory      = 11
	PacketIDTyreSets            = 12
	PacketIDMotionEx            = 13
)

// PacketHeader represents the 29-byte header sent at the start of every telemetry packet.
type PacketHeader struct {
	PacketFormat            uint16  `json:"packet_format"`
	GameYear                uint8   `json:"game_year"`
	GameMajorVersion        uint8   `json:"game_major_version"`
	GameMinorVersion        uint8   `json:"game_minor_version"`
	PacketVersion           uint8   `json:"packet_version"`
	PacketId                uint8   `json:"packet_id"`
	SessionUID              uint64  `json:"session_uid"`
	SessionTime             float32 `json:"session_time"`
	FrameIdentifier         uint32  `json:"frame_identifier"`
	OverallFrameIdentifier  uint32  `json:"overall_frame_identifier"`
	PlayerCarIndex          uint8   `json:"player_car_index"`
	SecondaryPlayerCarIndex uint8   `json:"secondary_player_car_index"`
}

func ParsePacketHeader(buf []byte) PacketHeader {
	if len(buf) < 29 {
		return PacketHeader{}
	}
	return PacketHeader{
		PacketFormat:            binary.LittleEndian.Uint16(buf[0:2]),
		GameYear:                buf[2],
		GameMajorVersion:        buf[3],
		GameMinorVersion:        buf[4],
		PacketVersion:           buf[5],
		PacketId:                buf[6],
		SessionUID:              binary.LittleEndian.Uint64(buf[7:15]),
		SessionTime:             math.Float32frombits(binary.LittleEndian.Uint32(buf[15:19])),
		FrameIdentifier:         binary.LittleEndian.Uint32(buf[19:23]),
		OverallFrameIdentifier:  binary.LittleEndian.Uint32(buf[23:27]),
		PlayerCarIndex:          buf[27],
		SecondaryPlayerCarIndex: buf[28],
	}
}

// --- MOTION DATA (Packet ID 0) ---

type CarMotionData struct {
	WorldPositionX     float32 `json:"world_position_x"`
	WorldPositionY     float32 `json:"world_position_y"`
	WorldPositionZ     float32 `json:"world_position_z"`
	WorldVelocityX     float32 `json:"world_velocity_x"`
	WorldVelocityY     float32 `json:"world_velocity_y"`
	WorldVelocityZ     float32 `json:"world_velocity_z"`
	WorldForwardDirX   int16   `json:"world_forward_dir_x"`
	WorldForwardDirY   int16   `json:"world_forward_dir_y"`
	WorldForwardDirZ   int16   `json:"world_forward_dir_z"`
	WorldRightDirX     int16   `json:"world_right_dir_x"`
	WorldRightDirY     int16   `json:"world_right_dir_y"`
	WorldRightDirZ     int16   `json:"world_right_dir_z"`
	GForceLateral      float32 `json:"g_force_lateral"`
	GForceLongitudinal float32 `json:"g_force_longitudinal"`
	GForceVertical     float32 `json:"g_force_vertical"`
	Yaw                float32 `json:"yaw"`
	Pitch              float32 `json:"pitch"`
	Roll               float32 `json:"roll"`
}

type PacketMotionData struct {
	Header        PacketHeader      `json:"header"`
	CarMotionData [22]CarMotionData `json:"car_motion_data"`
}

func ParsePacketMotionData(buf []byte, header PacketHeader) PacketMotionData {
	p := PacketMotionData{Header: header}
	if len(buf) < 1349 {
		return p
	}
	offset := 29
	for i := 0; i < 22; i++ {
		p.CarMotionData[i] = CarMotionData{
			WorldPositionX:     math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4])),
			WorldPositionY:     math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+4 : offset+8])),
			WorldPositionZ:     math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+8 : offset+12])),
			WorldVelocityX:     math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+12 : offset+16])),
			WorldVelocityY:     math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+16 : offset+20])),
			WorldVelocityZ:     math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+20 : offset+24])),
			WorldForwardDirX:   int16(binary.LittleEndian.Uint16(buf[offset+24 : offset+26])),
			WorldForwardDirY:   int16(binary.LittleEndian.Uint16(buf[offset+26 : offset+28])),
			WorldForwardDirZ:   int16(binary.LittleEndian.Uint16(buf[offset+28 : offset+30])),
			WorldRightDirX:     int16(binary.LittleEndian.Uint16(buf[offset+30 : offset+32])),
			WorldRightDirY:     int16(binary.LittleEndian.Uint16(buf[offset+32 : offset+34])),
			WorldRightDirZ:     int16(binary.LittleEndian.Uint16(buf[offset+34 : offset+36])),
			GForceLateral:      math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+36 : offset+40])),
			GForceLongitudinal: math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+40 : offset+44])),
			GForceVertical:     math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+44 : offset+48])),
			Yaw:                math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+48 : offset+52])),
			Pitch:              math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+52 : offset+56])),
			Roll:               math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+56 : offset+60])),
		}
		offset += 60
	}
	return p
}

// --- SESSION DATA (Packet ID 1) ---

type MarshalZone struct {
	ZoneStart float32 `json:"zone_start"`
	ZoneFlag  int8    `json:"zone_flag"`
}

type WeatherForecastSample struct {
	SessionType            uint8 `json:"session_type"`
	TimeOffset             uint8 `json:"time_offset"`
	Weather                uint8 `json:"weather"`
	TrackTemperature       int8  `json:"track_temperature"`
	TrackTemperatureChange int8  `json:"track_temperature_change"`
	AirTemperature         int8  `json:"air_temperature"`
	AirTemperatureChange   int8  `json:"air_temperature_change"`
	RainPercentage         uint8 `json:"rain_percentage"`
}

type PacketSessionData struct {
	Header         PacketHeader `json:"header"`
	Weather        uint8        `json:"weather"`
	TrackTemp      int8         `json:"track_temp"`
	AirTemp        int8         `json:"air_temp"`
	TotalLaps      uint8        `json:"total_laps"`
	TrackLength    uint16       `json:"track_length"`
	SessionType    uint8        `json:"session_type"`
	TrackId        int8         `json:"track_id"`
	Formula        uint8        `json:"formula"`
	SessionTimeLeft uint16      `json:"session_time_left"`
	SessionDuration uint16       `json:"session_duration"`
	PitSpeedLimit  uint8        `json:"pit_speed_limit"`
	GamePaused     uint8        `json:"game_paused"`
	IsSpectating   uint8        `json:"is_spectating"`
	SpectatorCarIndex uint8     `json:"spectator_car_index"`
	// Additional fields are not critical for main dashboard UI but we map the sizes to be compliant
}

func ParsePacketSessionData(buf []byte, header PacketHeader) PacketSessionData {
	p := PacketSessionData{Header: header}
	if len(buf) < 753 {
		return p
	}
	p.Weather = buf[29]
	p.TrackTemp = int8(buf[30])
	p.AirTemp = int8(buf[31])
	p.TotalLaps = buf[32]
	p.TrackLength = binary.LittleEndian.Uint16(buf[33:35])
	p.SessionType = buf[35]
	p.TrackId = int8(buf[36])
	p.Formula = buf[37]
	p.SessionTimeLeft = binary.LittleEndian.Uint16(buf[38:40])
	p.SessionDuration = binary.LittleEndian.Uint16(buf[40:42])
	p.PitSpeedLimit = buf[42]
	p.GamePaused = buf[43]
	p.IsSpectating = buf[44]
	p.SpectatorCarIndex = buf[45]
	return p
}

// --- LAP DATA (Packet ID 2) ---

type LapData struct {
	LastLapTimeInMS             uint32  `json:"last_lap_time_in_ms"`
	CurrentLapTimeInMS          uint32  `json:"current_lap_time_in_ms"`
	Sector1TimeMSPart           uint16  `json:"sector_1_time_ms_part"`
	Sector1TimeMinutesPart      uint8   `json:"sector_1_time_minutes_part"`
	Sector2TimeMSPart           uint16  `json:"sector_2_time_ms_part"`
	Sector2TimeMinutesPart      uint8   `json:"sector_2_time_minutes_part"`
	DeltaToCarInFrontMSPart     uint16  `json:"delta_to_car_in_front_ms_part"`
	DeltaToCarInFrontMinutesPart uint8  `json:"delta_to_car_in_front_minutes_part"`
	DeltaToRaceLeaderMSPart      uint16  `json:"delta_to_race_leader_ms_part"`
	DeltaToRaceLeaderMinutesPart uint8  `json:"delta_to_race_leader_minutes_part"`
	LapDistance                 float32 `json:"lap_distance"`
	TotalDistance               float32 `json:"total_distance"`
	SafetyCarDelta              float32 `json:"safety_car_delta"`
	CarPosition                 uint8   `json:"car_position"`
	CurrentLapNum               uint8   `json:"current_lap_num"`
	PitStatus                   uint8   `json:"pit_status"`
	NumPitStops                 uint8   `json:"num_pit_stops"`
	Sector                      uint8   `json:"sector"`
	CurrentLapInvalid           uint8   `json:"current_lap_invalid"`
	Penalties                   uint8   `json:"penalties"`
	TotalWarnings               uint8   `json:"total_warnings"`
	CornerCuttingWarnings       uint8   `json:"corner_cutting_warnings"`
	NumUnservedDriveThroughPens uint8   `json:"num_unserved_drive_through_pens"`
	NumUnservedStopGoPens       uint8   `json:"num_unserved_stop_go_pens"`
	GridPosition                uint8   `json:"grid_position"`
	DriverStatus                uint8   `json:"driver_status"`
	ResultStatus                uint8   `json:"result_status"`
	PitLaneTimerActive          uint8   `json:"pit_lane_timer_active"`
	PitLaneTimeInLaneInMS       uint16  `json:"pit_lane_time_in_lane_in_ms"`
	PitStopTimerInMS            uint16  `json:"pit_stop_timer_in_ms"`
	PitStopShouldServePen       uint8   `json:"pit_stop_should_serve_pen"`
	SpeedTrapFastestSpeed       float32 `json:"speed_trap_fastest_speed"`
	SpeedTrapFastestLap         uint8   `json:"speed_trap_fastest_lap"`
}

type PacketLapData struct {
	Header         PacketHeader  `json:"header"`
	LapData        [22]LapData   `json:"lap_data"`
	TimeTrialPBCarIdx    uint8   `json:"time_trial_pb_car_idx"`
	TimeTrialRivalCarIdx uint8   `json:"time_trial_rival_car_idx"`
}

func ParsePacketLapData(buf []byte, header PacketHeader) PacketLapData {
	p := PacketLapData{Header: header}
	if len(buf) < 1285 {
		return p
	}
	offset := 29
	for i := 0; i < 22; i++ {
		p.LapData[i] = LapData{
			LastLapTimeInMS:             binary.LittleEndian.Uint32(buf[offset : offset+4]),
			CurrentLapTimeInMS:          binary.LittleEndian.Uint32(buf[offset+4 : offset+8]),
			Sector1TimeMSPart:           binary.LittleEndian.Uint16(buf[offset+8 : offset+10]),
			Sector1TimeMinutesPart:      buf[offset+10],
			Sector2TimeMSPart:           binary.LittleEndian.Uint16(buf[offset+11 : offset+13]),
			Sector2TimeMinutesPart:      buf[offset+13],
			DeltaToCarInFrontMSPart:     binary.LittleEndian.Uint16(buf[offset+14 : offset+16]),
			DeltaToCarInFrontMinutesPart: buf[offset+16],
			DeltaToRaceLeaderMSPart:      binary.LittleEndian.Uint16(buf[offset+17 : offset+19]),
			DeltaToRaceLeaderMinutesPart: buf[offset+19],
			LapDistance:                 math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+20 : offset+24])),
			TotalDistance:               math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+24 : offset+28])),
			SafetyCarDelta:              math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+28 : offset+32])),
			CarPosition:                 buf[offset+32],
			CurrentLapNum:               buf[offset+33],
			PitStatus:                   buf[offset+34],
			NumPitStops:                 buf[offset+35],
			Sector:                      buf[offset+36],
			CurrentLapInvalid:           buf[offset+37],
			Penalties:                   buf[offset+38],
			TotalWarnings:               buf[offset+39],
			CornerCuttingWarnings:       buf[offset+40],
			NumUnservedDriveThroughPens: buf[offset+41],
			NumUnservedStopGoPens:       buf[offset+42],
			GridPosition:                buf[offset+43],
			DriverStatus:                buf[offset+44],
			ResultStatus:                buf[offset+45],
			PitLaneTimerActive:          buf[offset+46],
			PitLaneTimeInLaneInMS:       binary.LittleEndian.Uint16(buf[offset+47 : offset+49]),
			PitStopTimerInMS:            binary.LittleEndian.Uint16(buf[offset+49 : offset+51]),
			PitStopShouldServePen:       buf[offset+51],
			SpeedTrapFastestSpeed:       math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+52 : offset+56])),
			SpeedTrapFastestLap:         buf[offset+56],
		}
		offset += 57
	}
	p.TimeTrialPBCarIdx = buf[1283]
	p.TimeTrialRivalCarIdx = buf[1284]
	return p
}

// --- PARTICIPANTS DATA (Packet ID 4) ---

type ParticipantData struct {
	AiControlled    uint8    `json:"ai_controlled"`
	DriverId        uint8    `json:"driver_id"`
	NetworkId       uint8    `json:"network_id"`
	TeamId          uint8    `json:"team_id"`
	MyTeam          uint8    `json:"my_team"`
	RaceNumber      uint8    `json:"race_number"`
	Nationality     uint8    `json:"nationality"`
	Name            string   `json:"name"`
	YourTelemetry   uint8    `json:"your_telemetry"`
	ShowOnlineNames uint8    `json:"show_online_names"`
	TechLevel       uint16   `json:"tech_level"`
	Platform        uint8    `json:"platform"`
}

type PacketParticipantsData struct {
	Header        PacketHeader      `json:"header"`
	NumActiveCars uint8             `json:"num_active_cars"`
	Participants  [22]ParticipantData `json:"participants"`
}

func ParsePacketParticipantsData(buf []byte, header PacketHeader) PacketParticipantsData {
	p := PacketParticipantsData{Header: header}
	if len(buf) < 1350 {
		return p
	}
	p.NumActiveCars = buf[29]
	offset := 30
	for i := 0; i < 22; i++ {
		// Read name (48 bytes null terminated string)
		nameBytes := buf[offset+7 : offset+7+48]
		nameLen := 0
		for nameLen < len(nameBytes) && nameBytes[nameLen] != 0 {
			nameLen++
		}
		name := string(nameBytes[:nameLen])

		p.Participants[i] = ParticipantData{
			AiControlled:    buf[offset],
			DriverId:        buf[offset+1],
			NetworkId:       buf[offset+2],
			TeamId:          buf[offset+3],
			MyTeam:          buf[offset+4],
			RaceNumber:      buf[offset+5],
			Nationality:     buf[offset+6],
			Name:            name,
			YourTelemetry:   buf[offset+55],
			ShowOnlineNames: buf[offset+56],
			TechLevel:       binary.LittleEndian.Uint16(buf[offset+57 : offset+59]),
			Platform:        buf[offset+59],
		}
		offset += 60
	}
	return p
}

// --- CAR TELEMETRY (Packet ID 6) ---

type CarTelemetryData struct {
	Speed                   uint16     `json:"speed"`
	Throttle                float32    `json:"throttle"`
	Steer                   float32    `json:"steer"`
	Brake                   float32    `json:"brake"`
	Clutch                  uint8      `json:"clutch"`
	Gear                    int8       `json:"gear"`
	EngineRPM               uint16     `json:"engine_rpm"`
	Drs                     uint8      `json:"drs"`
	RevLightsPercent        uint8      `json:"rev_lights_percent"`
	RevLightsBitValue       uint16     `json:"rev_lights_bit_value"`
	BrakesTemperature       [4]uint16  `json:"brakes_temperature"`
	TyresSurfaceTemperature [4]uint8   `json:"tyres_surface_temperature"`
	TyresInnerTemperature   [4]uint8   `json:"tyres_inner_temperature"`
	EngineTemperature       uint16     `json:"engine_temperature"`
	TyresPressure           [4]float32 `json:"tyres_pressure"`
	SurfaceType             [4]uint8   `json:"surface_type"`
}

type PacketCarTelemetryData struct {
	Header                         PacketHeader       `json:"header"`
	CarTelemetryData               [22]CarTelemetryData `json:"car_telemetry_data"`
	MfdPanelIndex                  uint8              `json:"mfd_panel_index"`
	MfdPanelIndexSecondaryPlayer   uint8              `json:"mfd_panel_index_secondary_player"`
	SuggestedGear                  int8               `json:"suggested_gear"`
}

func ParsePacketCarTelemetryData(buf []byte, header PacketHeader) PacketCarTelemetryData {
	p := PacketCarTelemetryData{Header: header}
	if len(buf) < 1352 {
		return p
	}
	offset := 29
	for i := 0; i < 22; i++ {
		var bt [4]uint16
		bt[0] = binary.LittleEndian.Uint16(buf[offset+18 : offset+20])
		bt[1] = binary.LittleEndian.Uint16(buf[offset+20 : offset+22])
		bt[2] = binary.LittleEndian.Uint16(buf[offset+22 : offset+24])
		bt[3] = binary.LittleEndian.Uint16(buf[offset+24 : offset+26])

		var tst [4]uint8
		tst[0] = buf[offset+26]
		tst[1] = buf[offset+27]
		tst[2] = buf[offset+28]
		tst[3] = buf[offset+29]

		var tit [4]uint8
		tit[0] = buf[offset+30]
		tit[1] = buf[offset+31]
		tit[2] = buf[offset+32]
		tit[3] = buf[offset+33]

		var tp [4]float32
		tp[0] = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+36 : offset+40]))
		tp[1] = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+40 : offset+44]))
		tp[2] = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+44 : offset+48]))
		tp[3] = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+48 : offset+52]))

		var st [4]uint8
		st[0] = buf[offset+52]
		st[1] = buf[offset+53]
		st[2] = buf[offset+54]
		st[3] = buf[offset+55]

		p.CarTelemetryData[i] = CarTelemetryData{
			Speed:                   binary.LittleEndian.Uint16(buf[offset : offset+2]),
			Throttle:                math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+2 : offset+6])),
			Steer:                   math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+6 : offset+10])),
			Brake:                   math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+10 : offset+14])),
			Clutch:                  buf[offset+14],
			Gear:                    int8(buf[offset+15]),
			EngineRPM:               binary.LittleEndian.Uint16(buf[offset+16 : offset+18]),
			Drs:                     buf[offset+24], // Wait, m_drs is at offset 24 in C struct?
			// Let's re-verify:
			// Speed (2) + Throttle (4) + Steer (4) + Brake (4) + Clutch (1) + Gear (1) + EngineRPM (2) = 18 bytes
			// Drs (1) -> Offset 18?
			// In struct:
			// uint16 m_speed; // 2
			// float m_throttle; // 4
			// float m_steer; // 4
			// float m_brake; // 4
			// uint8 m_clutch; // 1
			// int8 m_gear; // 1
			// uint16 m_engineRPM; // 2
			// Total so far: 18 bytes.
			// Next fields:
			// uint8 m_drs; // 1 (Offset 18)
			// uint8 m_revLightsPercent; // 1 (Offset 19)
			// uint16 m_revLightsBitValue; // 2 (Offset 20)
			// uint16 m_brakesTemperature[4]; // 8 (Offset 22)
			// uint8 m_tyresSurfaceTemperature[4]; // 4 (Offset 30)
			// uint8 m_tyresInnerTemperature[4]; // 4 (Offset 34)
			// uint16 m_engineTemperature; // 2 (Offset 38)
			// float m_tyresPressure[4]; // 16 (Offset 40)
			// uint8 m_surfaceType[4]; // 4 (Offset 56)
			// Total size: 60 bytes per car.
			// Let's fix offsets in the code:
		}
		p.CarTelemetryData[i].Speed = binary.LittleEndian.Uint16(buf[offset : offset+2])
		p.CarTelemetryData[i].Throttle = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+2 : offset+6]))
		p.CarTelemetryData[i].Steer = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+6 : offset+10]))
		p.CarTelemetryData[i].Brake = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+10 : offset+14]))
		p.CarTelemetryData[i].Clutch = buf[offset+14]
		p.CarTelemetryData[i].Gear = int8(buf[offset+15])
		p.CarTelemetryData[i].EngineRPM = binary.LittleEndian.Uint16(buf[offset+16 : offset+18])
		p.CarTelemetryData[i].Drs = buf[offset+18]
		p.CarTelemetryData[i].RevLightsPercent = buf[offset+19]
		p.CarTelemetryData[i].RevLightsBitValue = binary.LittleEndian.Uint16(buf[offset+20 : offset+22])
		p.CarTelemetryData[i].BrakesTemperature = [4]uint16{
			binary.LittleEndian.Uint16(buf[offset+22 : offset+24]),
			binary.LittleEndian.Uint16(buf[offset+24 : offset+26]),
			binary.LittleEndian.Uint16(buf[offset+26 : offset+28]),
			binary.LittleEndian.Uint16(buf[offset+28 : offset+30]),
		}
		p.CarTelemetryData[i].TyresSurfaceTemperature = [4]uint8{
			buf[offset+30],
			buf[offset+31],
			buf[offset+32],
			buf[offset+33],
		}
		p.CarTelemetryData[i].TyresInnerTemperature = [4]uint8{
			buf[offset+34],
			buf[offset+35],
			buf[offset+36],
			buf[offset+37],
		}
		p.CarTelemetryData[i].EngineTemperature = binary.LittleEndian.Uint16(buf[offset+38 : offset+40])
		p.CarTelemetryData[i].TyresPressure = [4]float32{
			math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+40 : offset+44])),
			math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+44 : offset+48])),
			math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+48 : offset+52])),
			math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+52 : offset+56])),
		}
		p.CarTelemetryData[i].SurfaceType = [4]uint8{
			buf[offset+56],
			buf[offset+57],
			buf[offset+58],
			buf[offset+59],
		}

		offset += 60
	}
	p.MfdPanelIndex = buf[1349]
	p.MfdPanelIndexSecondaryPlayer = buf[1350]
	p.SuggestedGear = int8(buf[1351])
	return p
}

// --- CAR STATUS (Packet ID 7) ---

type CarStatusData struct {
	TractionControl         uint8   `json:"traction_control"`
	AntiLockBrakes          uint8   `json:"anti_lock_brakes"`
	FuelMix                 uint8   `json:"fuel_mix"`
	FrontBrakeBias          uint8   `json:"front_brake_bias"`
	PitLimiterStatus        uint8   `json:"pit_limiter_status"`
	FuelInTank              float32 `json:"fuel_in_tank"`
	FuelCapacity            float32 `json:"fuel_capacity"`
	FuelRemainingLaps       float32 `json:"fuel_remaining_laps"`
	MaxRPM                  uint16  `json:"max_rpm"`
	IdleRPM                 uint16  `json:"idle_rpm"`
	MaxGears                uint8   `json:"max_gears"`
	DrsAllowed              uint8   `json:"drs_allowed"`
	DrsActivationDistance   uint16  `json:"drs_activation_distance"`
	ActualTyreCompound      uint8   `json:"actual_tyre_compound"`
	VisualTyreCompound      uint8   `json:"visual_tyre_compound"`
	TyresAgeLaps            uint8   `json:"tyres_age_laps"`
	VehicleFiaFlags         int8    `json:"vehicle_fia_flags"`
	EnginePowerICE          float32 `json:"engine_power_ice"`
	EnginePowerMGUK         float32 `json:"engine_power_mgu_k"`
	ErsStoreEnergy          float32 `json:"ers_store_energy"`
	ErsDeployMode           uint8   `json:"ers_deploy_mode"`
	ErsHarvestedThisLapMGUK float32 `json:"ers_harvested_this_lap_mgu_k"`
	ErsHarvestedThisLapMGUH float32 `json:"ers_harvested_this_lap_mgu_h"`
	ErsDeployedThisLap      float32 `json:"ers_deployed_this_lap"`
	NetworkPaused           uint8   `json:"network_paused"`
}

type PacketCarStatusData struct {
	Header        PacketHeader      `json:"header"`
	CarStatusData [22]CarStatusData `json:"car_status_data"`
}

func ParsePacketCarStatusData(buf []byte, header PacketHeader) PacketCarStatusData {
	p := PacketCarStatusData{Header: header}
	if len(buf) < 1239 {
		return p
	}
	offset := 29
	for i := 0; i < 22; i++ {
		p.CarStatusData[i] = CarStatusData{
			TractionControl:         buf[offset],
			AntiLockBrakes:          buf[offset+1],
			FuelMix:                 buf[offset+2],
			FrontBrakeBias:          buf[offset+3],
			PitLimiterStatus:        buf[offset+4],
			FuelInTank:              math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+5 : offset+9])),
			FuelCapacity:            math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+9 : offset+13])),
			FuelRemainingLaps:       math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+13 : offset+17])),
			MaxRPM:                  binary.LittleEndian.Uint16(buf[offset+17 : offset+19]),
			IdleRPM:                 binary.LittleEndian.Uint16(buf[offset+19 : offset+21]),
			MaxGears:                buf[offset+21],
			DrsAllowed:              buf[offset+22],
			DrsActivationDistance:   binary.LittleEndian.Uint16(buf[offset+23 : offset+25]),
			ActualTyreCompound:      buf[offset+25],
			VisualTyreCompound:      buf[offset+26],
			TyresAgeLaps:            buf[offset+27],
			VehicleFiaFlags:         int8(buf[offset+28]),
			EnginePowerICE:          math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+29 : offset+33])),
			EnginePowerMGUK:         math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+33 : offset+37])),
			ErsStoreEnergy:          math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+37 : offset+41])),
			ErsDeployMode:           buf[offset+41],
			ErsHarvestedThisLapMGUK: math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+42 : offset+46])),
			ErsHarvestedThisLapMGUH: math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+46 : offset+50])),
			ErsDeployedThisLap:      math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+50 : offset+54])),
			NetworkPaused:           buf[offset+54],
		}
		offset += 55
	}
	return p
}

// --- EVENT DATA (Packet ID 3) ---

type PacketEventData struct {
	Header            PacketHeader `json:"header"`
	EventCode         string       `json:"event_code"`
	VehicleIdx        uint8        `json:"vehicle_idx"`
	LapTime           float32      `json:"lap_time"`
	PenaltyType       uint8        `json:"penalty_type"`
	InfringementType  uint8        `json:"infringement_type"`
	PenaltyTime       uint8        `json:"penalty_time"`
	PlacesGained      uint8        `json:"places_gained"`
	Speed             float32      `json:"speed"`
	NumLights         uint8        `json:"num_lights"`
	FlashbackFrame    uint32       `json:"flashback_frame"`
	FlashbackTime     float32      `json:"flashback_time"`
	OvertakingIdx     uint8        `json:"overtaking_idx"`
	BeingOvertakenIdx uint8        `json:"being_overtaken_idx"`
	SafetyCarType     uint8        `json:"safety_car_type"`
	SafetyCarEvent    uint8        `json:"safety_car_event"`
	Vehicle1Idx       uint8        `json:"vehicle_1_idx"`
	Vehicle2Idx       uint8        `json:"vehicle_2_idx"`
}

func ParsePacketEventData(buf []byte, header PacketHeader) PacketEventData {
	p := PacketEventData{Header: header}
	if len(buf) < 33 {
		return p
	}
	p.EventCode = string(buf[29:33])
	offset := 33
	
	switch p.EventCode {
	case "FTLP":
		if len(buf) >= 38 {
			p.VehicleIdx = buf[offset]
			p.LapTime = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+1 : offset+5]))
		}
	case "RTMT", "TMPT", "RCWN", "DTSV", "SGSV":
		if len(buf) >= 34 {
			p.VehicleIdx = buf[offset]
		}
	case "PENA":
		if len(buf) >= 40 {
			p.PenaltyType = buf[offset]
			p.InfringementType = buf[offset+1]
			p.VehicleIdx = buf[offset+2]
			p.Vehicle2Idx = buf[offset+3] // Other vehicle
			p.PenaltyTime = buf[offset+4]
			p.PlacesGained = buf[offset+6]
		}
	case "SPTP":
		if len(buf) >= 38 {
			p.VehicleIdx = buf[offset]
			p.Speed = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+1 : offset+5]))
		}
	case "STLG":
		if len(buf) >= 34 {
			p.NumLights = buf[offset]
		}
	case "FLBK":
		if len(buf) >= 41 {
			p.FlashbackFrame = binary.LittleEndian.Uint32(buf[offset : offset+4])
			p.FlashbackTime = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+4 : offset+8]))
		}
	case "OVTK":
		if len(buf) >= 35 {
			p.OvertakingIdx = buf[offset]
			p.BeingOvertakenIdx = buf[offset+1]
		}
	case "SCAR":
		if len(buf) >= 35 {
			p.SafetyCarType = buf[offset]
			p.SafetyCarEvent = buf[offset+1]
		}
	case "COLL":
		if len(buf) >= 35 {
			p.Vehicle1Idx = buf[offset]
			p.Vehicle2Idx = buf[offset+1]
		}
	}
	return p
}

// --- CAR SETUPS DATA (Packet ID 5) ---

type CarSetupData struct {
	FrontWing              uint8   `json:"front_wing"`
	RearWing               uint8   `json:"rear_wing"`
	OnThrottle             uint8   `json:"on_throttle"`
	OffThrottle            uint8   `json:"off_throttle"`
	FrontCamber            float32 `json:"front_camber"`
	RearCamber             float32 `json:"rear_camber"`
	FrontToe               float32 `json:"front_toe"`
	RearToe                float32 `json:"rear_toe"`
	FrontSuspension        uint8   `json:"front_suspension"`
	RearSuspension         uint8   `json:"rear_suspension"`
	FrontAntiRollBar       uint8   `json:"front_anti_roll_bar"`
	RearAntiRollBar        uint8   `json:"rear_anti_roll_bar"`
	FrontSuspensionHeight  uint8   `json:"front_suspension_height"`
	RearSuspensionHeight   uint8   `json:"rear_suspension_height"`
	BrakePressure          uint8   `json:"brake_pressure"`
	BrakeBias              uint8   `json:"brake_bias"`
	EngineBraking          uint8   `json:"engine_braking"`
	RearLeftTyrePressure   float32 `json:"rear_left_tyre_pressure"`
	RearRightTyrePressure  float32 `json:"rear_right_tyre_pressure"`
	FrontLeftTyrePressure  float32 `json:"front_left_tyre_pressure"`
	FrontRightTyrePressure float32 `json:"front_right_tyre_pressure"`
	Ballast                uint8   `json:"ballast"`
	FuelLoad               float32 `json:"fuel_load"`
}

type PacketCarSetupData struct {
	Header             PacketHeader     `json:"header"`
	CarSetups          [22]CarSetupData `json:"car_setups"`
	NextFrontWingValue float32          `json:"next_front_wing_value"`
}

func ParsePacketCarSetupData(buf []byte, header PacketHeader) PacketCarSetupData {
	p := PacketCarSetupData{Header: header}
	if len(buf) < 1133 {
		return p
	}
	offset := 29
	for i := 0; i < 22; i++ {
		p.CarSetups[i] = CarSetupData{
			FrontWing:              buf[offset],
			RearWing:               buf[offset+1],
			OnThrottle:             buf[offset+2],
			OffThrottle:            buf[offset+3],
			FrontCamber:            math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+4 : offset+8])),
			RearCamber:             math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+8 : offset+12])),
			FrontToe:               math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+12 : offset+16])),
			RearToe:                math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+16 : offset+20])),
			FrontSuspension:        buf[offset+20],
			RearSuspension:         buf[offset+21],
			FrontAntiRollBar:       buf[offset+22],
			RearAntiRollBar:        buf[offset+23],
			FrontSuspensionHeight:  buf[offset+24],
			RearSuspensionHeight:   buf[offset+25],
			BrakePressure:          buf[offset+26],
			BrakeBias:              buf[offset+27],
			EngineBraking:          buf[offset+28],
			RearLeftTyrePressure:   math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+29 : offset+33])),
			RearRightTyrePressure:  math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+33 : offset+37])),
			FrontLeftTyrePressure:  math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+37 : offset+41])),
			FrontRightTyrePressure: math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+41 : offset+45])),
			Ballast:                buf[offset+45],
			FuelLoad:               math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+46 : offset+50])),
		}
		offset += 50
	}
	p.NextFrontWingValue = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	return p
}

// --- FINAL CLASSIFICATION DATA (Packet ID 8) ---

type FinalClassificationData struct {
	Position          uint8    `json:"position"`
	NumLaps           uint8    `json:"num_laps"`
	GridPosition      uint8    `json:"grid_position"`
	Points            uint8    `json:"points"`
	NumPitStops       uint8    `json:"num_pit_stops"`
	ResultStatus      uint8    `json:"result_status"`
	BestLapTimeInMS   uint32   `json:"best_lap_time_in_ms"`
	TotalRaceTime     float64  `json:"total_race_time"`
	PenaltiesTime     uint8    `json:"penalties_time"`
	NumPenalties      uint8    `json:"num_penalties"`
	NumTyreStints     uint8    `json:"num_tyre_stints"`
	TyreStintsActual  [8]uint8 `json:"tyre_stints_actual"`
	TyreStintsVisual  [8]uint8 `json:"tyre_stints_visual"`
	TyreStintsEndLaps [8]uint8 `json:"tyre_stints_end_laps"`
}

type PacketFinalClassificationData struct {
	Header             PacketHeader                `json:"header"`
	NumCars            uint8                       `json:"num_cars"`
	ClassificationData [22]FinalClassificationData `json:"classification_data"`
}

func ParsePacketFinalClassificationData(buf []byte, header PacketHeader) PacketFinalClassificationData {
	p := PacketFinalClassificationData{Header: header}
	if len(buf) < 1020 {
		return p
	}
	p.NumCars = buf[29]
	offset := 30
	for i := 0; i < 22; i++ {
		p.ClassificationData[i] = FinalClassificationData{
			Position:        buf[offset],
			NumLaps:         buf[offset+1],
			GridPosition:    buf[offset+2],
			Points:          buf[offset+3],
			NumPitStops:     buf[offset+4],
			ResultStatus:    buf[offset+5],
			BestLapTimeInMS: binary.LittleEndian.Uint32(buf[offset+6 : offset+10]),
			TotalRaceTime:   math.Float64frombits(binary.LittleEndian.Uint64(buf[offset+10 : offset+18])),
			PenaltiesTime:   buf[offset+18],
			NumPenalties:    buf[offset+19],
			NumTyreStints:   buf[offset+20],
		}
		for j := 0; j < 8; j++ {
			p.ClassificationData[i].TyreStintsActual[j] = buf[offset+21+j]
			p.ClassificationData[i].TyreStintsVisual[j] = buf[offset+29+j]
			p.ClassificationData[i].TyreStintsEndLaps[j] = buf[offset+37+j]
		}
		offset += 45
	}
	return p
}

// --- CAR DAMAGE DATA (Packet ID 10) ---

type CarDamageData struct {
	TyresWear            [4]float32 `json:"tyres_wear"`
	TyresDamage          [4]uint8   `json:"tyres_damage"`
	BrakesDamage         [4]uint8   `json:"brakes_damage"`
	FrontLeftWingDamage  uint8      `json:"front_left_wing_damage"`
	FrontRightWingDamage uint8      `json:"front_right_wing_damage"`
	RearWingDamage       uint8      `json:"rear_wing_damage"`
	FloorDamage          uint8      `json:"floor_damage"`
	DiffuserDamage       uint8      `json:"diffuser_damage"`
	SidepodDamage        uint8      `json:"sidepod_damage"`
	DrsFault             uint8      `json:"drs_fault"`
	ErsFault             uint8      `json:"ers_fault"`
	GearBoxDamage        uint8      `json:"gear_box_damage"`
	EngineDamage         uint8      `json:"engine_damage"`
	EngineMGUHWear       uint8      `json:"engine_mguh_wear"`
	EngineESWear         uint8      `json:"engine_es_wear"`
	EngineCEWear         uint8      `json:"engine_ce_wear"`
	EngineICEWear        uint8      `json:"engine_ice_wear"`
	EngineMGUKWear       uint8      `json:"engine_mguk_wear"`
	EngineTCWear         uint8      `json:"engine_tc_wear"`
	EngineBlown          uint8      `json:"engine_blown"`
	EngineSeized         uint8      `json:"engine_seized"`
}

type PacketCarDamageData struct {
	Header        PacketHeader      `json:"header"`
	CarDamageData [22]CarDamageData `json:"car_damage_data"`
}

func ParsePacketCarDamageData(buf []byte, header PacketHeader) PacketCarDamageData {
	p := PacketCarDamageData{Header: header}
	if len(buf) < 953 {
		return p
	}
	offset := 29
	for i := 0; i < 22; i++ {
		p.CarDamageData[i] = CarDamageData{
			TyresWear: [4]float32{
				math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4])),
				math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+4 : offset+8])),
				math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+8 : offset+12])),
				math.Float32frombits(binary.LittleEndian.Uint32(buf[offset+12 : offset+16])),
			},
			TyresDamage: [4]uint8{
				buf[offset+16], buf[offset+17], buf[offset+18], buf[offset+19],
			},
			BrakesDamage: [4]uint8{
				buf[offset+20], buf[offset+21], buf[offset+22], buf[offset+23],
			},
			FrontLeftWingDamage:  buf[offset+24],
			FrontRightWingDamage: buf[offset+25],
			RearWingDamage:       buf[offset+26],
			FloorDamage:          buf[offset+27],
			DiffuserDamage:       buf[offset+28],
			SidepodDamage:        buf[offset+29],
			DrsFault:             buf[offset+30],
			ErsFault:             buf[offset+31],
			GearBoxDamage:        buf[offset+32],
			EngineDamage:         buf[offset+33],
			EngineMGUHWear:       buf[offset+34],
			EngineESWear:         buf[offset+35],
			EngineCEWear:         buf[offset+36],
			EngineICEWear:        buf[offset+37],
			EngineMGUKWear:       buf[offset+38],
			EngineTCWear:         buf[offset+39],
			EngineBlown:          buf[offset+40],
			EngineSeized:         buf[offset+41],
		}
		offset += 42
	}
	return p
}

// --- SESSION HISTORY DATA (Packet ID 11) ---

type LapHistoryData struct {
	LapTimeInMS            uint32 `json:"lap_time_in_ms"`
	Sector1TimeMSPart      uint16 `json:"sector_1_time_ms_part"`
	Sector1TimeMinutesPart uint8  `json:"sector_1_time_minutes_part"`
	Sector2TimeMSPart      uint16 `json:"sector_2_time_ms_part"`
	Sector2TimeMinutesPart uint8  `json:"sector_2_time_minutes_part"`
	Sector3TimeMSPart      uint16 `json:"sector_3_time_ms_part"`
	Sector3TimeMinutesPart uint8  `json:"sector_3_time_minutes_part"`
	LapValidBitFlags       uint8  `json:"lap_valid_bit_flags"`
}

type TyreStintHistoryData struct {
	EndLap             uint8 `json:"end_lap"`
	TyreActualCompound uint8 `json:"tyre_actual_compound"`
	TyreVisualCompound uint8 `json:"tyre_visual_compound"`
}

type PacketSessionHistoryData struct {
	Header             PacketHeader            `json:"header"`
	CarIdx             uint8                   `json:"car_idx"`
	NumLaps            uint8                   `json:"num_laps"`
	NumTyreStints      uint8                   `json:"num_tyre_stints"`
	BestLapTimeLapNum  uint8                   `json:"best_lap_time_lap_num"`
	BestSector1LapNum  uint8                   `json:"best_sector_1_lap_num"`
	BestSector2LapNum  uint8                   `json:"best_sector_2_lap_num"`
	BestSector3LapNum  uint8                   `json:"best_sector_3_lap_num"`
	LapHistoryData     [100]LapHistoryData     `json:"lap_history_data"`
	TyreStintsHistory  [8]TyreStintHistoryData `json:"tyre_stints_history_data"`
}

func ParsePacketSessionHistoryData(buf []byte, header PacketHeader) PacketSessionHistoryData {
	p := PacketSessionHistoryData{Header: header}
	if len(buf) < 1460 {
		return p
	}
	p.CarIdx = buf[29]
	p.NumLaps = buf[30]
	p.NumTyreStints = buf[31]
	p.BestLapTimeLapNum = buf[32]
	p.BestSector1LapNum = buf[33]
	p.BestSector2LapNum = buf[34]
	p.BestSector3LapNum = buf[35]

	offset := 36
	for i := 0; i < 100; i++ {
		p.LapHistoryData[i] = LapHistoryData{
			LapTimeInMS:            binary.LittleEndian.Uint32(buf[offset : offset+4]),
			Sector1TimeMSPart:      binary.LittleEndian.Uint16(buf[offset+4 : offset+6]),
			Sector1TimeMinutesPart: buf[offset+6],
			Sector2TimeMSPart:      binary.LittleEndian.Uint16(buf[offset+7 : offset+9]),
			Sector2TimeMinutesPart: buf[offset+9],
			Sector3TimeMSPart:      binary.LittleEndian.Uint16(buf[offset+10 : offset+12]),
			Sector3TimeMinutesPart: buf[offset+12],
			LapValidBitFlags:       buf[offset+13],
		}
		offset += 14
	}
	for i := 0; i < 8; i++ {
		p.TyreStintsHistory[i] = TyreStintHistoryData{
			EndLap:             buf[offset],
			TyreActualCompound: buf[offset+1],
			TyreVisualCompound: buf[offset+2],
		}
		offset += 3
	}
	return p
}

// --- TYRE SETS DATA (Packet ID 12) ---

type TyreSetData struct {
	ActualTyreCompound uint8  `json:"actual_tyre_compound"`
	VisualTyreCompound uint8  `json:"visual_tyre_compound"`
	Wear               uint8  `json:"wear"`
	Available          uint8  `json:"available"`
	RecommendedSession uint8  `json:"recommended_session"`
	LifeSpan           uint8  `json:"life_span"`
	UsableLife         uint8  `json:"usable_life"`
	LapDeltaTime       int16  `json:"lap_delta_time"`
	Fitted             uint8  `json:"fitted"`
}

type PacketTyreSetsData struct {
	Header      PacketHeader    `json:"header"`
	CarIdx      uint8           `json:"car_idx"`
	TyreSetData [20]TyreSetData `json:"tyre_set_data"`
	FittedIdx   uint8           `json:"fitted_idx"`
}

func ParsePacketTyreSetsData(buf []byte, header PacketHeader) PacketTyreSetsData {
	p := PacketTyreSetsData{Header: header}
	if len(buf) < 231 {
		return p
	}
	p.CarIdx = buf[29]
	offset := 30
	for i := 0; i < 20; i++ {
		p.TyreSetData[i] = TyreSetData{
			ActualTyreCompound: buf[offset],
			VisualTyreCompound: buf[offset+1],
			Wear:               buf[offset+2],
			Available:          buf[offset+3],
			RecommendedSession: buf[offset+4],
			LifeSpan:           buf[offset+5],
			UsableLife:         buf[offset+6],
			LapDeltaTime:       int16(binary.LittleEndian.Uint16(buf[offset+7 : offset+9])),
			Fitted:             buf[offset+9],
		}
		offset += 10
	}
	p.FittedIdx = buf[offset]
	return p
}

// --- MOTION EX DATA (Packet ID 13) ---

type PacketMotionExData struct {
	Header                 PacketHeader `json:"header"`
	SuspensionPosition     [4]float32   `json:"suspension_position"`
	SuspensionVelocity     [4]float32   `json:"suspension_velocity"`
	SuspensionAcceleration [4]float32   `json:"suspension_acceleration"`
	WheelSpeed             [4]float32   `json:"wheel_speed"`
	WheelSlipRatio         [4]float32   `json:"wheel_slip_ratio"`
	WheelSlipAngle         [4]float32   `json:"wheel_slip_angle"`
	WheelLatForce          [4]float32   `json:"wheel_lat_force"`
	WheelLongForce         [4]float32   `json:"wheel_long_force"`
	HeightOfCOGAboveGround float32      `json:"height_of_cog_above_ground"`
	LocalVelocityX         float32      `json:"local_velocity_x"`
	LocalVelocityY         float32      `json:"local_velocity_y"`
	LocalVelocityZ         float32      `json:"local_velocity_z"`
	AngularVelocityX       float32      `json:"angular_velocity_x"`
	AngularVelocityY       float32      `json:"angular_velocity_y"`
	AngularVelocityZ       float32      `json:"angular_velocity_z"`
	AngularAccelerationX   float32      `json:"angular_acceleration_x"`
	AngularAccelerationY   float32      `json:"angular_acceleration_y"`
	AngularAccelerationZ   float32      `json:"angular_acceleration_z"`
	FrontWheelsAngle       float32      `json:"front_wheels_angle"`
	WheelVertForce         [4]float32   `json:"wheel_vert_force"`
	FrontAeroHeight        float32      `json:"front_aero_height"`
	RearAeroHeight         float32      `json:"rear_aero_height"`
	FrontRollAngle         float32      `json:"front_roll_angle"`
	RearRollAngle          float32      `json:"rear_roll_angle"`
	ChassisYaw             float32      `json:"chassis_yaw"`
}

func ParsePacketMotionExData(buf []byte, header PacketHeader) PacketMotionExData {
	p := PacketMotionExData{Header: header}
	if len(buf) < 237 {
		return p
	}
	offset := 29

	readFloatArray := func(arr *[4]float32) {
		for i := 0; i < 4; i++ {
			arr[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
			offset += 4
		}
	}

	readFloatArray(&p.SuspensionPosition)
	readFloatArray(&p.SuspensionVelocity)
	readFloatArray(&p.SuspensionAcceleration)
	readFloatArray(&p.WheelSpeed)
	readFloatArray(&p.WheelSlipRatio)
	readFloatArray(&p.WheelSlipAngle)
	readFloatArray(&p.WheelLatForce)
	readFloatArray(&p.WheelLongForce)

	p.HeightOfCOGAboveGround = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	p.LocalVelocityX = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	p.LocalVelocityY = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	p.LocalVelocityZ = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	p.AngularVelocityX = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	p.AngularVelocityY = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	p.AngularVelocityZ = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	p.AngularAccelerationX = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	p.AngularAccelerationY = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	p.AngularAccelerationZ = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	p.FrontWheelsAngle = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4

	readFloatArray(&p.WheelVertForce)

	p.FrontAeroHeight = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	p.RearAeroHeight = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	p.FrontRollAngle = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	p.RearRollAngle = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	p.ChassisYaw = math.Float32frombits(binary.LittleEndian.Uint32(buf[offset : offset+4]))

	return p
}
