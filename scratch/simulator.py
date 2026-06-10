# e:\F1 Telemetry\scratch\simulator.py
import socket
import time
import struct
import random
import math

UDP_IP = "127.0.0.1"
UDP_PORT = 20777
SESSION_UID = 8847192047192074

print(f"Starting F1 Telemetry Simulator... Sending packets to {UDP_IP}:{UDP_PORT}")
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Constants
TRACK_ID = 16 # Interlagos (Brazil)
SESSION_TYPE = 15 # Race
TEAM_ID = 1 # Ferrari
DRIVER_NAME = "Luiz F."
TRACK_LENGTH = 4309 # meters

# Simulation State
frame_id = 0
session_time = 0.0
lap_distance = 0.0
total_distance = 0.0
current_lap = 1
speed = 0.0
rpm = 1000
gear = 1
throttle = 0.0
brake = 0.0
fuel_in_tank = 45.5
last_lap_time_ms = 0
sector1_ms = 0
sector2_ms = 0

sector1_trigger = 1400 # meters
sector2_trigger = 3000 # meters

def pack_header(packet_id):
    # struct PacketHeader: format <HBBBBQfIIBB (29 bytes)
    return struct.pack(
        "<HBBBBBQfIIBB",
        2024,              # m_packetFormat
        24,                # m_gameYear
        1,                 # m_gameMajorVersion
        0,                 # m_gameMinorVersion
        1,                 # m_packetVersion
        packet_id,         # m_packetId
        SESSION_UID,       # m_sessionUID
        session_time,      # m_sessionTime
        frame_id,          # m_frameIdentifier
        frame_id,          # m_overallFrameIdentifier
        0,                 # m_playerCarIndex
        255                # m_secondaryPlayerCarIndex
    )

def send_motion_packet():
    header = pack_header(0)
    
    # 22 cars data
    cars_data = b""
    for i in range(22):
        if i == 0:
            # Player car physics
            rad = (lap_distance / TRACK_LENGTH) * 2 * math.pi
            x = 500.0 * math.cos(rad)
            y = 0.0
            z = 500.0 * math.sin(rad)
            vx = -speed * math.sin(rad) / 3.6
            vy = 0.0
            vz = speed * math.cos(rad) / 3.6
            cars_data += struct.pack(
                "<ffffffhhhhhhffffff",
                x, y, z, vx, vy, vz,
                0, 0, 0, 0, 0, 0, # forward/right normals
                0.0, 0.0, 0.0,    # G-forces
                rad, 0.0, 0.0     # yaw, pitch, roll
            )
        else:
            # Other cars static
            cars_data += struct.pack("<ffffffhhhhhhffffff", 0,0,0,0,0,0, 0,0,0,0,0,0, 0,0,0, 0,0,0)
            
    sock.sendto(header + cars_data, (UDP_IP, UDP_PORT))

def send_session_packet():
    header = pack_header(1)
    
    # 47 bytes processed, rest padded to 753 bytes
    payload = struct.pack(
        "<BBBBHBBBHHBBBBB",
        0,                 # m_weather
        32,                # m_trackTemperature
        25,                # m_airTemperature
        71,                # m_totalLaps
        TRACK_LENGTH,      # m_trackLength
        SESSION_TYPE,      # m_sessionType
        TRACK_ID,          # m_trackId
        0,                 # m_formula
        1800,              # m_sessionTimeLeft
        3600,              # m_sessionDuration
        80,                # m_pitSpeedLimit
        0,                 # m_gamePaused
        0,                 # m_isSpectating
        255,               # m_spectatorCarIndex
        0                  # m_sliProNativeSupport
    )
    # Pad to 753 - 29 (header) - 18 (payload) = 706 bytes
    padding = b"\x00" * 706
    sock.sendto(header + payload + padding, (UDP_IP, UDP_PORT))

def send_lap_data_packet():
    header = pack_header(2)
    
    cars_data = b""
    for i in range(22):
        if i == 0:
            # Sector times min and ms calculation
            s1_min = max(0, int(sector1_ms // 60000))
            s1_ms = max(0, int(sector1_ms % 60000))
            s2_min = max(0, int(sector2_ms // 60000))
            s2_ms = max(0, int(sector2_ms % 60000))
            
            # Pack LapData: Format <IIHBHBHBHBfffBBBBBBBBBBBBBBBHHBfB
            cars_data += struct.pack(
                "<IIHBHBHBHBfffBBBBBBBBBBBBBBBHHBfB",
                last_lap_time_ms,               # last lap time ms
                int(session_time * 1000) % 90000, # current lap time ms
                s1_ms, s1_min,                  # S1
                s2_ms, s2_min,                  # S2
                0, 0,                           # delta to car front
                0, 0,                           # delta to leader
                lap_distance,
                total_distance,
                0.0,                            # safety car delta
                1,                              # position
                current_lap,
                0,                              # pit status
                0,                              # num pit stops
                0 if lap_distance < sector1_trigger else (1 if lap_distance < sector2_trigger else 2), # sector
                0,                              # lap invalid
                0,                              # penalties
                0,                              # warnings
                0,                              # corner warnings
                0, 0,                           # unserved pens
                1,                              # grid pos
                4,                              # driver status (on track)
                2,                              # result status (active)
                0, 0, 0, 0,                     # pit details
                315.5,                          # speed trap fastest
                0                               # speed trap lap
            )
        else:
            cars_data += b"\x00" * 57
            
    # Pad to 1285: 29 + 22*57 + 2 bytes padding/rival car indices
    padding = b"\xff\xff"
    sock.sendto(header + cars_data + padding, (UDP_IP, UDP_PORT))

def send_participants_packet():
    header = pack_header(4)
    num_active_cars = 20
    
    payload = struct.pack("<B", num_active_cars)
    
    # 22 cars data
    cars_data = b""
    for i in range(22):
        if i == 0:
            name_bytes = DRIVER_NAME.encode('utf-8').ljust(48, b'\x00')
            cars_data += struct.pack(
                "<BBBBBBB48sBBHB",
                0,                 # Human controlled
                9,                 # Driver ID (Max Verstappen or customized)
                0,                 # Network ID
                TEAM_ID,           # Team ID
                0,                 # MyTeam
                33,                # RaceNumber
                9,                 # Nationality (Brazilian)
                name_bytes,
                1,                 # telemetry public
                1,                 # show online names
                500,               # tech level
                3                  # Playstation platform
            )
        else:
            name_bytes = f"AI Driver {i}".encode('utf-8').ljust(48, b'\x00')
            cars_data += struct.pack(
                "<BBBBBBB48sBBHB",
                1, i, 0, 8, 0, i+10, 10, name_bytes, 0, 0, 0, 255
            )
            
    sock.sendto(header + payload + cars_data, (UDP_IP, UDP_PORT))

def send_car_telemetry_packet():
    header = pack_header(6)
    
    cars_data = b""
    for i in range(22):
        if i == 0:
            # Build tyre temps
            tyre_surface_temps = [int(85 + random.uniform(-2, 2)) for _ in range(4)]
            tyre_inner_temps = [int(95 + random.uniform(-1, 1)) for _ in range(4)]
            tyre_pressures = [22.5 + random.uniform(-0.2, 0.2) for _ in range(4)]
            
            # Format: <HfffBbHBBHHHHHBBBBBBBBHffffBBBB
            cars_data += struct.pack(
                "<HfffBbHBBHHHHHBBBBBBBBHffffBBBB",
                int(speed),                     # speed
                throttle,                       # throttle
                0.0,                            # steer
                brake,                          # brake
                0,                              # clutch
                gear,                           # gear
                int(rpm),                       # engine RPM
                1 if speed > 250 else 0,         # DRS active
                int((rpm / 15000) * 100),       # rev lights percent
                0,                              # rev lights bit value
                180, 180, 180, 180,             # brake temps
                *tyre_surface_temps,            # tyre surface temps
                *tyre_inner_temps,              # tyre inner temps
                98,                             # engine temp
                *tyre_pressures,                # tyre pressures
                0, 0, 0, 0                      # surface type (tarmac)
            )
        else:
            cars_data += b"\x00" * 60
            
    # Pad to 1352: 29 + 22*60 + 3 bytes padding
    padding = b"\x00\x00\x00"
    sock.sendto(header + cars_data + padding, (UDP_IP, UDP_PORT))

def send_car_status_packet():
    header = pack_header(7)
    
    cars_data = b""
    for i in range(22):
        if i == 0:
            # Format: <BBBBBfffHHBBHBBBBfffBfffB
            cars_data += struct.pack(
                "<BBBBBfffHHBBHBBBBfffBfffB",
                1, 1, 1, 55, 0,                 # driver assists
                fuel_in_tank,
                50.0,                           # fuel capacity
                fuel_in_tank / 1.5,             # fuel laps remaining
                15000, 1000,                    # max/idle RPM
                8,                              # max gears
                1, 0,                           # DRS allowed
                18, 18, 1, 0,                   # tyres details
                980000.0, 120000.0, 4000000.0,  # ICE, MGUK power and ERS store energy
                2,                              # ERS deploy mode (hotlap)
                25000.0, 5000.0, 30000.0,       # ERS stats
                0                               # network paused
            )
        else:
            cars_data += b"\x00" * 55
            
    sock.sendto(header + cars_data, (UDP_IP, UDP_PORT))

def send_event_packet():
    header = pack_header(3)
    event_code = b"FTLP"
    payload = struct.pack("<Bf", 0, 84.321)
    sock.sendto(header + event_code + payload, (UDP_IP, UDP_PORT))

def send_car_setup_packet():
    header = pack_header(5)
    cars_data = b""
    for i in range(22):
        cars_data += struct.pack(
            "<BBBBffffBBBBBBBBBffffBf",
            2, 2, 80, 60,
            -3.0, -2.0, 0.05, 0.20,
            3, 3, 7, 7, 3, 3, 100, 50, 4,
            22.0, 22.0, 21.5, 21.5,
            0, fuel_in_tank
        )
    next_fw = struct.pack("<f", 2.0)
    sock.sendto(header + cars_data + next_fw, (UDP_IP, UDP_PORT))

def send_final_classification_packet():
    header = pack_header(8)
    num_cars = 20
    payload = struct.pack("<B", num_cars)
    cars_data = b""
    for i in range(22):
        tyre_actual = b"\x10" * 8
        tyre_visual = b"\x10" * 8
        tyre_end = b"\x00" * 8
        cars_data += struct.pack(
            "<BBBBBBIdBBB",
            i+1, 15, i+1, 25 if i==0 else 0, 1, 3,
            75000, 1200.0,
            0, 0, 1
        ) + tyre_actual + tyre_visual + tyre_end
    sock.sendto(header + payload + cars_data, (UDP_IP, UDP_PORT))

def send_car_damage_packet():
    header = pack_header(10)
    cars_data = b""
    for i in range(22):
        cars_data += struct.pack(
            "<ffffBBBBBBBBBBBBBBBBBBBBBBBBBB",
            0.05, 0.05, 0.05, 0.05,
            0,0,0,0,
            0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
        )
    sock.sendto(header + cars_data, (UDP_IP, UDP_PORT))

def send_session_history_packet():
    header = pack_header(11)
    payload = struct.pack("<BBBBBBB", 0, 10, 1, 5, 5, 5, 5)
    laps_data = b""
    for i in range(100):
        lap_time_ms = 75000 if i < 10 else 0
        s1_ms = 25000 if i < 10 else 0
        s1_min = 0
        s2_ms = 25000 if i < 10 else 0
        s2_min = 0
        s3_ms = 25000 if i < 10 else 0
        s3_min = 0
        valid = 1 if i < 10 else 0
        laps_data += struct.pack("<IHB H B H B B", lap_time_ms, s1_ms, s1_min, s2_ms, s2_min, s3_ms, s3_min, valid)
    tyres_data = b""
    for i in range(8):
        tyres_data += struct.pack("<BBB", 10 if i==0 else 0, 16 if i==0 else 0, 16 if i==0 else 0)
    sock.sendto(header + payload + laps_data + tyres_data, (UDP_IP, UDP_PORT))

def send_tyre_sets_packet():
    header = pack_header(12)
    payload = struct.pack("<B", 0)
    sets_data = b""
    for i in range(20):
        sets_data += struct.pack(
            "<BBBBBBBhB",
            16 if i==0 else 0, 16 if i==0 else 0, 0, 1 if i < 5 else 0, 0, 0, 50,
            0,
            1 if i==0 else 0
        )
    fitted_idx = struct.pack("<B", 0)
    sock.sendto(header + payload + sets_data + fitted_idx, (UDP_IP, UDP_PORT))

def send_motion_ex_packet():
    header = pack_header(13)
    payload = b"\x00" * 208
    sock.sendto(header + payload, (UDP_IP, UDP_PORT))


# Main simulation loop at 60Hz
try:
    last_session_send = 0.0
    last_participants_send = 0.0
    
    acceleration_phase = True
    
    while True:
        loop_start = time.time()
        
        # 1. Update session time and frame ID
        frame_id += 1
        session_time += 1.0 / 60.0
        
        # 2. Physics & Engine Telemetry Simulation
        if acceleration_phase:
            throttle = 1.0
            brake = 0.0
            # Speed increases
            speed += 1.8 # rate of accel
            rpm = 5000 + (speed * 30) % 8500
            
            # Automatic gear shifting logic
            if speed < 40: gear = 1
            elif speed < 80: gear = 2
            elif speed < 120: gear = 3
            elif speed < 165: gear = 4
            elif speed < 210: gear = 5
            elif speed < 250: gear = 6
            elif speed < 290: gear = 7
            else:
                gear = 8
                # Top speed limit
                speed = min(speed, 325.0)
                rpm = 11500 + random.uniform(-100, 100)
                
            # Switch to braking after reaching close to top speed/distance
            if lap_distance > 1300 and lap_distance < 1600:
                acceleration_phase = False
            elif lap_distance > 2800 and lap_distance < 3100:
                acceleration_phase = False
        else:
            throttle = 0.0
            brake = 0.8
            speed -= 3.5 # heavy braking
            if speed < 60.0:
                speed = 60.0
                acceleration_phase = True
                
            rpm = 3000 + (speed * 40)
            if speed > 240: gear = 7
            elif speed > 190: gear = 6
            elif speed > 150: gear = 5
            elif speed > 110: gear = 4
            elif speed > 80: gear = 3
            else:
                gear = 2
        
        # Update distances
        distance_delta = (speed / 3.6) / 60.0 # meters per frame
        lap_distance += distance_delta
        total_distance += distance_delta
        
        # Sector splits tracking
        if lap_distance >= sector1_trigger and sector1_ms == 0:
            sector1_ms = int(session_time * 1000) % 35000
        if lap_distance >= sector2_trigger and sector2_ms == 0:
            raw_s2 = (int(session_time * 1000) % 75000) - sector1_ms
            if raw_s2 < 0:
                raw_s2 = 30000 # Positive fallback
            sector2_ms = raw_s2
            
        # Fuel consumption
        fuel_in_tank -= 0.0002 # burn rate
        
        # Lap Completion reset
        if lap_distance >= TRACK_LENGTH:
            last_lap_time_ms = int(session_time * 1000) % 85000
            print(f"Lap {current_lap} Completed! Time: {last_lap_time_ms} ms")
            current_lap += 1
            lap_distance = 0.0
            sector1_ms = 0
            sector2_ms = 0
            acceleration_phase = True
            
        # 3. Network Sends
        send_motion_packet()
        send_lap_data_packet()
        send_car_telemetry_packet()
        send_car_status_packet()
        
        # 2 per second session data sends
        if session_time - last_session_send >= 0.5:
            send_session_packet()
            last_session_send = session_time
            
        # Every 5 seconds participant data sends
        if session_time - last_participants_send >= 5.0:
            send_participants_packet()
            last_participants_send = session_time

        # Every 2 seconds (120 frames), send the rest of the new packet types
        if frame_id % 120 == 0:
            send_event_packet()
            send_car_setup_packet()
            send_final_classification_packet()
            send_car_damage_packet()
            send_session_history_packet()
            send_tyre_sets_packet()
            send_motion_ex_packet()
            
        # Sleep to regulate loop to exactly 60Hz
        elapsed = time.time() - loop_start
        sleep_time = (1.0 / 60.0) - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)
            
except KeyboardInterrupt:
    print("\nSimulator stopped.")
