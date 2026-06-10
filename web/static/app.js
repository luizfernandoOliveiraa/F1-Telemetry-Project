// e:\F1 Telemetry\web\static\app.js

document.addEventListener("DOMContentLoaded", () => {
    // Generate RPM LEDs (8 Green, 8 Red, 4 Blue)
    const ledStrip = document.getElementById("rpm-led-strip");
    const totalLeds = 20;
    for (let i = 0; i < totalLeds; i++) {
        const led = document.createElement("div");
        led.classList.add("rpm-led");
        if (i < 8) led.classList.add("green");
        else if (i < 16) led.classList.add("red");
        else led.classList.add("blue");
        ledStrip.appendChild(led);
    }

    // Modal Control
    const settingsModal = document.getElementById("settings-modal");
    const btnSettings = document.getElementById("btn-settings");
    const btnCloseSettings = document.getElementById("btn-close-settings");
    const btnCancelSettings = document.getElementById("btn-cancel-settings");
    const formConfig = document.getElementById("form-config");

    const openSettings = () => {
        fetch("/api/config")
            .then(res => res.json())
            .then(data => {
                document.getElementById("cfg-udp-port").value = data.udp_port;
                document.getElementById("cfg-kafka-broker").value = data.kafka_broker;
                document.getElementById("cfg-kafka-topic").value = data.kafka_topic;
                document.getElementById("cfg-azure-account").value = data.azure_storage_account || "";
                document.getElementById("cfg-azure-key").value = data.azure_access_key || "";
                document.getElementById("cfg-azure-container").value = data.azure_container || "";
                document.getElementById("cfg-azure-dir").value = data.azure_directory || "";
                
                settingsModal.classList.remove("hidden");
            })
            .catch(err => logConsole("Erro ao carregar configurações: " + err, "error"));
    };

    const closeSettings = () => settingsModal.classList.add("hidden");

    btnSettings.addEventListener("click", openSettings);
    btnCloseSettings.addEventListener("click", closeSettings);
    btnCancelSettings.addEventListener("click", closeSettings);

    formConfig.addEventListener("submit", (e) => {
        e.preventDefault();
        const payload = {
            udp_port: parseInt(document.getElementById("cfg-udp-port").value),
            kafka_broker: document.getElementById("cfg-kafka-broker").value,
            kafka_topic: document.getElementById("cfg-kafka-topic").value,
            azure_storage_account: document.getElementById("cfg-azure-account").value,
            azure_access_key: document.getElementById("cfg-azure-key").value,
            azure_container: document.getElementById("cfg-azure-container").value,
            azure_directory: document.getElementById("cfg-azure-dir").value,
        };

        fetch("/api/config", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        })
        .then(res => res.json())
        .then(data => {
            if (data.status === "success") {
                logConsole("Configurações salvas e aplicadas!", "kafka");
                closeSettings();
            } else {
                logConsole("Erro: " + data.message, "error");
            }
        })
        .catch(err => logConsole("Erro ao salvar configurações: " + err, "error"));
    });

    // Capture Controls
    const btnToggleCapture = document.getElementById("btn-toggle-capture");
    const statusBadge = document.getElementById("status-badge");
    const statusText = statusBadge.querySelector(".text");
    let isCapturing = false;

    btnToggleCapture.addEventListener("click", () => {
        const endpoint = isCapturing ? "/api/capture/stop" : "/api/capture/start";
        fetch(endpoint, { method: "POST" })
            .then(res => res.json())
            .then(data => {
                if (data.status === "success") {
                    logConsole(data.message, "system");
                } else {
                    logConsole("Erro: " + data.message, "error");
                }
            })
            .catch(err => logConsole("Erro na operação: " + err, "error"));
    });

    // WebSocket Management
    let ws;
    const connectWS = () => {
        const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
        const wsUrl = `${protocol}//${window.location.host}/ws`;
        
        ws = new WebSocket(wsUrl);

        ws.onopen = () => {
            logConsole("Conectado ao canal WebSocket do coletor.", "system");
        };

        ws.onmessage = (event) => {
            const msg = JSON.parse(event.data);
            if (msg.type === "status_update") {
                updateSessionHUD(msg.status);
            } else if (msg.type === "telemetry_live") {
                handleLiveTelemetry(msg.packet_id, msg.data);
            }
        };

        ws.onclose = () => {
            logConsole("Conexão WebSocket perdida. Reconectando em 3s...", "error");
            setTimeout(connectWS, 3000);
        };

        ws.onerror = (err) => {
            console.error("Erro no WebSocket:", err);
        };
    };

    connectWS();

    // Logger UI helper
    const consoleDiv = document.getElementById("logs-console");
    function logConsole(message, type = "udp") {
        const line = document.createElement("div");
        line.classList.add("log-line", type);
        const now = new Date().toLocaleTimeString();
        line.innerText = `[${now}] ${message}`;
        consoleDiv.appendChild(line);
        consoleDiv.scrollTop = consoleDiv.scrollHeight;
        
        // Cap lines at 100
        while (consoleDiv.childElementCount > 100) {
            consoleDiv.removeChild(consoleDiv.firstChild);
        }
    }

    // Helper to format sector strings
    function formatTime(ms) {
        if (!ms) return "--.---";
        const totalSecs = ms / 1000;
        return totalSecs.toFixed(3);
    }
    
    function formatLapTime(ms) {
        if (!ms) return "--:--.---";
        const mins = Math.floor(ms / 60000);
        const secs = ((ms % 60000) / 1000).toFixed(3);
        const paddedSecs = parseFloat(secs) < 10 ? "0" + secs : secs;
        return `${mins}:${paddedSecs}`;
    }

    // Update session metadata cards
    function updateSessionHUD(status) {
        isCapturing = status.capturing;
        if (isCapturing) {
            statusBadge.className = "badge active";
            statusText.innerText = "Capturando";
            btnToggleCapture.innerText = "Parar Captura";
            btnToggleCapture.className = "btn btn-danger";
        } else {
            statusBadge.className = "badge idle";
            statusText.innerText = "Inativo";
            btnToggleCapture.innerText = "Iniciar Captura";
            btnToggleCapture.className = "btn btn-primary";
        }

        document.getElementById("meta-track").innerText = status.track_name || "Aguardando...";
        document.getElementById("meta-vehicle").innerText = status.vehicle_name || "Aguardando...";
        document.getElementById("meta-session-type").innerText = status.session_type_name || "Aguardando...";
        document.getElementById("meta-session-uid").innerText = status.session_uid ? status.session_uid.toString() : "------------------";
        document.getElementById("meta-laps").innerText = `${status.laps_completed || 0} / ${status.total_laps || "--"}`;
        document.getElementById("meta-best-lap").innerText = status.best_lap_time;
        document.getElementById("meta-best-sectors").innerText = status.best_sectors;
        document.getElementById("packets-count").innerText = status.packets_received || 0;

        // Update UDP Auditor Table
        if (status.udp_packet_stats && Array.isArray(status.udp_packet_stats)) {
            status.udp_packet_stats.forEach((count, packetId) => {
                const row = document.querySelector(`#udp-audit-rows tr[data-id="${packetId}"]`);
                if (row) {
                    const countTd = row.querySelector(".count");
                    if (countTd) {
                        countTd.innerText = count;
                    }
                    
                    // Highlight rows that are receiving data but NOT mapped anywhere
                    if (count > 0) {
                        const lakeStatus = row.cells[3].querySelector(".status-badge");
                        const isLakeUnmapped = lakeStatus && lakeStatus.classList.contains("error");
                        
                        if (isLakeUnmapped) {
                            row.style.backgroundColor = "rgba(239, 68, 68, 0.08)"; // Subtle red warning alert
                        } else {
                            row.style.backgroundColor = ""; // Reset/normal
                        }
                    } else {
                        row.style.backgroundColor = ""; // Reset/normal
                    }
                }
            });
        }
    }

    // Handle live stream of metrics
    function handleLiveTelemetry(packetId, data) {
        const playerCarIdx = data.header ? data.header.player_car_index : 0;
        
        if (packetId === 6) { // Car Telemetry
            const tele = data.car_telemetry_data[playerCarIdx];
            if (!tele) return;

            // Speed
            document.getElementById("display-speed").innerText = tele.speed;
            
            // Gear
            let gearText = tele.gear;
            if (gearText === 0) gearText = "N";
            else if (gearText === -1) gearText = "R";
            document.getElementById("display-gear").innerText = gearText;

            // Pedals
            document.getElementById("bar-throttle").style.height = `${(tele.throttle * 100).toFixed(0)}%`;
            document.getElementById("bar-brake").style.height = `${(tele.brake * 100).toFixed(0)}%`;

            // Engine RPM & Shift lights
            document.getElementById("display-rpm").innerText = tele.engine_rpm;
            updateRPMShiftLights(tele.engine_rpm);

            // Tyres Temperatures and Pressures
            // RL, RR, FL, FR mapping
            const tyresLocs = ["rl", "rr", "fl", "fr"];
            for (let i = 0; i < 4; i++) {
                const loc = tyresLocs[i];
                document.getElementById(`tyre-temp-${loc}`).innerText = tele.tyres_surface_temperature[i] || "--";
                document.getElementById(`tyre-press-${loc}`).innerText = tele.tyres_pressure[i] ? tele.tyres_pressure[i].toFixed(1) : "--.-";
            }
        }
    }

    function updateRPMShiftLights(rpm) {
        // Redline starts at roughly 11000 RPM, max is 15000
        const minRPM = 10500;
        const maxRPM = 13800;
        const range = maxRPM - minRPM;
        
        const leds = document.querySelectorAll(".rpm-led");
        
        if (rpm < minRPM) {
            leds.forEach(led => led.classList.remove("on"));
            return;
        }

        const percentage = Math.min((rpm - minRPM) / range, 1);
        const ledsToLight = Math.floor(percentage * totalLeds);

        leds.forEach((led, index) => {
            if (index < ledsToLight) {
                led.classList.add("on");
            } else {
                led.classList.remove("on");
            }
        });
    }
});
