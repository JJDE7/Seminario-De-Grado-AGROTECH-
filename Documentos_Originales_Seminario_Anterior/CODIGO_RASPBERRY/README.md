# 🌱 AgroTech Dashboard

Sistema de monitoreo agrícola distribuido con interfaz táctil para **Raspberry Pi 4/5**, comunicación **MQTT** y sensores **ESP32**.

---

## ¿Qué hace?

Recibe en tiempo real los datos de nodos remotos de campo (temperatura, humedad, NPK del suelo, pH, conductividad eléctrica y batería), los muestra en una interfaz táctil optimizada para pantallas de 800×480 px, los guarda en disco con historial exportable a CSV y permite configurar el horario de mediciones de cada ESP32 desde la misma pantalla.

---

## Stack

| Capa | Tecnología |
|---|---|
| Hardware | Raspberry Pi 4 / 5 |
| UI | [Flet](https://flet.dev/) 0.84+ |
| Comunicación | MQTT via [paho-mqtt](https://pypi.org/project/paho-mqtt/) |
| Broker | Mosquitto (localhost:1883) |
| Nodos de campo | ESP32 + DHT22 + Sensor RS-485 + ESP32-CAM (opcional) |
| Almacenamiento | JSON + CSV en disco local |

---

## Arquitectura

```
ESP32 (nodo)                  Raspberry Pi 5
┌─────────────┐               ┌────────────────────────────────────┐
│ DHT22       │──MQTT JSON──▶ │ Mosquitto Broker (:1883)           │
│ Sensor NPK  │               │         │                          │
│ Batería     │               │         ▼                          │
│ ESP32-CAM   │──cam/data───▶ │  agrotech_app.py                   │
└─────────────┘               │  ├── Estado global (RAM)           │
                              │  ├── Persistencia JSON/CSV         │
                              │  └── UI Flet (800×480)             │
                              └────────────────────────────────────┘
```

Los datos de todos los nodos se reciben simultáneamente a través de un único cliente MQTT global. Cuando llega un mensaje, se actualiza el estado en RAM y se notifica a la pantalla activa vía un sistema de callbacks, sin polling ni hilos adicionales.

---

## Pantallas

| # | Pantalla | Descripción |
|---|---|---|
| 1 | **Inicio** | Bienvenida con logo y botón de entrada |
| 2 | **Selección de Nodo** | Cuadrícula de todos los nodos con estado (en línea / desconectado) |
| 3 | **Dashboard** | Datos en tiempo real con navegación por vistas (DHT22 + Batería / Suelo / Cámara) |
| 4 | **Configuración** | Horario de mediciones del nodo (2, 3 o hasta 12 medidas/día a hora personalizada) |
| 5 | **Historial** | Tabla paginada de hasta 500 registros con exportación a CSV |

---

## Estructura del Proyecto

```
agrotech/
├── agrotech_app.py        ← Aplicación principal
├── requirements.txt
├── README.md
└── data/                  ← Generada automáticamente
    ├── nodo1.json              ← Historial (máx. 500 registros)
    ├── nodo1_config.json       ← Horario activo del nodo
    ├── nodo1_historial.csv     ← Exportación manual
    └── cam_nodo1_FECHA.jpg     ← Imágenes guardadas
```

---

## Instalación en Raspberry Pi 5

### 1. Instalar Mosquitto

```bash
sudo apt update && sudo apt install -y mosquitto mosquitto-clients
sudo systemctl enable mosquitto
sudo systemctl start mosquitto
```

### 2. Crear entorno virtual e instalar dependencias

```bash
cd ~/agrotech
python3 -m venv venv
source venv/bin/activate
pip install flet==0.84.0 paho-mqtt
pip install flet-desktop --upgrade
```

### 3. Ejecutar

```bash
source venv/bin/activate
python agrotech_app.py
```

> Debes activar el entorno virtual (`source venv/bin/activate`) cada vez que abras una terminal nueva.

---

## Configuración

### Nodos

Edita la lista `NODOS` al inicio de `agrotech_app.py`:

```python
NODOS = [
    {
        "id":        "nodo1",           # Identificador único (se usa como nombre de archivo)
        "nombre":    "Nodo 1 · Parcela A",
        "topic":     "esp32/nodo1/data", # Topic MQTT — debe coincidir con el ESP32
        "camera_ip": "192.168.1.50",    # IP del ESP32-CAM, o None si no tiene cámara
        "activo":    True,              # False = aparece como desconectado
    },
]
```

### Broker MQTT

```python
MQTT_BROKER = "localhost"   # Cambia a la IP del broker si está en otro equipo
MQTT_PORT   = 1883
```

---

## Formato JSON del ESP32

El ESP32 publica en su topic (`esp32/nodoX/data`) el siguiente payload:

```json
{
  "dht":    { "t": 25.5, "h": 65.2 },
  "suelo":  { "N": 45, "P": 30, "K": 120, "hum": 35.5, "temp": 22.1, "ec": 350, "ph": 6.8 },
  "bateria":{ "porcentaje": 85.0 }
}
```

- Los tres objetos son opcionales; puedes enviar solo los que tengas disponibles.
- Para temperatura DHT acepta `"t"` o `"temp"`. Para humedad DHT acepta `"h"` o `"hum"`.

### Probar sin hardware

```bash
mosquitto_pub -h localhost -t "esp32/nodo1/data" -m \
'{"dht":{"t":24.5,"h":70.1},"suelo":{"N":38,"P":22,"K":115,"hum":42.0,"temp":21.3,"ec":310,"ph":6.5},"bateria":{"porcentaje":85.0}}'
```

---

## Protocolo de Cámara (MQTT)

Las imágenes se envían como fragmentos base64 en tres topics:

| Topic | Payload | Acción |
|---|---|---|
| `cam/start` | Tamaño total en bytes | Reinicia el buffer de imagen |
| `cam/data` | Fragmento base64 | Acumula en buffer |
| `cam/end` | (vacío) | Decodifica, guarda JPEG en disco y muestra en UI |

Las imágenes se guardan siempre en `data/cam_nodoX_FECHA_HORA.jpg` independientemente de si el dashboard está abierto.

---

## Compatibilidad Flet 0.84+

| API antigua | Flet 0.84+ |
|---|---|
| `ft.app()` | `ft.run()` |
| `ft.icons.X` | `ft.Icons.X` |
| `ft.colors.X` | `ft.Colors.X` |
| `ft.alignment.top_left` | `ft.Alignment(-1, -1)` |
| `ft.border.all()` | `ft.Border.all()` |
| `ft.padding.all()` | `ft.Padding.all()` |
| `ft.ElevatedButton` | `ft.FilledButton` |
| `page.go("/ruta")` | Navegación manual con `cont.content` |
| `ft.Ref[int]()` | `{"v": valor}` |

---

## Autoarranque al encender la Pi

Crea `/etc/systemd/system/agrotech.service`:

```ini
[Unit]
Description=AgroTech Dashboard
After=graphical.target mosquitto.service

[Service]
User=pi
WorkingDirectory=/home/pi/agrotech
ExecStart=/home/pi/agrotech/venv/bin/python agrotech_app.py
Restart=on-failure
Environment=DISPLAY=:0

[Install]
WantedBy=graphical.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable agrotech
sudo systemctl start agrotech
```

---

## Solución de Problemas

| Síntoma | Solución |
|---|---|
| Badge MQTT en rojo ("Sin conexión") | `sudo systemctl start mosquitto` |
| Datos no se actualizan | Verificar que el `topic` en `NODOS` coincide exactamente con el del ESP32 |
| Dashboard muestra `--` en todo | Publicar un mensaje de prueba con `mosquitto_pub` |
| Error `externally-managed-environment` | Activar el entorno virtual: `source venv/bin/activate` |
| Pantalla en blanco al abrir | El entorno virtual no está activo |
| Historial vacío | `ls ~/agrotech/data/` — verificar que existen los archivos JSON |
| Imágenes de cámara no aparecen | Verificar que el ESP32-CAM publica en `cam/start`, `cam/data`, `cam/end` |

---

## Dependencias

```
flet>=0.84.0
flet-desktop
paho-mqtt>=1.6.1
```

---

## Licencia

Proyecto privado — Quindío, Colombia 🇨🇴
