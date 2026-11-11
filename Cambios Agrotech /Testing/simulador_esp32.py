"""
AgroTech - Simulador de ESP32
--------------------------------
Emula uno o varios nodos ESP32 enviando:

1) Sensores por MQTT:
   Topic: esp32/<nodo>/data

2) Fotografías por HTTP:
   POST http://<servidor>:8080/upload?nodo=<nodo>

El formato de los datos MQTT coincide con el que espera
agrotech_app_v11(1).py.

Requisitos:
    pip install paho-mqtt

Uso rápido:
    1. Configura IMAGE_DIR más abajo.
    2. Ejecuta primero agrotech_app_v11(1).py.
    3. Asegúrate de tener un broker MQTT en localhost:1883.
    4. Ejecuta:
           python simulador_esp32.py

Para simular más nodos:
    SIMULATED_NODES = ["nodo1", "nodo2", "nodo3", "nodo4"]

IMPORTANTE:
- La carpeta de imágenes debe existir en el ordenador donde se ejecuta
  este simulador.
- El simulador elige una imagen aleatoria en cada envío HTTP.
- Por defecto se reutilizan imágenes; no se eliminan ni modifican.
"""

import json
import mimetypes
import os
import random
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime

import paho.mqtt.client as mqtt


# ============================================================
# CONFIGURACIÓN PRINCIPAL
# ============================================================

# Broker MQTT al que se conecta el simulador.
MQTT_BROKER = "localhost"
MQTT_PORT = 1883

# Servidor HTTP donde está ejecutándose AgroTech.
HTTP_SERVER_HOST = "127.0.0.1"
HTTP_PHOTO_PORT = 8080
HTTP_PHOTO_PATH = "/upload"

# >>> CAMBIA ESTA RUTA POR LA CARPETA REAL DE TUS IMÁGENES <<<
#
# Windows, ejemplos:
# IMAGE_DIR = r"C:\Users\Usuario\Desktop\Wallpapers"
# IMAGE_DIR = r"D:\Proyecto\imagenes"
#
# También puedes usar:
# IMAGE_DIR = os.path.expanduser("~/Pictures/AgroTech")




IMAGE_DIR = r"C:\Users\Usuario\Desktop\Wallpapers"




# Nodos que quieres simular.
# Para probar solamente el nodo que actualmente está activo
# en tu aplicación, deja únicamente "nodo1".
SIMULATED_NODES = [
    "nodo1",
    "nodo2",
    "nodo3",
    "nodo4",
]

# Cada cuánto se genera y publica una lectura MQTT.
SENSOR_INTERVAL_SECONDS = 10

# Cada cuánto se envía una fotografía HTTP.
PHOTO_INTERVAL_SECONDS = 30

# True: una fotografía se envía inmediatamente al arrancar.
SEND_PHOTO_ON_START = True

# Extensiones de imágenes aceptadas.
IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".bmp", ".webp"
}


# ============================================================
# RANGOS DE LOS SENSORES
# ============================================================
#
# Puedes modificar libremente estos valores para representar
# las condiciones de tu cultivo.

RANGES = {
    # DHT22
    "dht_temp": (18.0, 32.0),       # °C
    "dht_humidity": (45.0, 90.0),   # %

    # Sensor de suelo
    "soil_N": (20, 180),            # mg/kg
    "soil_P": (10, 100),            # mg/kg
    "soil_K": (30, 250),            # mg/kg
    "soil_humidity": (25.0, 85.0),  # %
    "soil_temp": (17.0, 30.0),      # °C
    "soil_ec": (200, 1800),         # µS/cm
    "soil_ph": (5.0, 7.5),          # pH

    # Batería
    "battery": (55.0, 100.0),       # %
}


# ============================================================
# ESTADO DE CADA NODO SIMULADO
# ============================================================

node_state = {}
state_lock = threading.Lock()

# Configuración recibida desde AgroTech.
# Se conserva por separado para poder ver en el CMD qué horario
# tiene aplicado cada nodo simulado.
received_config = {}
config_lock = threading.Lock()

for node_id in SIMULATED_NODES:
    received_config[node_id] = {
        "horarios": [],
        "ultima_configuracion": None,
    }


def crear_estado_nodo(nodo_id):
    """Crea valores iniciales realistas para un nodo."""
    return {
        "battery": random.uniform(*RANGES["battery"]),
        "cycle": 0,
    }


for node_id in SIMULATED_NODES:
    node_state[node_id] = crear_estado_nodo(node_id)


# ============================================================
# GENERACIÓN DE DATOS
# ============================================================

def clamp(value, minimum, maximum):
    return max(minimum, min(value, maximum))


def generar_datos(nodo_id):
    """
    Genera exactamente la estructura principal que consume
    el callback MQTT de AgroTech:
        dht
        suelo
        bateria
    """

    # Pequeña evolución de batería para que las lecturas no sean
    # completamente independientes entre sí.
    with state_lock:
        state = node_state[nodo_id]
        state["cycle"] += 1

        # El nodo va consumiendo batería lentamente.
        state["battery"] -= random.uniform(0.0, 0.12)

        # Recarga simulada ocasionalmente.
        if state["battery"] < 20:
            state["battery"] += random.uniform(0.5, 2.0)

        state["battery"] = clamp(
            state["battery"],
            RANGES["battery"][0],
            RANGES["battery"][1]
        )

        battery = round(state["battery"], 1)

    data = {
        "dht": {
            "t": round(random.uniform(*RANGES["dht_temp"]), 1),
            "h": round(random.uniform(*RANGES["dht_humidity"]), 1),
        },
        "suelo": {
            "N": random.randint(*RANGES["soil_N"]),
            "P": random.randint(*RANGES["soil_P"]),
            "K": random.randint(*RANGES["soil_K"]),
            "hum": round(random.uniform(*RANGES["soil_humidity"]), 1),
            "temp": round(random.uniform(*RANGES["soil_temp"]), 1),
            "ec": random.randint(*RANGES["soil_ec"]),
            "ph": round(random.uniform(*RANGES["soil_ph"]), 2),
        },
        "bateria": {
            "porcentaje": battery,
        },
    }

    return data


# ============================================================
# MQTT
# ============================================================

mqtt_client = None
mqtt_connected = threading.Event()


def on_connect(client, userdata, flags, reason_code, properties=None):
    """
    El simulador también escucha esp32/config para poder observar
    cuándo la aplicación envía una nueva configuración de horarios.
    """
    if reason_code == 0:
        mqtt_connected.set()
        print(f"[MQTT] Conectado a {MQTT_BROKER}:{MQTT_PORT}")

        client.subscribe("esp32/config")
        print("[MQTT] Suscrito a esp32/config")

    else:
        mqtt_connected.clear()
        print(f"[MQTT] Error de conexión. reason_code={reason_code}")


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    mqtt_connected.clear()
    print(f"[MQTT] Desconectado. reason_code={reason_code}")


def _reconstruir_horarios_config(config):
    """
    Reconstruye los horarios HH:MM usando el JSON que actualmente
    envía AgroTech:

        {
            "horas": [10, 18],
            "minutos": [30, 0]
        }

    También acepta el formato antiguo donde solo existía "horas".
    """
    horas = config.get("horas")
    minutos = config.get("minutos")

    if not isinstance(horas, list):
        raise ValueError("El campo 'horas' no es una lista.")

    # Formato nuevo: ambas listas deben corresponder posición a posición.
    if isinstance(minutos, list):
        if len(horas) != len(minutos):
            raise ValueError(
                f"'horas' tiene {len(horas)} elementos y "
                f"'minutos' tiene {len(minutos)}."
            )

        horarios = []
        for h, m in zip(horas, minutos):
            h = int(h)
            m = int(m)

            if not (0 <= h <= 23):
                raise ValueError(f"Hora fuera de rango: {h}")
            if not (0 <= m <= 59):
                raise ValueError(f"Minuto fuera de rango: {m}")

            horarios.append(f"{h:02d}:{m:02d}")

        return horarios

    # Compatibilidad con el formato anterior: horas enteras => HH:00.
    horarios = []
    for h in horas:
        h = int(h)
        if not (0 <= h <= 23):
            raise ValueError(f"Hora fuera de rango: {h}")
        horarios.append(f"{h:02d}:00")

    return horarios


def on_message(client, userdata, msg):
    """
    Emula la recepción que tendría un ESP32 cuando AgroTech publica
    una nueva configuración por MQTT.

    AgroTech publica actualmente el JSON en:
        esp32/config

    El JSON contiene las listas paralelas 'horas' y 'minutos'.
    """
    print()
    print("=" * 70)
    print("[ESP32] MENSAJE RECIBIDO DESDE AGROTECH")
    print("=" * 70)
    print(f"[ESP32] Topic   : {msg.topic}")

    try:
        payload = msg.payload.decode("utf-8")
    except UnicodeDecodeError:
        print("[ESP32] Error: el mensaje no contiene UTF-8 válido.")
        print("=" * 70)
        return

    print(f"[ESP32] JSON    : {payload}")

    try:
        config = json.loads(payload)

        if not isinstance(config, dict):
            raise ValueError("El JSON recibido no es un objeto.")

        horarios = _reconstruir_horarios_config(config)

        # AgroTech no incluye nodo_id en este JSON; el topic también es
        # global (esp32/config). Por ello, en este simulador la orden se
        # considera un broadcast y se aplica a todos los nodos simulados.
        ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with config_lock:
            for node_id in SIMULATED_NODES:
                received_config[node_id] = {
                    "horarios": horarios.copy(),
                    "ultima_configuracion": ahora,
                }

        print("[ESP32] Configuración JSON válida.")
        print(
            "[ESP32] Horarios reconstruidos: "
            + ", ".join(horarios)
        )
        print(
            "[ESP32] Configuración aplicada a los nodos simulados: "
            + ", ".join(SIMULATED_NODES)
        )
        print(f"[ESP32] Hora de recepción: {ahora}")

    except json.JSONDecodeError as exc:
        print(f"[ESP32] Error: JSON inválido: {exc}")
    except (TypeError, ValueError) as exc:
        print(f"[ESP32] Error procesando configuración: {exc}")
    except Exception as exc:
        print(f"[ESP32] Error inesperado procesando configuración: {exc}")

    print("=" * 70)


def iniciar_mqtt():
    global mqtt_client

    mqtt_client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="agrotech_simulador"
    )

    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message

    print(f"[MQTT] Conectando a {MQTT_BROKER}:{MQTT_PORT}...")
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()


def publicar_sensor(nodo_id):
    """Publica una lectura del nodo por su topic correspondiente."""
    topic = f"esp32/{nodo_id}/data"
    data = generar_datos(nodo_id)
    payload = json.dumps(data, separators=(",", ":"))

    if not mqtt_connected.is_set():
        print(f"[MQTT] {nodo_id}: sin conexión; lectura no enviada")
        return

    info = mqtt_client.publish(topic, payload, qos=0, retain=False)

    if info.rc == mqtt.MQTT_ERR_SUCCESS:
        print(
            f"[MQTT] {nodo_id} -> {topic} | "
            f"T={data['dht']['t']}°C "
            f"H={data['dht']['h']}% | "
            f"NPK={data['suelo']['N']}/"
            f"{data['suelo']['P']}/"
            f"{data['suelo']['K']} | "
            f"Suelo={data['suelo']['hum']}% | "
            f"pH={data['suelo']['ph']} | "
            f"Bat={data['bateria']['porcentaje']}%"
        )
    else:
        print(f"[MQTT] Error publicando {nodo_id}: rc={info.rc}")


# ============================================================
# IMÁGENES
# ============================================================

def obtener_imagenes():
    """Devuelve las imágenes disponibles en IMAGE_DIR."""
    if not os.path.isdir(IMAGE_DIR):
        raise FileNotFoundError(
            f"La carpeta de imágenes no existe:\n{IMAGE_DIR}"
        )

    archivos = []

    for nombre in os.listdir(IMAGE_DIR):
        ruta = os.path.join(IMAGE_DIR, nombre)

        if not os.path.isfile(ruta):
            continue

        extension = os.path.splitext(nombre)[1].lower()

        if extension in IMAGE_EXTENSIONS:
            archivos.append(ruta)

    return archivos


def seleccionar_imagen_aleatoria():
    imagenes = obtener_imagenes()

    if not imagenes:
        raise FileNotFoundError(
            f"No se encontraron imágenes compatibles en:\n{IMAGE_DIR}"
        )

    return random.choice(imagenes)


def enviar_imagen_http(nodo_id):
    """
    Emula el POST binario que espera _PhotoUploadHandler.

    La aplicación espera:
        POST /upload?nodo=nodo1

    con los bytes JPEG/PNG/etc. directamente en el body.
    """

    try:
        ruta_imagen = seleccionar_imagen_aleatoria()

        with open(ruta_imagen, "rb") as f:
            imagen_bytes = f.read()

        query = urllib.parse.urlencode({"nodo": nodo_id})
        url = (
            f"http://{HTTP_SERVER_HOST}:{HTTP_PHOTO_PORT}"
            f"{HTTP_PHOTO_PATH}?{query}"
        )

        extension = os.path.splitext(ruta_imagen)[1].lower()
        content_type = mimetypes.guess_type(ruta_imagen)[0] or "image/jpeg"

        request = urllib.request.Request(
            url=url,
            data=imagen_bytes,
            method="POST",
            headers={
                "Content-Type": content_type,
                "Content-Length": str(len(imagen_bytes)),
            },
        )

        inicio = time.perf_counter()

        with urllib.request.urlopen(request, timeout=15) as response:
            respuesta = response.read().decode(
                "utf-8", errors="replace"
            )
            status = response.status

        tiempo_ms = (time.perf_counter() - inicio) * 1000

        print(
            f"[HTTP] {nodo_id} -> {url} | "
            f"{os.path.basename(ruta_imagen)} | "
            f"{len(imagen_bytes) / 1024:.1f} KB | "
            f"HTTP {status} | {tiempo_ms:.0f} ms | "
            f"respuesta={respuesta!r}"
        )

    except Exception as exc:
        print(f"[HTTP] Error enviando foto de {nodo_id}: {exc}")


# ============================================================
# HILOS DE SIMULACIÓN
# ============================================================

def hilo_sensores(nodo_id):
    """Publica datos MQTT periódicamente."""
    print(
        f"[SIM] Iniciando sensores de {nodo_id} "
        f"cada {SENSOR_INTERVAL_SECONDS}s"
    )

    while True:
        try:
            publicar_sensor(nodo_id)
        except Exception as exc:
            print(f"[SIM] Error en sensores de {nodo_id}: {exc}")

        time.sleep(SENSOR_INTERVAL_SECONDS)


def hilo_camara(nodo_id):
    """Envía fotografías HTTP periódicamente."""
    print(
        f"[SIM] Iniciando cámara de {nodo_id} "
        f"cada {PHOTO_INTERVAL_SECONDS}s"
    )

    if SEND_PHOTO_ON_START:
        enviar_imagen_http(nodo_id)

    while True:
        time.sleep(PHOTO_INTERVAL_SECONDS)

        try:
            enviar_imagen_http(nodo_id)
        except Exception as exc:
            print(f"[SIM] Error en cámara de {nodo_id}: {exc}")


# ============================================================
# COMPROBACIONES
# ============================================================

def comprobar_configuracion():
    print("=" * 70)
    print("AGROTECH - SIMULADOR DE ESP32")
    print("=" * 70)

    print(f"Broker MQTT : {MQTT_BROKER}:{MQTT_PORT}")
    print(
        f"Servidor HTTP: "
        f"http://{HTTP_SERVER_HOST}:{HTTP_PHOTO_PORT}{HTTP_PHOTO_PATH}"
    )
    print(f"Carpeta imágenes: {IMAGE_DIR}")
    print(f"Nodos simulados: {', '.join(SIMULATED_NODES)}")
    print(f"Intervalo MQTT: {SENSOR_INTERVAL_SECONDS}s")
    print(f"Intervalo HTTP: {PHOTO_INTERVAL_SECONDS}s")
    print("Recepción MQTT: esp32/config")
    print('Formato esperado: {"horas":[...],"minutos":[...]}')
    print()

    if not os.path.isdir(IMAGE_DIR):
        print("[AVISO] La carpeta de imágenes todavía no existe.")

    else:
        try:
            imagenes = obtener_imagenes()
            print(f"[IMG] Imágenes encontradas: {len(imagenes)}")
        except Exception as exc:
            print(f"[IMG] {exc}")

    print("=" * 70)


# ============================================================
# MAIN
# ============================================================

def main():
    comprobar_configuracion()

    if not SIMULATED_NODES:
        raise RuntimeError(
            "SIMULATED_NODES está vacío. Añade al menos un nodo."
        )

    iniciar_mqtt()

    hilos = []

    for nodo_id in SIMULATED_NODES:
        if nodo_id not in node_state:
            node_state[nodo_id] = crear_estado_nodo(nodo_id)

        hilo = threading.Thread(
            target=hilo_sensores,
            args=(nodo_id,),
            daemon=True,
            name=f"MQTT-{nodo_id}"
        )
        hilo.start()
        hilos.append(hilo)

        hilo = threading.Thread(
            target=hilo_camara,
            args=(nodo_id,),
            daemon=True,
            name=f"HTTP-CAM-{nodo_id}"
        )
        hilo.start()
        hilos.append(hilo)

    print()
    print("[SIM] Simulación activa.")
    print("[SIM] Pulsa Ctrl+C para detenerla.")
    print()

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[SIM] Deteniendo simulador...")

    finally:
        if mqtt_client is not None:
            try:
                mqtt_client.loop_stop()
                mqtt_client.disconnect()
            except Exception:
                pass

        print("[SIM] Simulador detenido.")


if __name__ == "__main__":
    main()
