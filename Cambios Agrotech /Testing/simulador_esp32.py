"""
AgroTech - Simulador de ESP32 (Versión con ML Testing y CLI)
------------------------------------------------------------
Emula uno o varios nodos ESP32 enviando:
1) Sensores por MQTT (Topic: esp32/<nodo>/data)
2) Fotografías por HTTP (POST http://<servidor>:8080/upload?nodo=<nodo>)

NUEVAS FUNCIONES (Testing Machine Learning):
- Consola interactiva para cambiar el comportamiento en tiempo real.
- Modo "Planta Sana": Datos dentro de lo esperado.
- Modo "Planta Enferma": Genera anomalías severas (para probar Isolation Forest).
- Control de Piramómetro: Habilita/Inhabilita el envío del campo de radiación.
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

MQTT_BROKER = "localhost"
MQTT_PORT = 1883

HTTP_SERVER_HOST = "127.0.0.1"
HTTP_PHOTO_PORT = 8080
HTTP_PHOTO_PATH = "/upload"

# >>> CAMBIA ESTA RUTA POR LA CARPETA REAL DE TUS IMÁGENES <<<
IMAGE_DIR = r"C:\Users\Usuario\Desktop\fotos de plantas para agrotech"

SIMULATED_NODES = [
    "nodo2"
]

SENSOR_INTERVAL_SECONDS = 30
PHOTO_INTERVAL_SECONDS = 30
SEND_PHOTO_ON_START = True

IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".bmp", ".webp"
}

# Confirmación de horarios (esp32/<nodo>/configok)
CONFIRM_HORARIOS = True
CONFIRM_DELAY_SECONDS = (1.0, 3.0)
CONFIRM_PROBABILITY = 1.0


# ============================================================
# VARIABLES DE ESTADO GLOBALES PARA LA CONSOLA (CLI)
# ============================================================
sim_mode = "sana"      # Opciones: "sana", "enferma"
sim_pira = True        # Opciones: True, False


# ============================================================
# RANGOS DE LOS SENSORES
# ============================================================

# Rangos normales (Modo Planta Sana)
RANGES_SANA = {
    "dht_temp": (20.0, 30.0),       
    "dht_humidity": (50.0, 80.0),   
    "soil_N": (20, 40),            
    "soil_P": (15, 30),            
    "soil_K": (50, 150),            
    "soil_humidity": (25.0, 45.0),  
    "soil_temp": (20.0, 35.0),      
    "soil_ec": (200, 400),         
    "soil_ph": (5.5, 6.5),          
    "battery": (21.0, 100.0),       
    "piramometro_rad": (100.0, 1000.0) 
}

# Rangos intermedios (Modo Advertencia para probar sensibilidad del ML)
RANGES_ADVERTENCIA = {
    "dht_temp": (32, 39),       # Temperatura al límite superior
    "dht_humidity": (32, 40),   # Humedad ambiente comenzando a bajar
    "soil_N": (5, 10),             # Nutrientes en declive
    "soil_P": (5, 10),
    "soil_K": (5, 10),
    "soil_humidity": (15, 20),  # Suelo secándose moderadamente
    "soil_temp": (36, 38),      # Suelo calentándose
    "soil_ec": (400, 600),          # Salinidad subiendo a niveles de precaución
    "soil_ph": (4, 5),          # pH acidificándose levemente
    "battery": (21.0, 100.0),       
    "piramometro_rad": (1001.0, 1199.0) # Radiación alta pero no extrema
}

# Rangos anómalos (Modo Crítico para probar anomalías severas)
RANGES_CRITICO = {
    "dht_temp": (40.0, 45.0),       
    "dht_humidity": (15.0, 32),   
    "soil_N": (0, 5),              
    "soil_P": (0, 5),
    "soil_K": (0, 5),
    "soil_humidity": (5.0, 10.0),  
    "soil_temp": (39.0, 45.0),      
    "soil_ec": (700, 1000),        
    "soil_ph": (3, 4),          
    "battery": (1.0, 100.0),       
    "piramometro_rad": (1200.0, 1400.0) 
}


# ============================================================
# ESTADO DE CADA NODO SIMULADO
# ============================================================
node_state = {}
state_lock = threading.Lock()
received_config = {}
config_lock = threading.Lock()

for node_id in SIMULATED_NODES:
    received_config[node_id] = {
        "horarios": [],
        "ultima_configuracion": None,
    }

def crear_estado_nodo(nodo_id):
    return {
        "battery": random.uniform(*RANGES_SANA["battery"]),
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
    """Genera datos según el modo actual de simulación."""
    global sim_mode, sim_pira
    
    # Seleccionar el diccionario de rangos correcto según el estado
    if sim_mode == "sana":
        ranges_current = RANGES_SANA
    elif sim_mode == "advertencia":
        ranges_current = RANGES_ADVERTENCIA
    else:
        ranges_current = RANGES_CRITICO

    with state_lock:
        state = node_state[nodo_id]
        state["cycle"] += 1
        state["battery"] -= random.uniform(0.0, 0.12)
        if state["battery"] < 20:
            state["battery"] += random.uniform(0.5, 2.0)

        state["battery"] = clamp(
            state["battery"],
            RANGES_SANA["battery"][0],
            RANGES_SANA["battery"][1]
        )
        battery = round(state["battery"], 1)

    data = {
        "dht": {
            "t": round(random.uniform(*ranges_current["dht_temp"]), 1),
            "h": round(random.uniform(*ranges_current["dht_humidity"]), 1),
        },
        "suelo": {
            "N": random.randint(*ranges_current["soil_N"]),
            "P": random.randint(*ranges_current["soil_P"]),
            "K": random.randint(*ranges_current["soil_K"]),
            "hum": round(random.uniform(*ranges_current["soil_humidity"]), 1),
            "temp": round(random.uniform(*ranges_current["soil_temp"]), 1),
            "ec": random.randint(*ranges_current["soil_ec"]),
            "ph": round(random.uniform(*ranges_current["soil_ph"]), 2),
        },
        "bateria": {
            "porcentaje": battery,
        },
    }

    # Condicional de hardware: Simula si el nodo tiene o no conectado el Piramómetro
    if sim_pira:
        data["piramometro"] = {
            "rad": round(random.uniform(*ranges_current["piramometro_rad"]), 1)
        }

    return data


# ============================================================
# MQTT Y LÓGICA DE CONFIRMACIÓN
# ============================================================
mqtt_client = None
mqtt_connected = threading.Event()

def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        mqtt_connected.set()
        print(f"[MQTT] Conectado a {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe("esp32/config")
        client.subscribe("esp32/+/config")
    else:
        mqtt_connected.clear()
        print(f"[MQTT] Error de conexión. reason_code={reason_code}")

def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    mqtt_connected.clear()
    print(f"[MQTT] Desconectado. reason_code={reason_code}")

def _reconstruir_horarios_config(config):
    horas = config.get("horas")
    minutos = config.get("minutos")
    if not isinstance(horas, list):
        raise ValueError("El campo 'horas' no es una lista.")
    if isinstance(minutos, list):
        if len(horas) != len(minutos):
            raise ValueError(f"'horas' tiene {len(horas)} elementos y 'minutos' tiene {len(minutos)}.")
        horarios = []
        for h, m in zip(horas, minutos):
            h, m = int(h), int(m)
            if not (0 <= h <= 23) or not (0 <= m <= 59):
                raise ValueError("Hora/Minuto fuera de rango.")
            horarios.append(f"{h:02d}:{m:02d}")
        return horarios
    horarios = []
    for h in horas:
        h = int(h)
        if not (0 <= h <= 23):
            raise ValueError("Hora fuera de rango.")
        horarios.append(f"{h:02d}:00")
    return horarios

def enviar_confirmacion_horario(nodo_id, horarios):
    if not CONFIRM_HORARIOS: return
    if random.random() > CONFIRM_PROBABILITY: return
    espera = random.uniform(*CONFIRM_DELAY_SECONDS)
    time.sleep(espera)
    if not mqtt_connected.is_set(): return
    topic = f"esp32/{nodo_id}/configok"
    payload = json.dumps({"ok": True, "nodo": nodo_id, "horarios": horarios}, separators=(",", ":"))
    info = mqtt_client.publish(topic, payload, qos=1, retain=False)
    info.wait_for_publish(timeout=5)

def _aplicar_configuracion(nodos_afectados, horarios, origen_topic):
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with config_lock:
        for node_id in nodos_afectados:
            received_config[node_id] = {"horarios": horarios.copy(), "ultima_configuracion": ahora}
    print(f"\n[ESP32] Configuración JSON válida. Horarios: {', '.join(horarios)}")
    for node_id in nodos_afectados:
        threading.Thread(target=enviar_confirmacion_horario, args=(node_id, horarios.copy()), daemon=True).start()

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
    except UnicodeDecodeError: return
    partes = msg.topic.split("/")
    if msg.topic == "esp32/config":
        nodos_afectados = list(SIMULATED_NODES)
    elif len(partes) == 3 and partes[0] == "esp32" and partes[2] == "config":
        nodo_id = partes[1]
        if nodo_id not in SIMULATED_NODES: return
        nodos_afectados = [nodo_id]
    else: return
    try:
        config = json.loads(payload)
        horarios = _reconstruir_horarios_config(config)
        _aplicar_configuracion(nodos_afectados, horarios, msg.topic)
    except Exception as exc: pass

def iniciar_mqtt():
    global mqtt_client
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="agrotech_simulador")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()

def publicar_sensor(nodo_id):
    topic = f"esp32/{nodo_id}/data"
    data = generar_datos(nodo_id)
    payload = json.dumps(data, separators=(",", ":"))

    if not mqtt_connected.is_set(): return
    info = mqtt_client.publish(topic, payload, qos=0, retain=False)
    
    # Formateo dinámico del piramómetro para impresión en consola
    pira_str = f"Rad={data['piramometro']['rad']}W/m²" if "piramometro" in data else "Rad=---"
    
    if info.rc == mqtt.MQTT_ERR_SUCCESS:
        print(f"[MQTT] {nodo_id} | T={data['dht']['t']}°C H={data['dht']['h']}% | "
              f"NPK={data['suelo']['N']}/{data['suelo']['P']}/{data['suelo']['K']} | "
              f"HumS={data['suelo']['hum']}% pH={data['suelo']['ph']} | "
              f"Bat={data['bateria']['porcentaje']}% | {pira_str}")


# ============================================================
# IMÁGENES HTTP
# ============================================================
def obtener_imagenes():
    if not os.path.isdir(IMAGE_DIR): return []
    return [os.path.join(IMAGE_DIR, n) for n in os.listdir(IMAGE_DIR) 
            if os.path.isfile(os.path.join(IMAGE_DIR, n)) and os.path.splitext(n)[1].lower() in IMAGE_EXTENSIONS]

def enviar_imagen_http(nodo_id):
    try:
        imagenes = obtener_imagenes()
        if not imagenes: return
        ruta_imagen = random.choice(imagenes)
        with open(ruta_imagen, "rb") as f: imagen_bytes = f.read()

        query = urllib.parse.urlencode({"nodo": nodo_id})
        url = f"http://{HTTP_SERVER_HOST}:{HTTP_PHOTO_PORT}{HTTP_PHOTO_PATH}?{query}"
        content_type = mimetypes.guess_type(ruta_imagen)[0] or "image/jpeg"

        request = urllib.request.Request(url=url, data=imagen_bytes, method="POST",
                                         headers={"Content-Type": content_type, "Content-Length": str(len(imagen_bytes))})
        with urllib.request.urlopen(request, timeout=15) as response: pass
    except Exception as exc: pass


# ============================================================
# HILOS DE SIMULACIÓN
# ============================================================
def hilo_sensores(nodo_id):
    while True:
        try: publicar_sensor(nodo_id)
        except Exception as exc: print(f"[SIM] Error en sensores de {nodo_id}: {exc}")
        time.sleep(SENSOR_INTERVAL_SECONDS)

def hilo_camara(nodo_id):
    if SEND_PHOTO_ON_START: enviar_imagen_http(nodo_id)
    while True:
        time.sleep(PHOTO_INTERVAL_SECONDS)
        try: enviar_imagen_http(nodo_id)
        except Exception: pass


# ============================================================
# MAIN Y CONSOLA INTERACTIVA (CLI)
# ============================================================
def main():
    global sim_mode, sim_pira

    print("=" * 70)
    print("AGROTECH - SIMULADOR INTERACTIVO ESP32 + ML TESTING")
    print("=" * 70)
    print(f"Broker MQTT : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"Nodos simulados: {', '.join(SIMULATED_NODES)}")
    print(f"Intervalo MQTT: {SENSOR_INTERVAL_SECONDS}s | Intervalo HTTP: {PHOTO_INTERVAL_SECONDS}s")
    print("=" * 70)
    print("COMANDOS DISPONIBLES EN CONSOLA:")
    print("  sana        -> Entorno en parámetros normales.")
    print("  advertencia -> Desviaciones leves (Prueba de sensibilidad ML).")
    print("  critico     -> Anomalías severas (Prueba de alertas graves ML).")
    print("  pira on     -> Habilita el piramómetro.")
    print("  pira off    -> Inhabilita el piramómetro.")
    print("  estado      -> Muestra la configuración actual de la simulación.")
    print("  salir       -> Detener el simulador.")
    print("=" * 70)

    if not SIMULATED_NODES: return
    iniciar_mqtt()

    hilos = []
    for nodo_id in SIMULATED_NODES:
        hilo = threading.Thread(target=hilo_sensores, args=(nodo_id,), daemon=True)
        hilo.start()
        hilos.append(hilo)

        hilo = threading.Thread(target=hilo_camara, args=(nodo_id,), daemon=True)
        hilo.start()
        hilos.append(hilo)

    print("\n[SIM] Simulación activa. Escribe un comando y presiona Enter...\n")

    # Bucle de comandos interactivo
    try:
        while True:
            cmd = input().strip().lower()
            if cmd == 'sana':
                sim_mode = 'sana'
                print("\n⚙️  [CLI] MODO CAMBIADO: 🌱 PLANTA SANA (Parámetros Normales)")
            elif cmd == 'advertencia':
                sim_mode = 'advertencia'
                print("\n⚙️  [CLI] MODO CAMBIADO: ⚠️ ADVERTENCIA (Desviaciones Leves Activadas)")
            elif cmd == 'critico':
                sim_mode = 'critico'
                print("\n⚙️  [CLI] MODO CAMBIADO: 🚨 CRÍTICO (Anomalías Severas Activadas)")
            elif cmd == 'pira on':
                sim_pira = True
                print("\n⚙️  [CLI] HARDWARE: ☀️ Piramómetro ACTIVADO")
            elif cmd == 'pira off':
                sim_pira = False
                print("\n⚙️  [CLI] HARDWARE: ❌ Piramómetro DESACTIVADO (Excluido de los datos)")
            elif cmd == 'estado':
                print(f"\n📊 [CLI] ESTADO ACTUAL -> Modo: {sim_mode.upper()} | Piramómetro: {'ON' if sim_pira else 'OFF'}")
            elif cmd in ['salir', 'quit', 'exit']:
                print("\n[SIM] Deteniendo simulador...")
                break
            elif cmd == '':
                pass
            else:
                print(f"[CLI] Comando desconocido: '{cmd}'. Usa: sana, advertencia, critico, pira on, pira off, estado, salir") 
                
    except KeyboardInterrupt:
        print("\n[SIM] Interrupción manual. Deteniendo simulador...")
    finally:
        if mqtt_client is not None:
            try:
                mqtt_client.loop_stop()
                mqtt_client.disconnect()
            except Exception: pass
        print("[SIM] Simulador detenido.")

if __name__ == "__main__":
    main()
