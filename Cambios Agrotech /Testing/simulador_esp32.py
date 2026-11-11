"""
AgroTech - Simulador de ESP32 (Versión con ML Testing y CLI)
------------------------------------------------------------
Emula uno o varios nodos ESP32 enviando:
1) Sensores por MQTT (Topic: esp32/<nodo>/data)
2) Fotografías por HTTP (POST http://<servidor>:8080/upload?nodo=<nodo>)

NUEVAS FUNCIONES (Testing Machine Learning):
- Consola interactiva para cambiar el comportamiento en tiempo real.
- Modo "Planta Sana": Datos dentro de lo esperado.
- Modos de Anomalía Básicos: Alteraciones generales altas y bajas.
- Modos de Anomalía Mixta/Correlacionada:
    - Ambiente/Suelo: Caliente y Seco | Frío y Húmedo.
    - Nutrientes: Exceso de Fertilizante (Salinidad) | Lavado de Nutrientes (Lixiviación).
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

SENSOR_INTERVAL_SECONDS = 15
PHOTO_INTERVAL_SECONDS = 15
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
sim_mode = "sana"      
sim_pira = True        


# ============================================================
# AGRUPACIÓN DE SENSORES PARA ANOMALÍAS
# ============================================================
GRUPOS_ANOMALIAS = {
    "ambiente": ["dht_temp", "dht_humidity"],
    "suelo": ["soil_humidity", "soil_temp"],
    "nutrientes": ["soil_N", "soil_P", "soil_K", "soil_ec", "soil_ph"],
    "radiacion": ["piramometro_rad"]
}

# ============================================================
# RANGOS DE LOS SENSORES
# ============================================================

# Rangos normales (Modo Planta Sana)
RANGES_SANA = {
    "dht_temp": (19.0, 29.0), 
    "dht_humidity": (45.0, 75.0), 
    "soil_N": (15, 45),           
    "soil_P": (18, 55),           
    "soil_K": (45, 245),            
    "soil_humidity": (20.0, 43.0),
    "soil_temp": (21.0, 29.0),   
    "soil_ec": (200, 400),        
    "soil_ph": (6, 6.5),         
    "battery": (21.0, 100.0),       
    "piramometro_rad": (100.0, 1000.0) 
}

# --- ANOMALÍAS GENÉRICAS ---
RANGES_ANOMALOS_ALTA = {
    "dht_temp": (35.0, 45.0),       
    "dht_humidity": (85.0, 95.0),   
    "soil_N": (60, 100),            
    "soil_P": (45, 80),            
    "soil_K": (200, 300),            
    "soil_humidity": (55.0, 75.0),  
    "soil_temp": (38.0, 45.0),      
    "soil_ec": (500, 800),         
    "soil_ph": (7.0, 8.5),                
    "piramometro_rad": (1100.0, 1400.0) 
}

RANGES_ANOMALOS_BAJA = {
    "dht_temp": (5.0, 15.0),       
    "dht_humidity": (15.0, 30.0),   
    "soil_N": (0, 10),            
    "soil_P": (0, 10),            
    "soil_K": (0, 20),            
    "soil_humidity": (5.0, 15.0),  
    "soil_temp": (5.0, 15.0),      
    "soil_ec": (50, 100),         
    "soil_ph": (3.5, 4.5),                
    "piramometro_rad": (0.0, 50.0) 
}

# --- ANOMALÍAS MIXTAS/CORRELACIONADAS ---
RANGES_FRIO_HUMEDO = {
    "dht_temp": (5.0, 12.0),       
    "dht_humidity": (85.0, 98.0),   
    "soil_temp": (10.0, 15.0),      
    "soil_humidity": (60.0, 80.0),  
}

RANGES_CALIENTE_SECO = {
    "dht_temp": (35.0, 45.0),       
    "dht_humidity": (10.0, 25.0),   
    "soil_temp": (35.0, 42.0),      
    "soil_humidity": (5.0, 15.0),  
}

RANGES_EXCESO_FERTILIZANTE = {
    "soil_N": (80, 120),            
    "soil_P": (60, 90),            
    "soil_K": (250, 350),            
    "soil_ec": (800, 1500),         # Alta salinidad por iones de fertilizante
    "soil_ph": (4.0, 5.0),          # Acidificación severa
}

RANGES_LAVADO_NUTRIENTES = {
    "soil_N": (0, 5),            
    "soil_P": (0, 5),            
    "soil_K": (0, 10),            
    "soil_ec": (20, 80),            # Suelo inerte, sin sales conductoras
    "soil_ph": (7.5, 8.5),          # Tendencia a alcalinizarse
}

# MAPEO DE MODOS A SUS GRUPOS Y RANGOS
MODOS_CONFIG = {
    "ambiente_alta": ("ambiente", RANGES_ANOMALOS_ALTA),
    "ambiente_baja": ("ambiente", RANGES_ANOMALOS_BAJA),
    "ambiente_caliente_seco": ("ambiente", RANGES_CALIENTE_SECO),
    "ambiente_frio_humedo": ("ambiente", RANGES_FRIO_HUMEDO),
    
    "suelo_alta": ("suelo", RANGES_ANOMALOS_ALTA),
    "suelo_baja": ("suelo", RANGES_ANOMALOS_BAJA),
    "suelo_caliente_seco": ("suelo", RANGES_CALIENTE_SECO),
    "suelo_frio_humedo": ("suelo", RANGES_FRIO_HUMEDO),
    
    "nutrientes_alta": ("nutrientes", RANGES_ANOMALOS_ALTA),
    "nutrientes_baja": ("nutrientes", RANGES_ANOMALOS_BAJA),
    "nutrientes_exceso": ("nutrientes", RANGES_EXCESO_FERTILIZANTE),
    "nutrientes_lavado": ("nutrientes", RANGES_LAVADO_NUTRIENTES),
    
    "radiacion_alta": ("radiacion", RANGES_ANOMALOS_ALTA),
    "radiacion_baja": ("radiacion", RANGES_ANOMALOS_BAJA),
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
    global sim_mode, sim_pira
    
    current_ranges = RANGES_SANA.copy()

    # Si hay una anomalía activa, buscar en el mapeo y sobreescribir los rangos
    if sim_mode != "sana" and sim_mode in MODOS_CONFIG:
        grupo_afectado, rangos_objetivo = MODOS_CONFIG[sim_mode]
        sensores_del_grupo = GRUPOS_ANOMALIAS.get(grupo_afectado, [])
        
        for sensor in sensores_del_grupo:
            if sensor in rangos_objetivo:
                current_ranges[sensor] = rangos_objetivo[sensor]

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
            "t": round(random.uniform(*current_ranges["dht_temp"]), 1),
            "h": round(random.uniform(*current_ranges["dht_humidity"]), 1),
        },
        "suelo": {
            "N": random.randint(*current_ranges["soil_N"]),
            "P": random.randint(*current_ranges["soil_P"]),
            "K": random.randint(*current_ranges["soil_K"]),
            "hum": round(random.uniform(*current_ranges["soil_humidity"]), 1),
            "temp": round(random.uniform(*current_ranges["soil_temp"]), 1),
            "ec": random.randint(*current_ranges["soil_ec"]),
            "ph": round(random.uniform(*current_ranges["soil_ph"]), 2),
        },
        "bateria": {
            "porcentaje": battery,
        },
    }

    if sim_pira:
        data["piramometro"] = {
            "rad": round(random.uniform(*current_ranges["piramometro_rad"]), 1)
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
    
    pira_str = f"Rad={data['piramometro']['rad']}W/m²" if "piramometro" in data else "Rad=---"
    
    if info.rc == mqtt.MQTT_ERR_SUCCESS:
        print(f"[MQTT] {nodo_id} | T={data['dht']['t']}°C H={data['dht']['h']}% | "
              f"NPK={data['suelo']['N']}/{data['suelo']['P']}/{data['suelo']['K']} | "
              f"HumS={data['suelo']['hum']}% TempS={data['suelo']['temp']}°C | "
              f"EC={data['suelo']['ec']}µS/cm pH={data['suelo']['ph']} | "
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
    print("  --- BASE ---")
    print("  sana                     -> Entorno en parámetros normales.")
    print("  --- AMBIENTE ---")
    print("  ambiente_alta            -> Temperatura y humedad ambiente elevadas.")
    print("  ambiente_baja            -> Temperatura y humedad ambiente bajas.")
    print("  ambiente_caliente_seco   -> Temp Alta + Humedad Baja (Sequía).")
    print("  ambiente_frio_humedo     -> Temp Baja + Humedad Alta (Riesgo de hongos).")
    print("  --- SUELO ---")
    print("  suelo_alta               -> Humedad y temperatura del suelo elevadas.")
    print("  suelo_baja               -> Humedad y temperatura del suelo bajas.")
    print("  suelo_caliente_seco      -> Temp Suelo Alta + Humedad Baja (Suelo árido).")
    print("  suelo_frio_humedo        -> Temp Suelo Baja + Humedad Alta (Encharcamiento).")
    print("  --- NUTRIENTES ---")
    print("  nutrientes_alta          -> NPK, EC y pH por encima de lo normal.")
    print("  nutrientes_baja          -> NPK, EC y pH por debajo de lo normal.")
    print("  nutrientes_exceso        -> Alto NPK, alta EC, bajo pH (Toxicidad/Salinidad).")
    print("  nutrientes_lavado        -> Bajo NPK, baja EC, alto pH (Lixiviación/Suelo inerte).")
    print("  --- RADIACIÓN ---")
    print("  radiacion_alta           -> Radiación solar elevada.")
    print("  radiacion_baja           -> Radiación solar escasa.")
    print("  --- HARDWARE ---")
    print("  pira on / pira off       -> Habilita/Inhabilita el envío del piramómetro.")
    print("  estado                   -> Muestra la configuración actual.")
    print("  salir                    -> Detener el simulador.")
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

    try:
        while True:
            cmd = input().strip().lower()
            
            if cmd == "sana" or cmd in MODOS_CONFIG:
                sim_mode = cmd
                print(f"\n⚙️  [CLI] MODO CAMBIADO: {cmd.upper().replace('_', ' ')}")
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
                opciones_validas = ['sana'] + list(MODOS_CONFIG.keys())
                print(f"[CLI] Comando desconocido. Opciones válidas:\n{', '.join(opciones_validas)}\nO también: pira on, pira off, estado, salir") 
                
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
