"""
AgroTech Dashboard + FastAPI — Compatible con Flet 0.84+
v3.2 — Cambios principales:
- Suscripción simultánea a todos los nodos desde el inicio
- Navegación por ventanas sin scroll táctil
- Horarios personalizables (hora y minuto)
- Historial paginado (10 registros por página)
- NUEVO: Recepción de fotografías por HTTP (reemplaza MQTT+Base64).
- NUEVO: FastAPI + página HTML para consultas y configuración mediante REST.
- WebSocket eliminado: el navegador realiza solicitudes bajo demanda.
  MQTT se conserva para sensores, status y configuración de horario.
- NUEVO: sincronización Raspberry <-> main.py para configuraciones remotas
  de horarios, con versionado, confirmación de publicación y configok.

v12 - CORRECCIONES (firmware nodo2_esp32s3_v12.ino):
  1) mqtt_publicar_config() ahora publica con qos=1 y retain=True y espera
     confirmación real del broker (wait_for_publish). Antes el cliente
     temporal hacía publish()+disconnect() sin loop de red, así que el
     mensaje podía no salir nunca y la UI igual decía "enviada".
     Con retain, el broker guarda el horario y se lo entrega al ESP32
     apenas despierta y se suscribe (el nodo duerme casi todo el tiempo).
  2) La configuración va al tópico por nodo: esp32/<nodo_id>/config.
  3) La app se suscribe a esp32/<nodo_id>/configok y /status para saber
     si el nodo realmente recibió y guardó el horario, y lo muestra en
     la pantalla de configuración.
"""

import flet as ft
import paho.mqtt.client as mqtt
import json
import sqlite3
import threading
import time       
import base64   # v12: espera de la confirmacion del nodo
import urllib.request
import requests
import os
from datetime import datetime
from typing import Optional
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel
import uvicorn


# ══════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ══════════════════════════════════════════════════════════════════════
MQTT_BROKER = "localhost"
MQTT_PORT   = 1883

# NOTA: esta app se conecta al broker por "localhost" porque corre en la
# misma PC donde vive el broker. El ESP32-CAM, en cambio, se conecta por
# la IP LAN de esta PC (10.226.6.61 en el firmware). Si el ESP32 no logra
# conectar por MQTT aunque el WiFi sí conecte, revisa que el broker
# (ej. Mosquitto) tenga un listener en 0.0.0.0 y no solo en 127.0.0.1,
# y que el firewall de esta PC permita el puerto 1883 entrante.

# NUEVO: servidor HTTP que recibe las fotos enviadas por la ESP32-S3 CAM.
# Debe coincidir con PHOTO_SERVER_URL configurado en el firmware del nodo.
HTTP_PHOTO_PORT = 8080
HTTP_PHOTO_PATH = "/upload"

# NUEVO: almacenamiento histórico completo en SQLite.
# JSON se conserva como respaldo y para que la interfaz Flet siga funcionando.
SQLITE_DB = os.path.join(os.path.expanduser("~/agrotech/data"), "agrotech.db")

# NUEVO: servidor FastAPI para la aplicación web y API REST.
FASTAPI_HOST = "0.0.0.0"
FASTAPI_PORT = 8000

# SINCRONIZACIÓN CON SERVIDOR EN LA NUBE
# MQTT sigue siendo exclusivamente ESP32 <-> Raspberry.s
# SQLite conserva una bandera local: enviado=0 pendiente, enviado=1 confirmado.
CLOUD_SERVER_URL = "https://felipegmezm.pythonanywhere.com/"
CLOUD_API_KEY = "cM5STVFFparbVC6RuZmlEYB-B_H4GCSIwbWNulekgFDL3CpLlqRiTIvCCzko6idx"
CLOUD_TIMEOUT = 15
CLOUD_SYNC_INTERVAL = 30

# SINCRONIZACIÓN REMOTA DE HORARIOS
# La nube (main.py) conserva la configuración hasta que la Raspberry
# confirma que la publicó por MQTT. El ESP32 recibe el JSON por MQTT
# en esp32/<nodo_id>/config con retain=True.
CONFIG_CLOUD_SYNC_INTERVAL = 30

AGROTECH_DIR = os.path.expanduser("~/agrotech")
DATA_DIR    = os.path.join(AGROTECH_DIR, "data")
STORAGE_CONFIG_FILE = os.path.join(AGROTECH_DIR, "storage_config.json")
DEFAULT_MAX_STORAGE_MB = 512
DEFAULT_DELETE_COUNT = 100
os.makedirs(DATA_DIR, exist_ok=True)


def cargar_config_almacenamiento():
    cfg = {"max_mb": DEFAULT_MAX_STORAGE_MB, "eliminar_cantidad": DEFAULT_DELETE_COUNT}
    try:
        with open(STORAGE_CONFIG_FILE, "r", encoding="utf-8") as f:
            guardada = json.load(f)
        cfg["max_mb"] = max(100, int(guardada.get("max_mb", cfg["max_mb"])))
        cfg["eliminar_cantidad"] = max(1, int(guardada.get("eliminar_cantidad", cfg["eliminar_cantidad"])))
    except Exception:
        pass
    return cfg


def guardar_config_almacenamiento(max_mb, eliminar_cantidad):
    cfg = {"max_mb": max(100, int(max_mb)), "eliminar_cantidad": max(1, int(eliminar_cantidad))}
    os.makedirs(AGROTECH_DIR, exist_ok=True)
    tmp = STORAGE_CONFIG_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    os.replace(tmp, STORAGE_CONFIG_FILE)
    return cfg


def _tamano_directorio(ruta):
    total = 0
    for raiz, _, archivos in os.walk(ruta):
        for nombre in archivos:
            try: total += os.path.getsize(os.path.join(raiz, nombre))
            except OSError: pass
    return total


def estado_almacenamiento():
    cfg = cargar_config_almacenamiento()
    with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
        datos = conn.execute("SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)),0) FROM mediciones").fetchone()
        fotos = conn.execute("SELECT COUNT(*), COALESCE(SUM(tam_bytes),0) FROM fotos").fetchone()
    return {"carpeta": AGROTECH_DIR, "total_bytes": _tamano_directorio(AGROTECH_DIR),
            "datos_cantidad": datos[0], "datos_payload_bytes": datos[1],
            "fotos_cantidad": fotos[0], "fotos_bytes": fotos[1],
            "max_mb": cfg["max_mb"], "eliminar_cantidad": cfg["eliminar_cantidad"]}


def aplicar_limpieza_almacenamiento():
    """Si ~/agrotech excede el límite, elimina lotes antiguos de datos y fotos."""
    try:
        cfg = cargar_config_almacenamiento()
        limite = cfg["max_mb"] * 1024 * 1024
        if _tamano_directorio(AGROTECH_DIR) <= limite:
            return
        cantidad = cfg["eliminar_cantidad"]
        with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
            # Se eliminan mediciones más antiguas y sus entradas del historial JSON.
            antiguas = conn.execute("SELECT id,nodo_id,timestamp FROM mediciones ORDER BY id ASC LIMIT ?", (cantidad,)).fetchall()
            ids = [r[0] for r in antiguas]
            por_nodo = {}
            for _, nid, ts in antiguas: por_nodo.setdefault(nid, set()).add(ts)
            fotos_asociadas = []
            if ids:
                placeholders = ",".join("?" for _ in ids)
                fotos_asociadas = conn.execute(
                    f"SELECT id,ruta FROM fotos WHERE medicion_id IN ({placeholders})", ids
                ).fetchall()
                conn.executemany("DELETE FROM mediciones WHERE id=?", [(i,) for i in ids])
                for fid, ruta in fotos_asociadas:
                    try:
                        if os.path.isfile(ruta): os.remove(ruta)
                    except OSError as e: print(f"[LIMPIEZA] No se pudo borrar foto asociada {ruta}: {e}")
                    conn.execute("DELETE FROM fotos WHERE id=?", (fid,))
            # Se purgan fotos antiguas adicionales si aún se necesita liberar espacio.
            fotos_antiguas = conn.execute("SELECT id,ruta FROM fotos ORDER BY id ASC LIMIT ?", (cantidad,)).fetchall()
            for fid, ruta in fotos_antiguas:
                try:
                    if os.path.isfile(ruta): os.remove(ruta)
                except OSError as e: print(f"[LIMPIEZA] No se pudo borrar {ruta}: {e}")
                conn.execute("DELETE FROM fotos WHERE id=?", (fid,))
            conn.commit()
        for nid, timestamps in por_nodo.items():
            historial = cargar_historial(nid)
            historial = [r for r in historial if r.get("timestamp") not in timestamps]
            try:
                with open(ruta_historial(nid), "w", encoding="utf-8") as f: json.dump(historial, f, indent=2)
            except Exception as e: print(f"[LIMPIEZA] Error actualizando historial {nid}: {e}")
        # Vaciar páginas libres de SQLite después de eliminar filas.
        with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.execute("VACUUM")
        print(f"[LIMPIEZA] Se purgaron {len(ids)} mediciones, {len(fotos_asociadas)} fotos asociadas y {len(fotos_antiguas)} fotos adicionales.")
    except Exception as e:
        print(f"[LIMPIEZA] Error aplicando límite de almacenamiento: {e}")

NODOS = [
    {"id":"nodo1","nombre":"Nodo 1 · Parcela A","topic":"esp32/nodo1/data","camera_ip":"10.0.0.60","activo":True},
    {"id":"nodo2","nombre":"Nodo 2 · Parcela B","topic":"esp32/nodo2/data","camera_ip":"10.0.0.61","activo":True},
    {"id":"nodo3","nombre":"Nodo 3 · Parcela C","topic":"esp32/nodo3/data","camera_ip":None,"activo":False}, #Aqui se desactivo manualmente el nodo
    {"id":"nodo4","nombre":"Nodo 4 · Reserva","topic":"esp32/nodo4/data","camera_ip":None,"activo":False},   #Aqui se desactivo manualmente el nodo
]

# ══════════════════════════════════════════════════════════════════════
#  PALETA
# ══════════════════════════════════════════════════════════════════════
C = {
    "verde_oscuro":"#1B5E20","verde":"#2E7D32","verde_medio":"#388E3C","verde_claro":"#4CAF50",
    "verde_suave":"#E8F5E9","verde_borde":"#A5D6A7","azul":"#1565C0","azul_suave":"#E3F2FD",
    "azul_borde":"#90CAF9","azul_oscuro":"#0D47A1","naranja":"#E65100","naranja_suave":"#FFF3E0",
    "naranja_borde":"#FFCC80","cafe":"#4E342E","cafe_suave":"#EFEBE9","cafe_borde":"#BCAAA4",
    "purpura":"#6A1B9A","purpura_suave":"#F3E5F5","purpura_borde":"#CE93D8","fondo":"#F1F5F1",
    "blanco":"#FFFFFF","gris_texto":"#757575","gris_borde":"#E0E0E0","negro_texto":"#212121",
    "rojo":"#C62828","amarillo":"#F57F17","amarillo_suave":"#FFFDE7","amarillo_borde":"#FFE082",
}

# ══════════════════════════════════════════════════════════════════════
#  SQLITE — ALMACENAMIENTO HISTÓRICO
# ══════════════════════════════════════════════════════════════════════
def sqlite_iniciar():
    """Inicializa SQLite; las fotos se guardan como archivos y solo se registra su ruta."""
    try:
        os.makedirs(os.path.dirname(SQLITE_DB), exist_ok=True)
        with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS mediciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT, nodo_id TEXT NOT NULL,
                timestamp TEXT NOT NULL, temperatura REAL, humedad REAL,
                nitrogeno REAL, fosforo REAL, potasio REAL, humedad_suelo REAL,
                temperatura_suelo REAL, conductividad REAL, ph REAL, bateria REAL,
                payload TEXT NOT NULL, enviado INTEGER NOT NULL DEFAULT 0)""")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_mediciones_nodo_fecha ON mediciones(nodo_id, timestamp)")
            conn.execute("""CREATE TABLE IF NOT EXISTS configuraciones (
                nodo_id TEXT PRIMARY KEY, horarios TEXT NOT NULL, actualizado TEXT NOT NULL,
                origen TEXT NOT NULL DEFAULT 'Raspberry')""")
            conn.execute("""CREATE TABLE IF NOT EXISTS config_nube_sync (
                nodo_id TEXT PRIMARY KEY, version INTEGER NOT NULL DEFAULT 0,
                horarios TEXT NOT NULL DEFAULT '[]', publicada INTEGER NOT NULL DEFAULT 0,
                aplicada INTEGER NOT NULL DEFAULT 0, actualizado TEXT NOT NULL)""")
            # Migrar fotos antiguas con BLOB a archivos JPEG antes de eliminar la columna BLOB.
            cols = {r[1] for r in conn.execute("PRAGMA table_info(fotos)").fetchall()}
            if cols and "ruta" not in cols:
                legacy = "fotos_legacy_migracion"
                conn.execute(f'ALTER TABLE fotos RENAME TO "{legacy}"')
                conn.execute("""CREATE TABLE fotos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, nodo_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL, ruta TEXT NOT NULL, tam_bytes INTEGER NOT NULL,
                    enviado INTEGER NOT NULL DEFAULT 0, medicion_id INTEGER,
                    UNIQUE(nodo_id,timestamp))""")
                oldcols = {r[1] for r in conn.execute(f'PRAGMA table_info("{legacy}")').fetchall()}
                if {"nodo_id", "timestamp", "imagen"}.issubset(oldcols):
                    extra = ", enviado" if "enviado" in oldcols else ""
                    rows = conn.execute(f'SELECT nodo_id,timestamp,imagen{extra} FROM "{legacy}"').fetchall()
                    for row in rows:
                        nid, ts, blob = row[:3]
                        if not blob: continue
                        nombre = "cam_" + str(nid) + "_" + datetime.now().strftime("%Y%m%d_%H%M%S_%f") + ".jpg"
                        ruta = os.path.join(DATA_DIR, nombre)
                        with open(ruta, "wb") as f: f.write(bytes(blob))
                        enviado = int(row[3]) if len(row) > 3 else 0
                        conn.execute("INSERT OR IGNORE INTO fotos(nodo_id,timestamp,ruta,tam_bytes,enviado) VALUES(?,?,?,?,?)",
                                     (nid, ts, ruta, os.path.getsize(ruta), enviado))
                conn.execute(f'DROP TABLE "{legacy}"')
            else:
                conn.execute("""CREATE TABLE IF NOT EXISTS fotos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, nodo_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL, ruta TEXT NOT NULL, tam_bytes INTEGER NOT NULL,
                    enviado INTEGER NOT NULL DEFAULT 0, medicion_id INTEGER,
                    UNIQUE(nodo_id,timestamp))""")
            cols_fotos = {r[1] for r in conn.execute("PRAGMA table_info(fotos)").fetchall()}
            if "medicion_id" not in cols_fotos:
                conn.execute("ALTER TABLE fotos ADD COLUMN medicion_id INTEGER")
            cols_med = {r[1] for r in conn.execute("PRAGMA table_info(mediciones)").fetchall()}
            if "enviado" not in cols_med:
                conn.execute("ALTER TABLE mediciones ADD COLUMN enviado INTEGER NOT NULL DEFAULT 0")
            cols_cfg = {r[1] for r in conn.execute("PRAGMA table_info(configuraciones)").fetchall()}
            if "origen" not in cols_cfg:
                conn.execute("ALTER TABLE configuraciones ADD COLUMN origen TEXT NOT NULL DEFAULT 'Raspberry'")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_mediciones_enviado ON mediciones(enviado,id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fotos_enviado ON fotos(enviado,nodo_id)")
            sqlite_importar_fotos_carpeta(conn)
            conn.commit()
        print(f"[SQLITE] Base de datos lista: {SQLITE_DB}")
        return True
    except Exception as e:
        print(f"[SQLITE] Error inicializando: {e}")
        return False


def sqlite_importar_fotos_carpeta(conn):
    """Registra rutas de imágenes ya presentes en DATA_DIR sin copiar sus bytes a SQLite."""
    import re
    patron = re.compile(r"^cam_(nodo[^_]+)_(\d{8})_(\d{6})(?:_\d+)?\.jpe?g$", re.IGNORECASE)
    try: nombres = os.listdir(DATA_DIR)
    except Exception as e:
        print(f"[SQLITE] No se pudo revisar carpeta de fotos: {e}")
        return
    for nombre in sorted(nombres):
        m = patron.match(nombre)
        if not m: continue
        nid, fecha, hora = m.groups()[:3]
        try: ts = datetime.strptime(f"{fecha}_{hora}", "%Y%m%d_%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
        except ValueError: continue
        ruta = os.path.join(DATA_DIR, nombre)
        try:
            conn.execute("INSERT OR IGNORE INTO fotos(nodo_id,timestamp,ruta,tam_bytes,enviado) VALUES(?,?,?,?,0)",
                         (nid, ts, ruta, os.path.getsize(ruta)))
        except OSError: pass


def _numero(valor):
    try:
        return float(valor) if isinstance(valor, (int, float)) else None
    except Exception:
        return None


def guardar_sqlite(nodo_id, dato, timestamp):
    """Guarda una medición completa en SQLite sin alterar el JSON existente."""
    try:
        dht = dato.get("dht", {})
        suelo = dato.get("suelo", {})
        bateria = dato.get("bateria", {})

        with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
            conn.execute("""
                INSERT INTO mediciones (
                    nodo_id, timestamp, temperatura, humedad,
                    nitrogeno, fosforo, potasio,
                    humedad_suelo, temperatura_suelo, conductividad,
                    ph, bateria, payload
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                nodo_id,
                timestamp,
                _numero(dht.get("t")),
                _numero(dht.get("h")),
                _numero(suelo.get("N")),
                _numero(suelo.get("P")),
                _numero(suelo.get("K")),
                _numero(suelo.get("hum")),
                _numero(suelo.get("temp")),
                _numero(suelo.get("ec")),
                _numero(suelo.get("ph")),
                _numero(bateria.get("porcentaje")),
                json.dumps(dato, ensure_ascii=False),
            ))
            id_medicion = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.commit()
            return id_medicion
    except Exception as e:
        print(f"[SQLITE] Error guardando medición de {nodo_id}: {e}")
        return None


def sqlite_ultimo(nodo_id):
    """Obtiene la última medición de un nodo para consultas WebSocket."""
    try:
        with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
            row = conn.execute("""
                SELECT timestamp, payload
                FROM mediciones
                WHERE nodo_id = ?
                ORDER BY id DESC
                LIMIT 1
            """, (nodo_id,)).fetchone()
        if not row:
            return None
        dato = json.loads(row[1])
        return {
            "tipo": "lectura",
            "nodo_id": nodo_id,
            "timestamp": row[0],
            **dato,
        }
    except Exception as e:
        print(f"[SQLITE] Error consultando último dato: {e}")
        return None


def sqlite_historial(nodo_id, limite=100):
    """Obtiene historial desde SQLite, sin depender del límite de 500 del JSON."""
    try:
        limite = max(1, min(int(limite), 5000))
    except (TypeError, ValueError):
        limite = 100

    try:
        with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
            rows = conn.execute("""
                SELECT timestamp, payload
                FROM mediciones
                WHERE nodo_id = ?
                ORDER BY id DESC
                LIMIT ?
            """, (nodo_id, limite)).fetchall()

        resultado = []
        for timestamp, payload in rows:
            dato = json.loads(payload)
            resultado.append({
                "timestamp": timestamp,
                **dato,
            })
        return resultado
    except Exception as e:
        print(f"[SQLITE] Error consultando historial: {e}")
        return []



# ══════════════════════════════════════════════════════════════════════
#  SINCRONIZACIÓN HTTP CON EL SERVIDOR EN LA NUBE
# ══════════════════════════════════════════════════════════════════════
_sync_event = threading.Event()

def _cloud_url(path):
    return CLOUD_SERVER_URL.rstrip("/") + path

def _cloud_headers():
    return {
        "X-API-Key": CLOUD_API_KEY,
        "Content-Type": "application/json",
    }

def sqlite_mediciones_pendientes(limite=100):
    with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
        return conn.execute("""
            SELECT id, nodo_id, timestamp, payload
            FROM mediciones
            WHERE enviado = 0
            ORDER BY id ASC
            LIMIT ?
        """, (limite,)).fetchall()

def sqlite_marcar_medicion_enviada(id_medicion):
    with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
        conn.execute(
            "UPDATE mediciones SET enviado = 1 WHERE id = ?",
            (id_medicion,)
        )
        conn.commit()

def sqlite_fotos_pendientes(limite=100):
    with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
        return conn.execute("""
            SELECT id, nodo_id, timestamp, ruta, tam_bytes
            FROM fotos WHERE enviado = 0 ORDER BY id ASC LIMIT ?
        """, (limite,)).fetchall()


def sqlite_marcar_foto_enviada(id_foto):
    with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
        conn.execute(
            "UPDATE fotos SET enviado = 1 WHERE id = ?",
            (id_foto,)
        )
        conn.commit()

def solicitar_sincronizacion():
    _sync_event.set()

def sincronizar_con_nube():
    """Envía solo pendientes y confirma cada elemento individualmente."""
    if not CLOUD_SERVER_URL or "TU-DOMINIO" in CLOUD_SERVER_URL:
        return

    for id_local, nodo_id, timestamp, payload in sqlite_mediciones_pendientes():
        try:
            datos_originales = json.loads(payload)
            # La bandera es estrictamente local y nunca viaja a la nube.
            datos_originales.pop("enviado", None)

            # El ESP32/Raspberry guarda localmente solo dht/suelo/bateria,
            # pero la API de la nube exige además tipo, nodo_id y timestamp.
            # Construimos aquí el formato del contrato de la nube sin alterar
            # el payload original almacenado en SQLite.
            datos = {
                "tipo": "lectura",
                "nodo_id": nodo_id,
                "timestamp": timestamp,
                **datos_originales,
            }

            numero_nodo = nodo_id.replace("nodo", "", 1)
            respuesta = requests.post(
                _cloud_url(f"/api/nodos/{numero_nodo}/datos"),
                json=datos,
                headers=_cloud_headers(),
                timeout=CLOUD_TIMEOUT
            )

            if not respuesta.ok:
                print(f"[SYNC] HTTP {respuesta.status_code} medición {id_local}: {respuesta.text[:300]}")
                break

            cuerpo = respuesta.json()
            if cuerpo.get("ok") is True:
                sqlite_marcar_medicion_enviada(id_local)
                print(f"[SYNC] Medición local {id_local} confirmada por nube.")
            else:
                print(f"[SYNC] Nube rechazó medición {id_local}: {cuerpo}")
                break

        except Exception as e:
            print(f"[SYNC] Error enviando medición {id_local}: {e}")
            break

    for id_foto, nodo_id, timestamp, ruta_foto, tam_bytes in sqlite_fotos_pendientes():
        try:
            with open(ruta_foto, "rb") as archivo_foto:
                imagen = archivo_foto.read()
            numero_nodo = nodo_id.replace("nodo", "", 1)
            respuesta = requests.post(
                _cloud_url(f"/api/nodos/{numero_nodo}/foto"),
                params={"timestamp": timestamp},
                data=imagen,
                headers={
                    "X-API-Key": CLOUD_API_KEY,
                    "Content-Type": "image/jpeg",
                    "X-Foto-Timestamp": timestamp,
                },
                timeout=CLOUD_TIMEOUT
            )

            if not respuesta.ok:
                print(f"[SYNC] HTTP {respuesta.status_code} foto {nodo_id} (id {id_foto}): {respuesta.text[:300]}")
                break

            cuerpo = respuesta.json()
            if cuerpo.get("ok") is True:
                sqlite_marcar_foto_enviada(id_foto)
                print(f"[SYNC] Foto local {id_foto} de {nodo_id} confirmada por nube.")
            else:
                print(f"[SYNC] Nube rechazó foto local {id_foto} de {nodo_id}: {cuerpo}")
                break

        except Exception as e:
            print(f"[SYNC] Error enviando foto local {id_foto} de {nodo_id}: {e}")
            break

def sincronizador_iniciar():
    def _run():
        while True:
            _sync_event.wait(timeout=CLOUD_SYNC_INTERVAL)
            _sync_event.clear()
            try:
                sincronizar_con_nube()
            except Exception as e:
                print(f"[SYNC] Error general: {e}")

    threading.Thread(
        target=_run,
        daemon=True,
        name="agrotech-cloud-sync"
    ).start()

    # Primera ejecución al arrancar: recupera todo lo que quedó pendiente,
    # incluido el último dato de cada ESP y las fotografías.
    solicitar_sincronizacion()

# ══════════════════════════════════════════════════════════════════════
#  PERSISTENCIA DE DATOS
# ══════════════════════════════════════════════════════════════════════
def ruta_historial(nodo_id):
    return os.path.join(DATA_DIR, f"{nodo_id}.json")

def ruta_config(nodo_id):
    return os.path.join(DATA_DIR, f"{nodo_id}_config.json")

def guardar_dato(nodo_id, dato):
    """Añade un registro al historial del nodo con timestamp."""
    ruta = ruta_historial(nodo_id)
    try:
        if os.path.exists(ruta):
            with open(ruta, "r") as f:
                historial = json.load(f)
        else:
            historial = []
        if not dato.get("timestamp"):
            dato["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        historial.append(dato)
        # Guardar máximo 500 registros por nodo
        if len(historial) > 500:
            historial = historial[-500:]
        with open(ruta, "w") as f:
            json.dump(historial, f, indent=2)
        return dato.get("timestamp")
    except Exception as e:
        print(f"[DATA] Error guardando: {e}")
        return None

def cargar_historial(nodo_id):
    """Carga el historial de un nodo desde disco."""
    ruta = ruta_historial(nodo_id)
    try:
        if os.path.exists(ruta):
            with open(ruta, "r") as f:
                return json.load(f)
    except Exception as e:
        print(f"[DATA] Error cargando: {e}")
    return []

def cargar_ultimo_dato(nodo_id):
    """Carga el último dato guardado de un nodo."""
    historial = cargar_historial(nodo_id)
    if historial:
        return historial[-1]
    return None

def _normalizar_horario(valor):
    """Convierte un horario antiguo (ej. 10) o nuevo (ej. '10:33') a HH:MM."""
    try:
        if isinstance(valor, str) and ":" in valor:
            h, m = valor.split(":", 1)
            h, m = int(h), int(m)
        else:
            h, m = int(valor), 0
        if 0 <= h <= 23 and 0 <= m <= 59:
            return f"{h:02d}:{m:02d}"
    except (TypeError, ValueError):
        pass
    return None


def _normalizar_horarios(valores):
    """Normaliza una lista de horarios y elimina entradas inválidas."""
    resultado = []
    for valor in valores or []:
        horario = _normalizar_horario(valor)
        if horario is not None:
            resultado.append(horario)
    return resultado


def _minutos_desde_medianoche(horario):
    """Convierte HH:MM a minutos desde medianoche para ordenar."""
    h, m = map(int, horario.split(":"))
    return h * 60 + m


def guardar_config_sqlite(nodo_id, horarios_list, origen="Raspberry (interfaz local)"):
    """Guarda la configuración de horarios en SQLite para la API REST."""
    horarios = _normalizar_horarios(horarios_list)
    horarios = sorted(set(horarios), key=_minutos_desde_medianoche)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
            conn.execute("""
                INSERT INTO configuraciones (nodo_id, horarios, actualizado, origen)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(nodo_id) DO UPDATE SET
                    horarios=excluded.horarios,
                    actualizado=excluded.actualizado,
                    origen=excluded.origen
            """, (nodo_id, json.dumps(horarios), timestamp, origen))
            conn.commit()
        return True
    except Exception as e:
        print(f"[SQLITE] Error guardando configuración de {nodo_id}: {e}")
        return False


def sqlite_config(nodo_id):
    """Obtiene la configuración de horarios desde SQLite; usa JSON como respaldo."""
    try:
        with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
            row = conn.execute("""
                SELECT horarios, actualizado, origen
                FROM configuraciones
                WHERE nodo_id = ?
            """, (nodo_id,)).fetchone()
        if row:
            horarios = _normalizar_horarios(json.loads(row[0]))
            return {"horas": sorted(set(horarios), key=_minutos_desde_medianoche), "actualizado": row[1], "origen": row[2] if len(row) > 2 else "Raspberry (configuración local)"}
    except Exception as e:
        print(f"[SQLITE] Error consultando configuración de {nodo_id}: {e}")
    return cargar_config_nodo(nodo_id)


def guardar_config_nodo(nodo_id, horarios_list):
    """Guarda la configuración de horarios con precisión de minuto."""
    try:
        horarios = _normalizar_horarios(horarios_list)
        with open(ruta_config(nodo_id), "w") as f:
            json.dump({
                "horas": horarios,
                "actualizado": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }, f)
    except Exception as e:
        print(f"[CONFIG] Error guardando: {e}")


def cargar_config_nodo(nodo_id):
    """Carga la configuración guardada y migra automáticamente horarios antiguos HH:00."""
    try:
        if os.path.exists(ruta_config(nodo_id)):
            with open(ruta_config(nodo_id), "r") as f:
                cfg = json.load(f)
            cfg["horas"] = _normalizar_horarios(cfg.get("horas", [10, 18]))
            return cfg
    except Exception:
        pass
    return {"horas": ["10:00", "18:00"], "actualizado": "—"}

# ══════════════════════════════════════════════════════════════════════
#  HELPERS DE FORMATO (nivel módulo, usados en init y en MQTT)
# ══════════════════════════════════════════════════════════════════════
def _fi(v):
    return int(v) if isinstance(v, (int, float)) else None

def _ff(v):
    return round(float(v), 2) if isinstance(v, (int, float)) else None

def _fmt_dht(dato):
    d = dato.get("dht", {})
    return {
        "t": f'{d["t"]:.1f}' if isinstance(d.get("t"), (int, float)) else "--",
        "h": f'{d["h"]:.1f}' if isinstance(d.get("h"), (int, float)) else "--",
    }

def _fmt_suelo(dato):
    s = dato.get("suelo", {})
    return {
        "N":   str(_fi(s.get("N")))   if _fi(s.get("N"))   is not None else "--",
        "P":   str(_fi(s.get("P")))   if _fi(s.get("P"))   is not None else "--",
        "K":   str(_fi(s.get("K")))   if _fi(s.get("K"))   is not None else "--",
        "hum": f'{_ff(s.get("hum")):.1f}'  if _ff(s.get("hum"))  is not None else "--",
        "temp":f'{_ff(s.get("temp")):.1f}' if _ff(s.get("temp")) is not None else "--",
        "ec":  str(_fi(s.get("ec")))  if _fi(s.get("ec"))  is not None else "--",
        "ph":  f'{_ff(s.get("ph")):.2f}'   if _ff(s.get("ph"))   is not None else "--",
    }

def _fmt_bateria(dato):
    b = dato.get("bateria", {})
    p = b.get("porcentaje")
    return {"porcentaje": f"{p:.1f}" if isinstance(p, (int, float)) else "--"}

# ══════════════════════════════════════════════════════════════════════
#  ESTADO GLOBAL — DATOS DE TODOS LOS NODOS
#  Se pre-carga desde disco para que la UI arranque con el último dato
#  conocido aunque MQTT aún no haya entregado nada nuevo.
# ══════════════════════════════════════════════════════════════════════
estado_nodos = {}
for nodo in NODOS:
    _ultimo = cargar_ultimo_dato(nodo["id"])
    if _ultimo:
        estado_nodos[nodo["id"]] = {
            "dht":    _fmt_dht(_ultimo),
            "suelo":  _fmt_suelo(_ultimo),
            "bateria":_fmt_bateria(_ultimo),
            "ultima_actualizacion": datetime.now(),
            # Campos de imagen — se llenan al recibir foto vía HTTP
            "ultima_imagen_b64": None,
            "ultima_imagen_ts":  None,
            "imagen_gen": 0,   # contador; sube cada vez que llega foto nueva
        }
    else:
        estado_nodos[nodo["id"]] = {
            "dht":    {"t":"--","h":"--"},
            "suelo":  {"N":"--","P":"--","K":"--","hum":"--","temp":"--","ec":"--","ph":"--"},
            "bateria":{"porcentaje":"--"},
            "ultima_actualizacion": None,
            "ultima_imagen_b64": None,
            "ultima_imagen_ts":  None,
            "imagen_gen": 0,
        }

# Recupera la última fotografía desde el archivo al iniciar, no desde SQLite.
for _nodo in NODOS:
    _nid = _nodo["id"]
    try:
        _candidatas = [os.path.join(DATA_DIR, n) for n in os.listdir(DATA_DIR)
                       if n.startswith(f"cam_{_nid}_") and n.lower().endswith((".jpg", ".jpeg"))]
        if _candidatas:
            _ruta = max(_candidatas, key=os.path.getmtime)
            with open(_ruta, "rb") as _f:
                estado_nodos[_nid]["ultima_imagen_b64"] = base64.b64encode(_f.read()).decode("ascii")
            estado_nodos[_nid]["ultima_imagen_ruta"] = _ruta
            estado_nodos[_nid]["ultima_imagen_ts"] = datetime.fromtimestamp(os.path.getmtime(_ruta)).strftime("%H:%M:%S")
            estado_nodos[_nid]["imagen_gen"] = 1
    except Exception as _e:
        print(f"[CAM] No se pudo precargar foto de {_nid}: {_e}")

estado_mqtt = {"conectado": False}
# v12: última confirmación recibida de cada nodo (tópico esp32/<id>/configok)
# y último status ("hola", "chao", "config_on", "config_off").
estado_config_ack = {}
estado_status_nodo = {}
estado_config_horarios = {}
_mqtt_client: Optional[mqtt.Client] = None
_camara_activa = threading.Event()

# Callbacks registrados por las pantallas activas
_callbacks_actualizacion = []

def registrar_callback(callback):
    """Registra un callback que será llamado cuando lleguen datos MQTT o una foto por HTTP."""
    if callback not in _callbacks_actualizacion:
        _callbacks_actualizacion.append(callback)

def desregistrar_callback(callback):
    """Elimina un callback de la lista."""
    if callback in _callbacks_actualizacion:
        _callbacks_actualizacion.remove(callback)

def notificar_actualizacion():
    """Notifica a todos los callbacks registrados."""
    for callback in _callbacks_actualizacion:
        try:
            callback()
        except Exception as e:
            print(f"[CALLBACK] Error: {e}")

# ══════════════════════════════════════════════════════════════════════
#  MQTT — Suscripción a TODOS los nodos activos
#  (sensores, status y configuración de horario — NO fotos)
# ══════════════════════════════════════════════════════════════════════
def mqtt_iniciar_global(on_update):
    """Inicia MQTT y se suscribe a TODOS los nodos activos simultáneamente."""
    global _mqtt_client
    mqtt_detener()

    # Crear diccionario topic -> nodo_id para identificar de dónde vienen los datos
    topic_to_nodo = {}
    for nodo in NODOS:
        if nodo["activo"]:
            topic_to_nodo[nodo["topic"]] = nodo["id"]

    def _on_connect(client, userdata, flags, rc, props=None):
        estado_mqtt["conectado"] = (rc == 0)
        if rc == 0:
            # Suscribirse a TODOS los nodos activos
            for topic in topic_to_nodo.keys():
                client.subscribe(topic)
                print(f"[MQTT] Suscrito a {topic}")
            # v12: además del tópico de datos, escuchamos la confirmación de
            # horario y el status de cada nodo activo.
            for nodo in NODOS:
                if nodo["activo"]:
                    client.subscribe(f'esp32/{nodo["id"]}/config')
                    client.subscribe(f'esp32/{nodo["id"]}/configok')
                    client.subscribe(f'esp32/{nodo["id"]}/status')
                    print(f'[MQTT] Suscrito a esp32/{nodo["id"]}/config, /configok y /status')
            # Leer también horarios retenidos de nodos actualmente desactivados en la UI.
            for nodo in NODOS:
                if not nodo["activo"]:
                    client.subscribe(f'esp32/{nodo["id"]}/config')
                    print(f'[MQTT] Suscrito a esp32/{nodo["id"]}/config (nodo inactivo)')
            # NOTA: ya NO nos suscribimos a cam/start, cam/data, cam/end.
            # Las fotos ahora llegan por HTTP (ver http_photo_server_iniciar()),
            # no por MQTT+Base64.
        on_update()
        notificar_actualizacion()

    def _on_disconnect(client, userdata, rc, props=None, reason=None):
        estado_mqtt["conectado"] = False
        on_update()
        notificar_actualizacion()

    def _on_message(client, userdata, msg):
        """Procesa mensajes de datos de cualquier nodo (sensores/status)."""
        try:
            texto = msg.payload.decode("utf-8", errors="ignore")

            # Horario retenido que el broker entrega al suscribirse.
            if msg.topic.endswith("/config"):
                nid = msg.topic.split("/")[1]
                try:
                    cfg_payload = json.loads(texto)
                    horarios_topic = _horarios_desde_payload_config(cfg_payload)
                    if horarios_topic:
                        cfg_guardada = sqlite_config(nid)
                        estado_config_horarios[nid] = {
                            "horarios": sorted(set(horarios_topic), key=_minutos_desde_medianoche),
                            "origen": cfg_payload.get("origen") or cfg_guardada.get("origen") or "Tópico MQTT retenido (origen no identificado)",
                            "actualizado": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        print(f"[MQTT] Horario retenido de {nid}: {horarios_topic}")
                        notificar_actualizacion()
                except Exception as e:
                    print(f"[MQTT] Configuración retenida inválida en {msg.topic}: {e}")
                return

            # ── v12: confirmación de horario enviada por el ESP32 ──
            if msg.topic.endswith("/configok"):
                nid = msg.topic.split("/")[1]
                try:
                    ack = json.loads(texto)
                except Exception:
                    ack = {"raw": texto}
                ack["recibido"] = datetime.now().strftime("%H:%M:%S")
                estado_config_ack[nid] = ack
                print(f"[MQTT] CONFIRMACION de {nid}: {texto}")
                # La confirmación del ESP32 cierra el ciclo:
                # main.py -> Raspberry -> MQTT -> ESP32 -> MQTT -> main.py
                _procesar_configok_nube(nid, ack)
                notificar_actualizacion()
                on_update()
                return

            # ── v12: status del nodo (hola / chao / config_on / config_off) ──
            if msg.topic.endswith("/status"):
                nid = msg.topic.split("/")[1]
                estado_status_nodo[nid] = {
                    "valor": texto,
                    "hora": datetime.now().strftime("%H:%M:%S"),
                }
                print(f"[MQTT] status {nid}: {texto}")
                return

            # Identificar de qué nodo viene el mensaje
            nodo_id = topic_to_nodo.get(msg.topic)
            if not nodo_id:
                return  # Topic desconocido

            data = json.loads(texto)
            registro = {}
            timestamp_medicion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Actualizar estado del nodo correspondiente
            if "dht" in data:
                d = data["dht"]
                t = d.get("t", d.get("temp"))
                h = d.get("h", d.get("hum"))
                estado_nodos[nodo_id]["dht"]["t"] = f"{t:.1f}" if isinstance(t,(int,float)) else "--"
                estado_nodos[nodo_id]["dht"]["h"] = f"{h:.1f}" if isinstance(h,(int,float)) else "--"
                registro["dht"] = {"t": t, "h": h}

            if "suelo" in data:
                s = data["suelo"]
                estado_nodos[nodo_id]["suelo"] = {
                    "N":   str(_fi(s.get("N"))) if _fi(s.get("N")) is not None else "--",
                    "P":   str(_fi(s.get("P"))) if _fi(s.get("P")) is not None else "--",
                    "K":   str(_fi(s.get("K"))) if _fi(s.get("K")) is not None else "--",
                    "hum": f'{_ff(s.get("hum")):.1f}' if _ff(s.get("hum")) is not None else "--",
                    "temp":f'{_ff(s.get("temp")):.1f}' if _ff(s.get("temp")) is not None else "--",
                    "ec":  str(_fi(s.get("ec"))) if _fi(s.get("ec")) is not None else "--",
                    "ph":  f'{_ff(s.get("ph")):.2f}' if _ff(s.get("ph")) is not None else "--",
                }
                registro["suelo"] = s

            if "bateria" in data:
                b = data["bateria"]
                p = b.get("porcentaje")
                estado_nodos[nodo_id]["bateria"]["porcentaje"] = f"{p:.1f}" if isinstance(p,(int,float)) else "--"
                registro["bateria"] = b

            # Guardar en historial si hay datos
            if registro:
                registro["timestamp"] = timestamp_medicion

                # Se conservan las dos persistencias:
                # 1) JSON existente para la interfaz Flet
                # 2) SQLite para historial completo y consultas externas
                guardar_dato(nodo_id, registro)
                id_medicion = guardar_sqlite(nodo_id, registro, timestamp_medicion)
                aplicar_limpieza_almacenamiento()
                if id_medicion is not None:
                    solicitar_sincronizacion()

                estado_nodos[nodo_id]["ultima_actualizacion"] = datetime.now()

            # Notificar a todos los callbacks registrados
            notificar_actualizacion()
            on_update()
        except Exception as e:
            print(f"[MQTT] Error procesando mensaje: {e}")

    try:
        _mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        _mqtt_client.on_connect    = _on_connect
        _mqtt_client.on_disconnect = _on_disconnect
        _mqtt_client.on_message    = _on_message
        _mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        _mqtt_client.loop_start()
    except Exception as e:
        print(f"[MQTT] No se pudo conectar: {e}")
        estado_mqtt["conectado"] = False
        on_update()

def mqtt_detener():
    global _mqtt_client
    if _mqtt_client:
        try: _mqtt_client.loop_stop(); _mqtt_client.disconnect()
        except: pass
        _mqtt_client = None
    estado_mqtt["conectado"] = False

def mqtt_publicar_config(payload: str, nodo_id: str = "nodo2") -> bool:
    """Publica la configuración de horario para un nodo concreto.

    v12 - qué cambió y por qué:
      * Tópico por nodo: esp32/<nodo_id>/config (antes era el global
        esp32/config, que le llegaba a todos los nodos por igual).
        El firmware v12 se suscribe a ambos, así que hay compatibilidad.
      * retain=True: el ESP32 pasa casi todo el tiempo en deep sleep. Un
        mensaje normal QoS 0 se pierde si nadie está suscrito en ese
        instante. Con retain, el broker guarda el último horario y se lo
        entrega al nodo en cuanto despierta y se suscribe.
      * loop_start() + wait_for_publish(): paho solo ENCOLA en publish().
        Sin loop de red, el disconnect() inmediato del cliente temporal
        cortaba la conexión antes de que el paquete saliera, y la función
        devolvía True igual. Por eso la UI decía "enviada" sin enviar nada.
    """
    global _mqtt_client
    topic = f"esp32/{nodo_id}/config"
    try:
        if _mqtt_client and _mqtt_client.is_connected():
            info = _mqtt_client.publish(topic, payload, qos=1, retain=True)
            info.wait_for_publish(timeout=5)
            ok = info.is_published()
            print(f"[CONFIG] Publicado en {topic}: {payload} -> {ok}")
            return ok

        # Conexión temporal, esta vez con loop de red activo
        tmp = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        tmp.connect(MQTT_BROKER, MQTT_PORT, keepalive=10)
        tmp.loop_start()
        info = tmp.publish(topic, payload, qos=1, retain=True)
        info.wait_for_publish(timeout=5)
        ok = info.is_published()
        tmp.loop_stop()
        tmp.disconnect()
        print(f"[CONFIG] Publicado (cliente temporal) en {topic}: {payload} -> {ok}")
        return ok
    except Exception as e:
        print(f"[CONFIG] Error publicando: {e}")
        return False


def mqtt_publicar_hora(nodo_id: str = "nodo2") -> bool:
    """v13: publica la hora actual de la PC en esp32/<nodo_id>/time.

    IMPORTANTE: SIN retain. Antes la fecha viajaba dentro del horario
    retenido; el broker se la reenviaba al ESP32 en cada despertar y el
    firmware reajustaba el DS3231 a esa fecha VIEJA, por eso el nodo nunca
    llegaba a la hora programada. Este mensaje solo lo recibe un nodo que
    esté despierto (modo config); si está dormido se pierde, y está bien.
    """
    global _mqtt_client
    topic = f"esp32/{nodo_id}/time"
    payload = json.dumps({"fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
    try:
        if _mqtt_client and _mqtt_client.is_connected():
            info = _mqtt_client.publish(topic, payload, qos=1, retain=False)
            info.wait_for_publish(timeout=5)
            print(f"[CONFIG] Hora publicada en {topic}: {payload}")
            return info.is_published()
        return False
    except Exception as e:
        print(f"[CONFIG] Error publicando hora: {e}")
        return False


def mqtt_borrar_config_retenida(nodo_id: str = "nodo2") -> bool:
    """Borra el mensaje retenido de configuración de un nodo.

    Útil si quieres que el ESP32 deje de recibir el horario viejo al
    despertar: se publica un payload vacío con retain=True, que es la forma
    que define MQTT para eliminar un retenido.
    """
    global _mqtt_client
    topic = f"esp32/{nodo_id}/config"
    try:
        if _mqtt_client and _mqtt_client.is_connected():
            _mqtt_client.publish(topic, "", qos=1, retain=True)
            print(f"[CONFIG] Retenido borrado en {topic}")
            return True
        return False
    except Exception as e:
        print(f"[CONFIG] Error borrando retenido: {e}")
        return False

# ══════════════════════════════════════════════════════════════════════
#  SINCRONIZACIÓN DE CONFIGURACIONES DE HORARIO CON LA NUBE
# ══════════════════════════════════════════════════════════════════════
_config_sync_event = threading.Event()
_config_sync_lock = threading.Lock()


def _horarios_desde_payload_config(valor):
    """Convierte la configuración remota a una lista HH:MM.

    Acepta tanto el formato que entrega main.py:
        ["06:00", "12:30"]
    como el formato compacto que usa el ESP32:
        {"horas": [6, 12], "minutos": [0, 30]}
    """
    try:
        if isinstance(valor, dict):
            if isinstance(valor.get("horarios"), list):
                return _normalizar_horarios(valor.get("horarios"))

            horas = valor.get("horas")
            minutos = valor.get("minutos")
            if isinstance(horas, list) and isinstance(minutos, list):
                if len(horas) != len(minutos):
                    return []
                return _normalizar_horarios([
                    f"{int(h):02d}:{int(m):02d}"
                    for h, m in zip(horas, minutos)
                ])

        if isinstance(valor, list):
            return _normalizar_horarios(valor)
    except (TypeError, ValueError):
        pass
    return []


def _config_nube_guardar_local(nodo_id, version, horarios, publicada=False, aplicada=False):
    """Guarda localmente qué versión de main.py se publicó al ESP32."""
    try:
        horarios = _normalizar_horarios(horarios)
        with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
            conn.execute("""
                INSERT INTO config_nube_sync
                    (nodo_id, version, horarios, publicada, aplicada, actualizado)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(nodo_id) DO UPDATE SET
                    version=excluded.version,
                    horarios=excluded.horarios,
                    publicada=excluded.publicada,
                    aplicada=excluded.aplicada,
                    actualizado=excluded.actualizado
            """, (
                nodo_id,
                int(version),
                json.dumps(horarios),
                1 if publicada else 0,
                1 if aplicada else 0,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ))
            conn.commit()
        return True
    except Exception as e:
        print(f"[CONFIG-CLOUD] Error guardando estado local de {nodo_id}: {e}")
        return False


def _config_nube_obtener_local(nodo_id):
    try:
        with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
            row = conn.execute("""
                SELECT version, horarios, publicada, aplicada
                FROM config_nube_sync
                WHERE nodo_id = ?
            """, (nodo_id,)).fetchone()
        if not row:
            return None
        return {
            "version": row[0],
            "horarios": _normalizar_horarios(json.loads(row[1] or "[]")),
            "publicada": bool(row[2]),
            "aplicada": bool(row[3]),
        }
    except Exception as e:
        print(f"[CONFIG-CLOUD] Error leyendo estado local de {nodo_id}: {e}")
        return None


def _config_cloud_confirmar(nodo_numero, version, estado, detalle=""):
    """Informa a main.py que la Raspberry publicó o que el ESP aplicó."""
    try:
        respuesta = requests.post(
            _cloud_url(f"/api/nodos/{nodo_numero}/configuracion/confirmar"),
            json={
                "version": int(version),
                "estado": estado,
                "detalle": str(detalle)[:300],
            },
            headers=_cloud_headers(),
            timeout=CLOUD_TIMEOUT,
        )
        if not respuesta.ok:
            print(
                f"[CONFIG-CLOUD] HTTP {respuesta.status_code} confirmando "
                f"nodo{nodo_numero} v{version}: {respuesta.text[:300]}"
            )
            return False

        cuerpo = respuesta.json()
        if cuerpo.get("ok") is not True:
            print(f"[CONFIG-CLOUD] Nube rechazó confirmación: {cuerpo}")
            return False

        print(
            f"[CONFIG-CLOUD] Confirmación '{estado}' enviada para "
            f"nodo{nodo_numero}, versión {version}."
        )
        return True
    except Exception as e:
        print(
            f"[CONFIG-CLOUD] Error confirmando nodo{nodo_numero} "
            f"v{version}: {e}"
        )
        return False


def sincronizar_configuraciones_con_nube():
    """Descarga configuraciones pendientes de main.py y las traduce a JSON ESP32.

    Flujo:
      1. main.py entrega {nodo, version, horarios:[HH:MM,...]}.
      2. Raspberry convierte a {horas:[...], minutos:[...]}.
      3. Publica en esp32/<nodo_id>/config con QoS 1 + retain=True.
      4. Solo si MQTT confirma la publicación, main.py recibe 'publicada'.
      5. El ESP32 puede aplicar el horario al despertar y responder /configok.
      6. Al recibir /configok, la Raspberry confirma 'aplicada' a main.py.
    """
    if not CLOUD_SERVER_URL or "TU-DOMINIO" in CLOUD_SERVER_URL:
        return

    try:
        respuesta = requests.get(
            _cloud_url("/api/configuracion/pendientes"),
            headers={"X-API-Key": CLOUD_API_KEY},
            timeout=CLOUD_TIMEOUT,
        )
        if not respuesta.ok:
            print(
                f"[CONFIG-CLOUD] HTTP {respuesta.status_code} consultando "
                f"pendientes: {respuesta.text[:300]}"
            )
            return

        cuerpo = respuesta.json()
        pendientes = cuerpo.get("pendientes", [])
        if not pendientes:
            return

        print(f"[CONFIG-CLOUD] {len(pendientes)} configuración(es) pendiente(s).")

        for pendiente in pendientes:
            try:
                nodo_numero = int(pendiente["nodo"])
                nodo_id = str(pendiente.get("nodo_id") or f"nodo{nodo_numero}")
                version = int(pendiente["version"])
                horarios = _horarios_desde_payload_config(pendiente.get("horarios", []))

                if nodo_numero not in (1, 2, 3, 4) or not horarios:
                    print(
                        f"[CONFIG-CLOUD] Configuración inválida para {nodo_id}: "
                        f"{pendiente}"
                    )
                    continue

                payload = json.dumps({
                    "horas": [int(h.split(":")[0]) for h in horarios],
                    "minutos": [int(h.split(":")[1]) for h in horarios],
                    "origen": "Base de datos en la nube",
                })

                # Si main.py tiene una versión nueva, reemplazamos la anterior
                # en la cola local. El mensaje MQTT retain también queda con la
                # versión más reciente cuando se publica correctamente.
                with _config_sync_lock:
                    _config_nube_guardar_local(
                        nodo_id,
                        version,
                        horarios,
                        publicada=False,
                        aplicada=False,
                    )

                print(
                    f"[CONFIG-CLOUD] nodo{nodo_numero} v{version}: "
                    f"{horarios} -> {payload}"
                )

                # mqtt_publicar_config() usa QoS 1 y retain=True.
                # Esto permite que un ESP dormido reciba el último horario
                # cuando vuelva a conectarse al broker.
                publicado = mqtt_publicar_config(payload, nodo_id)

                if not publicado:
                    print(
                        f"[CONFIG-CLOUD] No se pudo publicar nodo{nodo_numero} "
                        f"v{version}; seguirá pendiente en main.py."
                    )
                    continue

                _config_nube_guardar_local(
                    nodo_id,
                    version,
                    horarios,
                    publicada=True,
                    aplicada=False,
                )
                guardar_config_nodo(nodo_id, horarios)
                guardar_config_sqlite(nodo_id, horarios, "Base de datos en la nube")

                # Sincroniza el RTC solo si el nodo está despierto; el mensaje
                # NO es retenido y por tanto no puede reajustar el reloj con una
                # fecha antigua cuando el ESP despierte horas después.
                mqtt_publicar_hora(nodo_id)

                _config_cloud_confirmar(
                    nodo_numero,
                    version,
                    "publicada",
                    "Configuración publicada por MQTT con QoS 1 y retain=True",
                )

            except Exception as e:
                print(f"[CONFIG-CLOUD] Error procesando configuración: {e}")

    except requests.RequestException as e:
        print(f"[CONFIG-CLOUD] Sin conexión con main.py: {e}")
    except Exception as e:
        print(f"[CONFIG-CLOUD] Error general: {e}")


def solicitar_sincronizacion_configuraciones():
    _config_sync_event.set()


def _procesar_configok_nube(nodo_id, ack):
    """Cuando el ESP32 confirma, marca la versión correspondiente en main.py."""
    horarios_ack = _horarios_desde_payload_config(ack)
    if not horarios_ack:
        print(f"[CONFIG-CLOUD] configok de {nodo_id} no contiene horarios válidos: {ack}")
        return

    local = _config_nube_obtener_local(nodo_id)
    if not local:
        print(
            f"[CONFIG-CLOUD] configok de {nodo_id} recibido, pero no hay "
            "versión local asociada. No se marca aplicada en la nube."
        )
        return

    if horarios_ack != local["horarios"]:
        print(
            f"[CONFIG-CLOUD] configok de {nodo_id} NO coincide con v{local['version']}. "
            f"Esperado={local['horarios']} recibido={horarios_ack}."
        )
        return

    if not local["publicada"]:
        print(
            f"[CONFIG-CLOUD] configok de {nodo_id} coincide, pero la versión "
            "aún no figura como publicada localmente."
        )
        return

    nodo_numero = int(nodo_id.replace("nodo", "", 1))
    if _config_cloud_confirmar(
        nodo_numero,
        local["version"],
        "aplicada",
        "El ESP32 confirmó por MQTT que guardó el horario recibido",
    ):
        _config_nube_guardar_local(
            nodo_id,
            local["version"],
            local["horarios"],
            publicada=True,
            aplicada=True,
        )


def sincronizador_configuraciones_iniciar():
    """Hilo periódico: consulta main.py cada 30 s y al arrancar."""
    def _run():
        while True:
            _config_sync_event.wait(timeout=CONFIG_CLOUD_SYNC_INTERVAL)
            _config_sync_event.clear()
            try:
                sincronizar_configuraciones_con_nube()
            except Exception as e:
                print(f"[CONFIG-CLOUD] Error en ciclo: {e}")

    threading.Thread(
        target=_run,
        daemon=True,
        name="agrotech-config-cloud-sync",
    ).start()

    # Recupera inmediatamente una configuración que haya quedado pendiente
    # mientras la Raspberry estaba apagada.
    solicitar_sincronizacion_configuraciones()


# ══════════════════════════════════════════════════════════════════════
#  CÁMARA
#  Las imágenes se reciben y guardan SIEMPRE, estés o no dentro del
#  dashboard del nodo. La UI se actualiza si los controles están activos;
#  de lo contrario, los datos quedan en estado_nodos y se muestran la
#  próxima vez que el usuario abra ese dashboard.
#
#  CAMBIO: el transporte ya no es MQTT+Base64 fragmentado, sino un POST
#  HTTP binario que llega al servidor definido más abajo. La función
#  _procesar_imagen_recibida() concentra la lógica común de guardado y
#  actualización de UI, sin importar de dónde vino la imagen.
# ══════════════════════════════════════════════════════════════════════
_cam_buffer = {
    "nodo_id":  None,   # qué nodo está enviando fotos (dashboard activo)
    "img_ctrl": None,   # ft.Image activo (solo si el dashboard está abierto)
    "txt_ctrl": None,
    "page":     None,
}

_CAM_PLACEHOLDER = "data:image/gif;base64,R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw=="

def camara_iniciar_mqtt(img_ctrl, txt_ctrl, page, nodo_id):
    """Registra los controles de UI del dashboard activo para actualizaciones en vivo.
    (El nombre se conserva por compatibilidad; ya no depende de MQTT)."""
    _cam_buffer["nodo_id"]  = nodo_id
    _cam_buffer["img_ctrl"] = img_ctrl
    _cam_buffer["txt_ctrl"] = txt_ctrl
    _cam_buffer["page"]     = page
    print(f"[CAM] Dashboard activo para {nodo_id} — listo para recibir fotos por HTTP")

def camara_detener():
    """Limpia los refs de UI pero conserva nodo_id para seguir guardando fotos."""
    _cam_buffer["img_ctrl"] = None
    _cam_buffer["txt_ctrl"] = None
    _cam_buffer["page"]     = None
    # nodo_id se conserva para que _procesar_imagen_recibida sepa a qué nodo
    # asignar la imagen si llega mientras el dashboard está cerrado.

def _normalizar_nodo_id(nodo_param: str) -> str:
    """
    El firmware manda algo como nodo=nodo2_prueba. Aquí lo mapeamos al id
    real usado en NODOS (nodo1, nodo2, ...), quitando sufijos tipo "_prueba".
    """
    ids_validos = {n["id"] for n in NODOS}
    if nodo_param in ids_validos:
        return nodo_param
    base = nodo_param.split("_")[0]
    if base in ids_validos:
        return base
    return nodo_param  # no reconocido; se guarda igual pero se avisa en consola

def _procesar_imagen_recibida(nodo_id: str, img_bytes: bytes):
    """Guarda la foto como archivo y almacena en SQLite únicamente su ruta."""
    try:
        ahora = datetime.now()
        timestamp_foto = ahora.strftime("%Y-%m-%d %H:%M:%S")
        nombre = f"cam_{nodo_id}_{ahora.strftime('%Y%m%d_%H%M%S_%f')}.jpg"
        ruta_jpg = os.path.join(DATA_DIR, nombre)
        with open(ruta_jpg, "wb") as f:
            f.write(img_bytes)
        hora = ahora.strftime("%H:%M:%S")
        with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
            med = conn.execute("SELECT id FROM mediciones WHERE nodo_id=? ORDER BY id DESC LIMIT 1", (nodo_id,)).fetchone()
            med_id = med[0] if med else None
            conn.execute("""INSERT OR IGNORE INTO fotos
                (nodo_id,timestamp,ruta,tam_bytes,enviado,medicion_id) VALUES(?,?,?,?,0,?)""",
                (nodo_id, timestamp_foto, ruta_jpg, len(img_bytes), med_id))
            conn.commit()
        solicitar_sincronizacion()
        if nodo_id in estado_nodos:
            estado_nodos[nodo_id]["ultima_imagen_b64"] = base64.b64encode(img_bytes).decode("ascii")
            estado_nodos[nodo_id]["ultima_imagen_ts"] = hora
            estado_nodos[nodo_id]["ultima_imagen_ruta"] = ruta_jpg
            estado_nodos[nodo_id]["imagen_gen"] = estado_nodos[nodo_id].get("imagen_gen", 0) + 1
        ctrl, txt, pg = _cam_buffer.get("img_ctrl"), _cam_buffer.get("txt_ctrl"), _cam_buffer.get("page")
        if ctrl and pg and _cam_buffer.get("nodo_id") == nodo_id:
            ctrl.src = f"data:image/jpeg;base64,{base64.b64encode(img_bytes).decode('ascii')}"
            if txt:
                txt.value = f"Última foto: {hora} — {len(img_bytes)/1024:.1f} KB"
                txt.italic = False
            try: pg.update()
            except Exception: pass
        notificar_actualizacion()
        aplicar_limpieza_almacenamiento()
    except Exception as e:
        print(f"[CAM] Error procesando imagen: {e}")

class _PhotoUploadHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # silencia el log default de http.server; usamos nuestros prints

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != HTTP_PHOTO_PATH:
            self.send_response(404)
            self.end_headers()
            return

        qs = parse_qs(parsed.query)
        nodo_param = qs.get("nodo", ["unknown"])[0]
        nodo_id = _normalizar_nodo_id(nodo_param)

        length = int(self.headers.get("Content-Length", 0))
        if length <= 0:
            self.send_response(400)
            self.end_headers()
            return

        img_bytes = self.rfile.read(length)
        print(f"[HTTP] Foto recibida de nodo='{nodo_param}' -> '{nodo_id}' ({length} bytes)")

        if nodo_id not in estado_nodos:
            print(f"[HTTP] Advertencia: '{nodo_id}' no está en NODOS, la foto no se asocia a ningún dashboard")

        _procesar_imagen_recibida(nodo_id, img_bytes)

        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

def http_photo_server_iniciar():
    """Levanta el servidor HTTP de fotos en un hilo daemon aparte."""
    def _run():
        try:
            server = ThreadingHTTPServer(("0.0.0.0", HTTP_PHOTO_PORT), _PhotoUploadHandler)
            print(f"[HTTP] Servidor de fotos escuchando en 0.0.0.0:{HTTP_PHOTO_PORT}{HTTP_PHOTO_PATH}")
            server.serve_forever()
        except Exception as e:
            print(f"[HTTP] No se pudo iniciar el servidor de fotos: {e}")

    hilo = threading.Thread(target=_run, daemon=True)
    hilo.start()

# ══════════════════════════════════════════════════════════════════════
#  FASTAPI — APLICACIÓN WEB + API REST
#  No usa WebSocket, SSE ni polling automático.
#  El navegador solicita datos solamente cuando el usuario pulsa una acción.
# ══════════════════════════════════════════════════════════════════════
app = FastAPI(title="AgroTech API", version="1.0")


class ConfigHorarioAPI(BaseModel):
    horarios: list[str]


HTML_AGROTECH = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AgroTech · API REST</title>
<style>
:root { --verde:#2E7D32; --verde2:#1B5E20; --fondo:#F1F5F1; --borde:#D7E2D7; --texto:#212121; --gris:#6B7280; --rojo:#C62828; }
*{box-sizing:border-box} body{margin:0;font-family:Arial,sans-serif;background:var(--fondo);color:var(--texto)}
header{background:linear-gradient(135deg,var(--verde),var(--verde2));color:white;padding:18px 24px;box-shadow:0 2px 8px #0002}
header h1{margin:0;font-size:24px} header p{margin:5px 0 0;opacity:.82;font-size:13px}
main{max-width:1180px;margin:20px auto;padding:0 14px}.toolbar,.card{background:white;border:1px solid var(--borde);border-radius:12px;padding:16px;box-shadow:0 2px 8px #0000000d;margin-bottom:14px}
.toolbar{display:flex;gap:10px;align-items:end;flex-wrap:wrap}.field{display:flex;flex-direction:column;gap:5px}.field label{font-size:12px;color:var(--gris);font-weight:bold}
select,input,button{font:inherit;border-radius:8px;border:1px solid #CBD5CB;padding:9px 11px}button{cursor:pointer;background:var(--verde);color:white;border:none;font-weight:bold}button.secondary{background:#607D60}button.danger{background:var(--rojo)}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}.metric{background:#F7FAF7;border:1px solid var(--borde);border-radius:9px;padding:10px}.metric small{display:block;color:var(--gris);font-size:11px}.metric strong{font-size:18px;color:var(--verde)}
img{max-width:100%;width:100%;max-height:430px;object-fit:contain;border-radius:9px;background:#eee;min-height:180px}.status{font-size:12px;color:var(--gris);min-height:18px}.ok{color:var(--verde)}.error{color:var(--rojo)}
.horarios{display:flex;gap:8px;flex-wrap:wrap}.horario{display:flex;gap:5px;align-items:center;background:#F7FAF7;padding:7px;border-radius:8px;border:1px solid var(--borde)}
.horario button{padding:5px 8px;background:var(--rojo)}.acciones{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:7px;border-bottom:1px solid #E5E7EB;text-align:center}th{background:var(--verde);color:white;position:sticky;top:0}.table-wrap{overflow:auto;max-height:380px}
@media(max-width:850px){.grid{grid-template-columns:1fr}.metrics{grid-template-columns:repeat(2,1fr)}}
</style>
</head>
<body>
<header><h1>🌱 AgroTech</h1><p>Monitoreo agrícola · interfaz web basada en API REST</p></header>
<main>
<section class="toolbar">
  <div class="field"><label>Nodo</label><select id="nodo"></select></div>
  <button onclick="cargarDatos()">Actualizar datos</button>
  <button onclick="cargarFoto()">Solicitar fotografía</button>
  <button class="secondary" onclick="cargarHistorial()">Consultar historial</button>
</section>
<div id="estado" class="status"></div>
<section class="grid">
  <div class="card"><h3>Última medición</h3><div id="metricas" class="metrics"><div class="metric"><small>Temperatura</small><strong>--</strong></div></div></div>
  <div class="card"><h3>Última fotografía</h3><img id="foto" alt="Última fotografía del nodo"><div id="fotoInfo" class="status">Pulsa «Solicitar fotografía» para consultar el archivo guardado.</div></div>
</section>
<section class="card"><h3>Configuración de horarios</h3><div id="horarios" class="horarios"></div><div class="acciones"><button onclick="agregarHorario()">+ Añadir horario</button><button onclick="guardarHorarios()">Enviar configuración</button></div><div id="configEstado" class="status"></div><div id="horariosActuales" class="status"></div></section>
<section class="card"><h3>Historial</h3><div id="historial" class="table-wrap">Pulsa «Consultar historial».</div></section>
</main>
<script>
let horarios=[];
const $=id=>document.getElementById(id);
function nodo(){return $('nodo').value}
function setEstado(msg,ok=false){$('estado').textContent=msg;$('estado').className='status '+(ok?'ok':'')}
async function api(url,opts={}){const r=await fetch(url,opts);if(!r.ok){let d={};try{d=await r.json()}catch{};throw new Error(d.detail||`HTTP ${r.status}`)}return r}
async function cargarNodos(){const d=await (await api('/api/nodos')).json();$('nodo').innerHTML=d.nodos.map(n=>`<option value="${n.id}">${n.nombre}</option>`).join('');await cargarConfig();await cargarDatos()}
function mostrarMetricas(d){const s=d.suelo||{},a=d.dht||{},b=d.bateria||{};$('metricas').innerHTML=[['Temperatura',a.t,'°C'],['Humedad',a.h,'%'],['N',s.N,'mg/kg'],['P',s.P,'mg/kg'],['K',s.K,'mg/kg'],['Hum. suelo',s.hum,'%'],['Temp. suelo',s.temp,'°C'],['EC',s.ec,'µS/cm'],['pH',s.ph,''],['Batería',b.porcentaje,'%']].map(x=>`<div class="metric"><small>${x[0]}</small><strong>${x[1]??'--'} ${x[2]}</strong></div>`).join('')}
async function cargarDatos(){try{const d=await (await api(`/api/nodos/${nodo()}/ultimo`)).json();mostrarMetricas(d);setEstado(`Última lectura: ${d.timestamp}`,true)}catch(e){setEstado(e.message)}}
async function cargarFoto(){try{$('foto').src=`/api/nodos/${nodo()}/foto?t=${Date.now()}`;const r=await api(`/api/nodos/${nodo()}/foto/meta`);const d=await r.json();$('fotoInfo').textContent=`Última foto: ${d.timestamp} · ${Math.round(d.tam_bytes/1024)} KB`;setEstado('Fotografía consultada desde SQLite.',true)}catch(e){$('foto').removeAttribute('src');$('fotoInfo').textContent=e.message;setEstado(e.message)}}
function renderHorarios(){horarios.forEach((h,i)=>{});$('horarios').innerHTML=horarios.map((h,i)=>`<div class="horario"><input type="time" value="${h}" onchange="horarios[${i}]=this.value"><button onclick="horarios.splice(${i},1);renderHorarios()">×</button></div>`).join('')}
function agregarHorario(){let usados=new Set(horarios),v='00:00';for(let m=0;m<1440;m++){v=String(Math.floor(m/60)).padStart(2,'0')+':'+String(m%60).padStart(2,'0');if(!usados.has(v))break}horarios.push(v);horarios.sort();renderHorarios()}
async function cargarConfig(){try{const d=await (await api(`/api/nodos/${nodo()}/config`)).json();horarios=d.horarios||[];renderHorarios();$('configEstado').textContent=`Última actualización: ${d.actualizado||'—'}`;$('horariosActuales').textContent=`Horas configuradas actualmente: ${horarios.join(', ')||'sin horarios'} · Horas configuradas desde: ${d.origen||'origen desconocido'}`}catch(e){$('configEstado').textContent=e.message}}
async function guardarHorarios(){const unicos=[...new Set(horarios.filter(Boolean))].sort();if(!unicos.length){$('configEstado').textContent='Debe existir al menos un horario.';return}if(unicos.length!==horarios.length){$('configEstado').textContent='No se permiten horarios duplicados.';return}try{const d=await (await api(`/api/nodos/${nodo()}/config`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({horarios:unicos})})).json();horarios=d.horarios;renderHorarios();$('configEstado').textContent=`Configuración enviada por MQTT: ${horarios.join(', ')}`;$('horariosActuales').textContent=`Horas configuradas actualmente: ${horarios.join(', ')} · Horas configuradas desde: Interfaz web local`;setEstado('Configuración guardada en SQLite y publicada por MQTT.',true)}catch(e){$('configEstado').textContent=e.message}}
async function cargarHistorial(){try{const d=await (await api(`/api/nodos/${nodo()}/historial?limite=100`)).json();if(!d.datos.length){$('historial').textContent='Sin registros.';return}let h='<table><thead><tr><th>Fecha</th><th>T.Amb</th><th>H.Amb</th><th>N</th><th>P</th><th>K</th><th>H.Suelo</th><th>T.Suelo</th><th>EC</th><th>pH</th><th>Bat.</th></tr></thead><tbody>';for(const r of d.datos){const a=r.dht||{},s=r.suelo||{},b=r.bateria||{};h+=`<tr><td>${r.timestamp||'—'}</td><td>${a.t??'—'}</td><td>${a.h??'—'}</td><td>${s.N??'—'}</td><td>${s.P??'—'}</td><td>${s.K??'—'}</td><td>${s.hum??'—'}</td><td>${s.temp??'—'}</td><td>${s.ec??'—'}</td><td>${s.ph??'—'}</td><td>${b.porcentaje??'—'}</td></tr>`}$('historial').innerHTML=h+'</tbody></table>';setEstado(`${d.cantidad} registros consultados desde SQLite.`,true)}catch(e){$('historial').textContent=e.message}}
$('nodo').addEventListener('change',()=>{cargarConfig();$('historial').textContent='Pulsa «Consultar historial».';$('foto').removeAttribute('src')});
cargarNodos();
</script>
</body></html>"""

@app.get("/", response_class=HTMLResponse)
def web_inicio():
    return HTML_AGROTECH

@app.get("/api/nodos")
def api_nodos():
    return {"nodos":[{"id":n["id"],"nombre":n["nombre"],"activo":n["activo"],"camera":n.get("camera_ip") is not None} for n in NODOS]}


def _validar_nodo(nodo_id):
    if nodo_id not in {n["id"] for n in NODOS}:
        raise HTTPException(status_code=404, detail=f"Nodo no encontrado: {nodo_id}")


@app.get("/api/nodos/{nodo_id}/ultimo")
def api_ultimo(nodo_id: str):
    _validar_nodo(nodo_id)
    dato = sqlite_ultimo(nodo_id)
    if dato is None:
        raise HTTPException(status_code=404, detail="No existen mediciones para este nodo")
    return dato


@app.get("/api/nodos/{nodo_id}/historial")
def api_historial(nodo_id: str, limite: int = 100):
    _validar_nodo(nodo_id)
    limite = max(1, min(limite, 5000))
    datos = sqlite_historial(nodo_id, limite)
    return {"nodo_id": nodo_id, "cantidad": len(datos), "datos": datos}


def _foto_sqlite(nodo_id):
    """Devuelve los metadatos de la última foto, sin leer bytes desde SQLite."""
    with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
        return conn.execute("""SELECT timestamp,ruta,tam_bytes FROM fotos
            WHERE nodo_id=? ORDER BY id DESC LIMIT 1""", (nodo_id,)).fetchone()


@app.get("/api/nodos/{nodo_id}/foto")
def api_foto(nodo_id: str):
    _validar_nodo(nodo_id)
    row = _foto_sqlite(nodo_id)
    if row is None or not os.path.isfile(row[1]):
        raise HTTPException(status_code=404, detail="El nodo todavía no tiene fotografías")
    with open(row[1], "rb") as f:
        imagen = f.read()
    return Response(content=imagen, media_type="image/jpeg", headers={
        "X-Foto-Timestamp": row[0], "X-Foto-Tamano": str(row[2])})


@app.get("/api/nodos/{nodo_id}/foto/meta")
def api_foto_meta(nodo_id: str):
    _validar_nodo(nodo_id)
    row = _foto_sqlite(nodo_id)
    if row is None:
        raise HTTPException(status_code=404, detail="El nodo todavía no tiene fotografías")
    return {"nodo_id": nodo_id, "timestamp": row[0], "tam_bytes": row[2], "ruta": row[1]}


@app.get("/api/nodos/{nodo_id}/fotos/historial")
def api_fotos_historial(nodo_id: str, limite: int = 100):
    _validar_nodo(nodo_id)
    limite = max(1, min(int(limite), 5000))
    with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
        rows = conn.execute("""SELECT id,timestamp,ruta,tam_bytes,enviado FROM fotos
            WHERE nodo_id=? ORDER BY id DESC LIMIT ?""", (nodo_id, limite)).fetchall()
    return {"nodo_id": nodo_id, "cantidad": len(rows), "fotos": [
        {"id": r[0], "timestamp": r[1], "ruta": r[2], "tam_bytes": r[3],
         "enviado": r[4], "url": f"/api/nodos/{nodo_id}/fotos/{r[0]}"}
        for r in rows]}


@app.get("/api/nodos/{nodo_id}/fotos/{id_foto}")
def api_foto_historica(nodo_id: str, id_foto: int):
    _validar_nodo(nodo_id)
    with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
        row = conn.execute("""SELECT timestamp,ruta,tam_bytes FROM fotos
            WHERE id=? AND nodo_id=?""", (id_foto, nodo_id)).fetchone()
    if row is None or not os.path.isfile(row[1]):
        raise HTTPException(status_code=404, detail="Fotografía no encontrada")
    with open(row[1], "rb") as f: imagen = f.read()
    return Response(content=imagen, media_type="image/jpeg", headers={
        "X-Foto-Timestamp": row[0], "X-Foto-Tamano": str(row[2])})


@app.get("/api/almacenamiento")
def api_almacenamiento():
    return estado_almacenamiento()


class ConfigAlmacenamientoAPI(BaseModel):
    max_mb: int
    eliminar_cantidad: int


@app.post("/api/almacenamiento")
def api_guardar_almacenamiento(config: ConfigAlmacenamientoAPI):
    if config.max_mb < 100 or config.eliminar_cantidad < 1:
        raise HTTPException(status_code=400, detail="El límite mínimo es 100 MB y el lote debe ser mayor que cero.")
    guardar_config_almacenamiento(config.max_mb, config.eliminar_cantidad)
    aplicar_limpieza_almacenamiento()
    return estado_almacenamiento()


@app.get("/api/nodos/{nodo_id}/config")
def api_config_get(nodo_id: str):
    _validar_nodo(nodo_id)
    cfg = sqlite_config(nodo_id)
    topic_cfg = estado_config_horarios.get(nodo_id)
    if topic_cfg:
        cfg = {"horas": topic_cfg["horarios"], "actualizado": topic_cfg["actualizado"], "origen": topic_cfg["origen"]}
    with sqlite3.connect(SQLITE_DB, timeout=10) as conn:
        row = conn.execute("SELECT origen FROM configuraciones WHERE nodo_id=?", (nodo_id,)).fetchone()
    origen = topic_cfg["origen"] if topic_cfg else (row[0] if row else cfg.get("origen", "Raspberry (configuración local)"))
    return {"nodo_id": nodo_id, "horarios": _normalizar_horarios(cfg.get("horas", [])), "actualizado": cfg.get("actualizado", "—"), "origen": origen}


@app.post("/api/nodos/{nodo_id}/config")
def api_config_post(nodo_id: str, config: ConfigHorarioAPI):
    _validar_nodo(nodo_id)
    horarios = _normalizar_horarios(config.horarios)
    horarios = sorted(set(horarios), key=_minutos_desde_medianoche)
    if not horarios:
        raise HTTPException(status_code=400, detail="No se proporcionaron horarios válidos")
    if len(horarios) > 1440:
        raise HTTPException(status_code=400, detail="No se pueden superar 1440 horarios")

    payload = json.dumps({
        "horas": [int(h.split(":")[0]) for h in horarios],
        "minutos": [int(h.split(":")[1]) for h in horarios],
        "origen": "Interfaz web local",
    })
    ok = mqtt_publicar_config(payload, nodo_id)
    mqtt_publicar_hora(nodo_id)
    if not ok:
        raise HTTPException(status_code=503, detail="No se pudo publicar la configuración mediante MQTT")

    guardar_config_nodo(nodo_id, horarios)
    if not guardar_config_sqlite(nodo_id, horarios, "Interfaz web local"):
        raise HTTPException(status_code=500, detail="La configuración fue enviada por MQTT, pero no pudo guardarse en SQLite")
    estado_config_horarios[nodo_id] = {"horarios": horarios, "origen": "Interfaz web local",
        "actualizado": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

    return {"ok": True, "nodo_id": nodo_id, "horarios": horarios, "mqtt": "enviado", "actualizado": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}


def fastapi_iniciar():
    """Levanta FastAPI en un hilo daemon para convivir con Flet."""
    def _run():
        try:
            print(f"[API] FastAPI escuchando en http://{FASTAPI_HOST}:{FASTAPI_PORT}")
            uvicorn.run(app, host=FASTAPI_HOST, port=FASTAPI_PORT, log_level="info")
        except Exception as e:
            print(f"[API] No se pudo iniciar FastAPI: {e}")
    hilo = threading.Thread(target=_run, daemon=True)
    hilo.start()


# ══════════════════════════════════════════════════════════════════════
#  COMPONENTES REUTILIZABLES
# ══════════════════════════════════════════════════════════════════════
def sensor_card(label, unidad, ctrl, icono, color):
    return ft.Container(
        border_radius=8, bgcolor=C["blanco"],
        padding=ft.Padding.symmetric(horizontal=8, vertical=6),
        shadow=ft.BoxShadow(blur_radius=4, color=ft.Colors.with_opacity(0.08,ft.Colors.BLACK), offset=ft.Offset(0,1)),
        content=ft.Column([
            ft.Row([ft.Icon(icono,color=color,size=12),
                    ft.Text(label,size=9,color=C["gris_texto"],weight=ft.FontWeight.W_500)],
                   spacing=3, vertical_alignment=ft.CrossAxisAlignment.CENTER),
            ft.Row([ctrl, ft.Text(unidad,size=9,color=C["gris_texto"])],
                   spacing=3, vertical_alignment=ft.CrossAxisAlignment.END),
        ], spacing=3))

def sec_header(icono, titulo, subtitulo, color):
    return ft.Row([
        ft.Container(width=28,height=28,border_radius=7,
            bgcolor=ft.Colors.with_opacity(0.15,color),
            content=ft.Icon(icono,color=color,size=16),
            alignment=ft.Alignment(0,0)),
        ft.Column([ft.Text(titulo,size=12,weight=ft.FontWeight.BOLD,color=C["negro_texto"]),
                   ft.Text(subtitulo,size=9,color=C["gris_texto"])], spacing=0),
    ], spacing=8, vertical_alignment=ft.CrossAxisAlignment.CENTER)

def chip(texto, color):
    return ft.Container(
        content=ft.Text(texto,size=9,color=color,weight=ft.FontWeight.W_600),
        padding=ft.Padding.symmetric(horizontal=8,vertical=3), border_radius=20,
        bgcolor=ft.Colors.with_opacity(0.12,color),
        border=ft.Border.all(1,ft.Colors.with_opacity(0.3,color)))

def barra_superior(titulo, on_back, extras=None):
    items = [
        ft.IconButton(ft.Icons.ARROW_BACK, icon_color=ft.Colors.WHITE, on_click=on_back),
        ft.Icon(ft.Icons.ECO, color=C["verde_claro"], size=20),
        ft.Text(titulo, color=ft.Colors.WHITE, size=15, weight=ft.FontWeight.W_600),
        ft.Container(expand=True),
    ]
    if extras:
        items += extras
    return ft.Container(bgcolor=C["verde"], padding=ft.Padding.symmetric(horizontal=8,vertical=8),
        content=ft.Row(items + [ft.Container(width=8)],
                       spacing=6, vertical_alignment=ft.CrossAxisAlignment.CENTER))

# ══════════════════════════════════════════════════════════════════════
#  PANTALLA 1 — Inicio
# ══════════════════════════════════════════════════════════════════════
def build_inicio(on_inicio):
    return ft.Container(expand=True,
        gradient=ft.LinearGradient(begin=ft.Alignment(-1,-1), end=ft.Alignment(1,1),
            colors=[C["verde"], C["verde_oscuro"], "#0A3D12"]),
        content=ft.Stack([
            ft.Container(top=-40,left=-40,
                content=ft.Icon(ft.Icons.ECO,color=ft.Colors.with_opacity(0.06,ft.Colors.WHITE),size=300)),
            ft.Container(bottom=-30,right=-30,
                content=ft.Icon(ft.Icons.ECO,color=ft.Colors.with_opacity(0.12,ft.Colors.WHITE),size=200)),
            ft.Container(top=60,right=20,
                content=ft.Icon(ft.Icons.GRASS,color=ft.Colors.with_opacity(0.08,ft.Colors.WHITE),size=160)),
            ft.Container(expand=True, alignment=ft.Alignment(0,0),
                content=ft.Column([
                    ft.Container(width=110,height=110,border_radius=55,
                        bgcolor=ft.Colors.with_opacity(0.2,ft.Colors.WHITE),
                        border=ft.Border.all(2,ft.Colors.with_opacity(0.35,ft.Colors.WHITE)),
                        content=ft.Icon(ft.Icons.ECO,size=60,color=ft.Colors.WHITE),
                        alignment=ft.Alignment(0,0)),
                    ft.Container(height=20),
                    ft.Text("AgroTech",size=44,weight=ft.FontWeight.BOLD,
                            color=ft.Colors.WHITE,text_align=ft.TextAlign.CENTER),
                    ft.Text("Sistema de Monitoreo Agrícola",size=14,italic=True,
                            color=ft.Colors.with_opacity(0.75,ft.Colors.WHITE),
                            text_align=ft.TextAlign.CENTER),
                    ft.Container(height=36),
                    ft.FilledButton(
                        content=ft.Row([
                            ft.Icon(ft.Icons.PLAY_CIRCLE_FILLED_OUTLINED,color=C["verde"],size=20),
                            ft.Text("INICIO",size=15,weight=ft.FontWeight.BOLD,color=C["verde"]),
                        ], spacing=10, alignment=ft.MainAxisAlignment.CENTER),
                        style=ft.ButtonStyle(bgcolor=ft.Colors.WHITE,
                            shape=ft.RoundedRectangleBorder(radius=40),
                            padding=ft.Padding.symmetric(horizontal=32,vertical=12),elevation=10),
                        on_click=on_inicio, width=220),
                    ft.Container(height=20),
                    ft.Text("v3.1 · Quindío, Colombia",size=11,
                            color=ft.Colors.with_opacity(0.35,ft.Colors.WHITE)),
                ], alignment=ft.MainAxisAlignment.CENTER,
                   horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=8)),
        ], expand=True))

# ══════════════════════════════════════════════════════════════════════
#  PANTALLA 2 — SELECCIÓN DE NODO

# ══════════════════════════════════════════════════════════════════════
def build_nodos(on_volver, on_sel):
    def card(nodo):
        activo=nodo["activo"]; tiene_cam=nodo.get("camera_ip") is not None
        cb=C["verde_claro"] if activo else C["gris_borde"]
        ci=C["verde"] if activo else C["gris_texto"]
        ct=C["negro_texto"] if activo else C["gris_texto"]
        cd=C["verde_claro"] if activo else "#9E9E9E"
        cfg = sqlite_config(nodo["id"])
        horas_list = cfg.get("horas", [10, 18])
        num_medidas = len(horas_list)

        return ft.Container(border_radius=16,bgcolor=C["blanco"],border=ft.Border.all(1.5,cb),
            padding=ft.Padding.all(16),ink=activo,
            on_click=(lambda e,n=nodo: on_sel(n)) if activo else None,
            shadow=ft.BoxShadow(blur_radius=10,color=ft.Colors.with_opacity(0.08,ft.Colors.BLACK),
                offset=ft.Offset(0,3)) if activo else None,
            content=ft.Column([
                ft.Row([
                    ft.Container(width=36,height=36,border_radius=10,
                        bgcolor=ft.Colors.with_opacity(0.12,ci),
                        content=ft.Icon(ft.Icons.MEMORY,color=ci,size=20),
                        alignment=ft.Alignment(0,0)),
                    ft.Container(expand=True),
                    ft.Container(width=8,height=8,border_radius=4,bgcolor=cd),
                    ft.Text("En línea" if activo else "Desconectado",size=11,color=cd,weight=ft.FontWeight.W_500),
                ], spacing=6, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                ft.Container(height=8),
                ft.Text(nodo["nombre"],size=14,weight=ft.FontWeight.W_700,color=ct),
                ft.Container(height=3),
                ft.Row([ft.Icon(ft.Icons.WIFI_TETHERING,color=C["gris_texto"],size=12),
                        ft.Text(nodo["topic"],size=10,color=C["gris_texto"])], spacing=4),
                ft.Row([ft.Icon(ft.Icons.SCHEDULE,color=C["verde"],size=12),
                        ft.Text(f'{num_medidas} medidas/día',size=10,color=C["verde"])], spacing=4),
                ft.Container(height=8),
                ft.Row([chip("S.Ambiente",C["naranja"]),chip("S.Suelo",C["verde_medio"]),
                        chip("Cam",C["cafe"]) if tiene_cam else ft.Container()], spacing=6,wrap=True),
            ], spacing=0))

    def stat(v, label, icono, color):
        return ft.Container(border_radius=12,bgcolor=C["blanco"],
            padding=ft.Padding.symmetric(horizontal=14,vertical=10),
            border=ft.Border.all(1,C["gris_borde"]),
            content=ft.Row([ft.Icon(icono,color=color,size=18),
                ft.Column([ft.Text(str(v),size=18,weight=ft.FontWeight.BOLD,color=color),
                           ft.Text(label,size=10,color=C["gris_texto"])], spacing=0)],
                spacing=10,vertical_alignment=ft.CrossAxisAlignment.CENTER))

    total=len(NODOS); online=sum(1 for n in NODOS if n["activo"])
    cards=[ft.Container(col={"xs":12,"sm":6,"md":4,"lg":3},content=card(n)) for n in NODOS]

    return ft.Column([
        barra_superior("AgroTech", on_back=on_volver),
        ft.Container(expand=True,bgcolor=C["fondo"],padding=ft.Padding.symmetric(horizontal=16,vertical=14),
            content=ft.Column([
                ft.Text("Selección de Nodo",size=22,weight=ft.FontWeight.BOLD,color=C["negro_texto"]),
                ft.Text("Elige el nodo del que deseas visualizar los datos.",size=12,color=C["gris_texto"]),
                ft.Divider(color=C["gris_borde"],height=1),
                ft.Row([stat(total,"Total",ft.Icons.HUB,C["azul"]),
                        stat(online,"En línea",ft.Icons.SENSORS,C["verde"]),
                        stat(total-online,"Offline",ft.Icons.SENSORS_OFF,C["rojo"])], spacing=10),
                ft.Container(height=6),
                ft.ResponsiveRow(controls=cards,spacing=14,run_spacing=14),
            ], expand=True, spacing=8)),
    ], spacing=0, expand=True)

# ══════════════════════════════════════════════════════════════════════
#  PANTALLA 3 — DASHBOARD CON NAVEGACIÓN POR VENTANAS
# ══════════════════════════════════════════════════════════════════════
def build_dashboard(nodo, page, on_volver, on_config, on_historico):
    nodo_id = nodo["id"]
    tiene_cam = nodo.get("camera_ip") is not None

    # Controles de valor - TAMAÑOS REDUCIDOS
    vt=ft.Text("--",size=16,weight=ft.FontWeight.BOLD,color=C["naranja"])
    vh=ft.Text("--",size=16,weight=ft.FontWeight.BOLD,color=C["azul"])
    vN=ft.Text("--",size=14,weight=ft.FontWeight.BOLD,color=C["verde_medio"])
    vP=ft.Text("--",size=14,weight=ft.FontWeight.BOLD,color=C["verde_medio"])
    vK=ft.Text("--",size=14,weight=ft.FontWeight.BOLD,color=C["verde_medio"])
    vSH=ft.Text("--",size=14,weight=ft.FontWeight.BOLD,color=C["azul"])
    vST=ft.Text("--",size=14,weight=ft.FontWeight.BOLD,color=C["naranja"])
    vEC=ft.Text("--",size=14,weight=ft.FontWeight.BOLD,color=C["cafe"])
    vPH=ft.Text("--",size=14,weight=ft.FontWeight.BOLD,color=C["purpura"])
    vBAT=ft.Text("--",size=14,weight=ft.FontWeight.BOLD,color=C["verde"])
    dot=ft.Container(width=7,height=7,border_radius=4,bgcolor=C["rojo"])
    txt_m=ft.Text("Conectando...",size=9,color=C["gris_texto"])
    img_c=ft.Image(src=_CAM_PLACEHOLDER,fit="contain",border_radius=8,width=500)
    txt_c=ft.Text("Esperando foto del nodo via HTTP...",size=9,color=C["gris_texto"],italic=True)

    # ── Contador de generación de imagen ya mostrada ──────────────────
    # Permite detectar si llegó una foto nueva sin comparar strings base64.
    img_gen_mostrada = {"v": -1}

    # ── Inicializar controles desde estado_nodos (pre-cargado del disco) ──
    def actualizar():
        """
        Actualiza la interfaz con datos del estado global del nodo.
        Se llama:
          - inmediatamente al abrir el dashboard (muestra el último dato guardado)
          - cada vez que llega un mensaje MQTT nuevo (sensores)
          - cada vez que llega una foto nueva por HTTP
        """
        d=estado_nodos[nodo_id]["dht"]; vt.value=d["t"]; vh.value=d["h"]
        s=estado_nodos[nodo_id]["suelo"]
        vN.value=s["N"]; vP.value=s["P"]; vK.value=s["K"]
        vSH.value=s["hum"]; vST.value=s["temp"]; vEC.value=s["ec"]; vPH.value=s["ph"]
        vBAT.value=estado_nodos[nodo_id]["bateria"]["porcentaje"]
        if estado_mqtt["conectado"]:
            dot.bgcolor=C["verde_claro"]; txt_m.value="MQTT Conectado"; txt_m.color=C["verde_claro"]
        else:
            dot.bgcolor=C["rojo"]; txt_m.value="Sin conexión"; txt_m.color=C["rojo"]

        # ── Actualizar imagen si llegó una nueva mientras estábamos fuera ──
        if tiene_cam:
            gen_actual = estado_nodos[nodo_id].get("imagen_gen", 0)
            if gen_actual > img_gen_mostrada["v"]:
                img_gen_mostrada["v"] = gen_actual
                img_b64 = estado_nodos[nodo_id].get("ultima_imagen_b64")
                hora_img = estado_nodos[nodo_id].get("ultima_imagen_ts", "")
                if img_b64:
                    img_c.src = f"data:image/jpeg;base64,{img_b64}"
                    txt_c.value  = f"Última foto: {hora_img}" if hora_img else "Foto recibida"
                    txt_c.italic = False

        try:
            page.update()
        except Exception as e:
            print(f"[ACTUALIZAR] Error: {e}")

    # Registrar callback para recibir actualizaciones (MQTT y fotos HTTP)
    registrar_callback(actualizar)

    # Actualizar inmediatamente para mostrar el estado actual (datos + foto guardada)
    actualizar()

    def volver(e):
        # Desregistrar callback al salir
        desregistrar_callback(actualizar)
        on_volver()

    # Registrar controles de cámara para actualizaciones en vivo vía HTTP
    if tiene_cam:
        camara_iniciar_mqtt(img_c, txt_c, page, nodo_id)

    # NAVEGACIÓN POR VENTANAS (sin scroll)
    vista_actual = {"v": 0}  # 0: DHT+Bat, 1: Suelo, 2: Cámara

    # Contenedor de las vistas
    contenedor_vistas = ft.Container(expand=True)

    # Vista 1: DHT22 + Batería
    vista_dht_bat = ft.Container(border_radius=12,bgcolor=C["fondo"],
        padding=ft.Padding.all(8),
        content=ft.Column([
            # DHT22
            ft.Container(border_radius=12,bgcolor=C["naranja_suave"],
                border=ft.Border.all(1,C["naranja_borde"]),padding=ft.Padding.all(8),
                content=ft.Column([
                    sec_header(ft.Icons.DEVICE_THERMOSTAT,"Sensor Ambiental","DHT22 — Temperatura y Humedad",C["naranja"]),
                    ft.Divider(color=C["naranja_borde"],height=1),ft.Container(height=2),
                    ft.Row([
                        ft.Container(expand=True,content=sensor_card("Temperatura Ambiente","°C",vt,ft.Icons.THERMOSTAT,C["naranja"])),
                        ft.Container(width=6),
                        ft.Container(expand=True,content=sensor_card("Humedad Relativa","%",vh,ft.Icons.WATER_DROP,C["azul"])),
                    ]),
                ], spacing=4)),
            ft.Container(height=6),
            # Batería
            ft.Container(border_radius=12,bgcolor=C["verde_suave"],
                border=ft.Border.all(1,C["verde_borde"]),padding=ft.Padding.all(8),
                content=ft.Column([
                    sec_header(ft.Icons.BATTERY_CHARGING_FULL,"Batería del Nodo","Voltaje 12V",C["verde"]),
                    ft.Divider(color=C["verde_borde"],height=1),ft.Container(height=2),
                    sensor_card("Carga restante","%",vBAT,ft.Icons.BATTERY_FULL,C["verde"]),
                ], spacing=4)),
        ], spacing=0, expand=True))

    # Vista 2: Sensor Siete variables
    vista_suelo = ft.Container(border_radius=12,bgcolor=C["verde_suave"],
        border=ft.Border.all(1,C["verde_borde"]),padding=ft.Padding.all(8),
        content=ft.Column([
            sec_header(ft.Icons.GRASS,"Sensor de Suelo","RS-485 · 7 Parámetros",C["verde"]),
            ft.Divider(color=C["verde_borde"],height=1),ft.Container(height=2),
            ft.Text("Nutrientes NPK",size=9,color=C["gris_texto"],weight=ft.FontWeight.W_600),
            ft.Row([
                ft.Container(expand=True,content=sensor_card("Nitrógeno (N)","mg/kg",vN,ft.Icons.SCIENCE,C["verde_medio"])),
                ft.Container(width=4),
                ft.Container(expand=True,content=sensor_card("Fósforo (P)","mg/kg",vP,ft.Icons.SCIENCE,C["verde_medio"])),
                ft.Container(width=4),
                ft.Container(expand=True,content=sensor_card("Potasio (K)","mg/kg",vK,ft.Icons.SCIENCE,C["verde_medio"])),
            ]),
            ft.Container(height=4),
            ft.Text("Propiedades Físicas y Químicas",size=9,color=C["gris_texto"],weight=ft.FontWeight.W_600),
            ft.ResponsiveRow([
                ft.Container(col={"xs":6,"md":3},content=sensor_card("Humedad","%",vSH,ft.Icons.WATER,C["azul"])),
                ft.Container(col={"xs":6,"md":3},content=sensor_card("Temperatura","°C",vST,ft.Icons.THERMOSTAT,C["naranja"])),
                ft.Container(col={"xs":6,"md":3},content=sensor_card("Conduct.","µS/cm",vEC,ft.Icons.BOLT,C["cafe"])),
                ft.Container(col={"xs":6,"md":3},content=sensor_card("pH","",vPH,ft.Icons.WATER_DROP,C["purpura"])),
            ], spacing=4, run_spacing=4),
        ], spacing=4, expand=True, scroll=ft.ScrollMode.AUTO))

    # Vista 3: Cámara
    vista_camara = ft.Container(border_radius=12,bgcolor=C["cafe_suave"],
        border=ft.Border.all(1,C["cafe_borde"]),padding=ft.Padding.all(8),
        content=ft.Column([
            sec_header(ft.Icons.CAMERA_ALT,"Cámara ESP32-CAM",
                f'Fotos vía HTTP — {nodo["camera_ip"] if tiene_cam else "No disponible"}',C["cafe"]),
            ft.Divider(color=C["cafe_borde"],height=1),ft.Container(height=2),
            ft.Row([txt_c],alignment=ft.MainAxisAlignment.CENTER),
            ft.Container(alignment=ft.Alignment(0,0),border_radius=8,
                clip_behavior=ft.ClipBehavior.ANTI_ALIAS,
                content=img_c, expand=True) if tiene_cam else ft.Container(expand=True,
                    content=ft.Text("Este nodo no tiene cámara",size=12,color=C["gris_texto"]),
                    alignment=ft.Alignment(0,0)),
        ], spacing=4, expand=True))

    vistas = [vista_dht_bat, vista_suelo]
    if tiene_cam:
        vistas.append(vista_camara)

    nombres_vistas = ["DHT22 + Batería", "Sensor de Suelo"]
    if tiene_cam:
        nombres_vistas.append("Cámara")

    def cambiar_vista(nueva_vista):
        vista_actual["v"] = nueva_vista
        contenedor_vistas.content = vistas[nueva_vista]
        # Actualizar botones de navegación
        actualizar_botones_nav()
        page.update()

    # Botones de navegación entre vistas
    btn_anterior = ft.IconButton(
        icon=ft.Icons.ARROW_BACK_IOS,
        icon_color=C["verde"],
        icon_size=16,
        on_click=lambda e: cambiar_vista(max(0, vista_actual["v"] - 1)))

    btn_siguiente = ft.IconButton(
        icon=ft.Icons.ARROW_FORWARD_IOS,
        icon_color=C["verde"],
        icon_size=16,
        on_click=lambda e: cambiar_vista(min(len(vistas) - 1, vista_actual["v"] + 1)))

    txt_vista_actual = ft.Text("", size=10, weight=ft.FontWeight.BOLD, color=C["negro_texto"])

    def actualizar_botones_nav():
        btn_anterior.disabled = (vista_actual["v"] == 0)
        btn_siguiente.disabled = (vista_actual["v"] == len(vistas) - 1)
        txt_vista_actual.value = nombres_vistas[vista_actual["v"]]

    actualizar_botones_nav()
    contenedor_vistas.content = vistas[0]

    # Botones de acción
    btn_config = ft.FilledButton(
        content=ft.Row([
            ft.Icon(ft.Icons.SETTINGS, size=14, color=ft.Colors.WHITE),
            ft.Text("Config", size=10, color=ft.Colors.WHITE, weight=ft.FontWeight.W_600),
        ], spacing=4, tight=True),
        style=ft.ButtonStyle(bgcolor=C["azul"], shape=ft.RoundedRectangleBorder(radius=6),
            padding=ft.Padding.symmetric(horizontal=8, vertical=6)),
        on_click=lambda e: on_config())

    btn_historial = ft.FilledButton(
        content=ft.Row([
            ft.Icon(ft.Icons.HISTORY, size=14, color=ft.Colors.WHITE),
            ft.Text("Historial", size=10, color=ft.Colors.WHITE, weight=ft.FontWeight.W_600),
        ], spacing=4, tight=True),
        style=ft.ButtonStyle(bgcolor=C["cafe"], shape=ft.RoundedRectangleBorder(radius=6),
            padding=ft.Padding.symmetric(horizontal=8, vertical=6)),
        on_click=lambda e: on_historico())

    return ft.Column([
        # Barra superior
        ft.Container(bgcolor=C["verde"],padding=ft.Padding.symmetric(horizontal=6,vertical=6),
            content=ft.Row([
                ft.IconButton(ft.Icons.ARROW_BACK,icon_color=ft.Colors.WHITE,icon_size=18,on_click=volver),
                ft.Icon(ft.Icons.ECO,color=C["verde_claro"],size=14),
                ft.Text(nodo["nombre"],color=ft.Colors.WHITE,size=11,weight=ft.FontWeight.W_600),
                ft.Container(expand=True),
                ft.Row([dot,txt_m],spacing=4,vertical_alignment=ft.CrossAxisAlignment.CENTER),
                ft.Container(width=6),
            ], spacing=4,vertical_alignment=ft.CrossAxisAlignment.CENTER)),

        # Cuerpo con navegación
        ft.Container(expand=True,bgcolor=C["fondo"],
            padding=ft.Padding.symmetric(horizontal=8,vertical=6),
            content=ft.Column([
                # Encabezado con botones
                ft.Row([
                    ft.Column([
                        ft.Text("Monitoreo en Tiempo Real",size=13,weight=ft.FontWeight.BOLD,color=C["negro_texto"]),
                        ft.Text(f'Topic: {nodo["topic"]}',size=8,color=C["gris_texto"]),
                    ], spacing=1, expand=True),
                    btn_config,
                    btn_historial,
                ], vertical_alignment=ft.CrossAxisAlignment.CENTER, spacing=6),
                ft.Divider(color=C["gris_borde"],height=1),

                # Navegación de vistas
                ft.Row([
                    btn_anterior,
                    ft.Container(expand=True, content=ft.Row([txt_vista_actual],
                        alignment=ft.MainAxisAlignment.CENTER)),
                    btn_siguiente,
                ], vertical_alignment=ft.CrossAxisAlignment.CENTER),

                ft.Container(height=2),
                # Contenedor de vista actual
                contenedor_vistas,
            ], spacing=4, expand=True)),
    ], spacing=0, expand=True)

# ══════════════════════════════════════════════════════════════════════
#  PANTALLA 4 — CONFIGURACIÓN DE HORARIOS PERSONALIZABLES
# ══════════════════════════════════════════════════════════════════════
def build_config(nodo, page, on_volver):
    nodo_id = nodo["id"]
    cfg = sqlite_config(nodo_id)
    _cfg_topic = estado_config_horarios.get(nodo_id)
    if _cfg_topic:
        cfg = {"horas": _cfg_topic["horarios"], "actualizado": _cfg_topic["actualizado"], "origen": _cfg_topic["origen"]}
    horarios_guardados = _normalizar_horarios(cfg.get("horas", ["10:00", "18:00"]))

    # Modos rápidos de 2 y 3 medidas + modo personalizado.
    # En personalizado ya no existe un límite arbitrario de 12 medidas.
    # El límite físico es 1440 medidas si cada una debe ocupar un minuto único.
    modo = {"v": "2"}
    num_custom = {"v": max(4, len(horarios_guardados))}
    horarios_seleccionados = {
        "2": horarios_guardados[:2] if len(horarios_guardados) >= 2 else ["10:00", "18:00"],
        "3": horarios_guardados[:3] if len(horarios_guardados) >= 3 else ["06:00", "12:00", "18:00"],
        "N": list(horarios_guardados) if len(horarios_guardados) >= 4 else [
            "06:00", "08:00", "10:00", "12:00", "14:00", "16:00", "18:00", "20:00"
        ],
    }

    # La pantalla siempre abre en "2 Medidas".
    # Las configuraciones guardadas se conservan para reutilizarlas si el
    # usuario cambia a otro modo, pero no fuerzan la selección inicial.

    estado_txt = ft.Text(
        f'Horario activo: {len(horarios_guardados)} medidas/dia  -  '
        f'Ultima actualizacion: {cfg.get("actualizado", "--")}  |  Desde: {cfg.get("origen", "Raspberry (configuración local)")}',
        size=9, color=C["gris_texto"], italic=True)

    origen_horarios_txt = ft.Text(
        f'Horas configuradas actualmente: {", ".join(horarios_guardados) or "sin horarios"}  |  Horas configuradas desde: {cfg.get("origen", "Raspberry (configuración local)")}',
        size=9, color=C["azul_oscuro"], italic=True)
    _storage_cfg = cargar_config_almacenamiento()
    storage_max_input = ft.TextField(label="Límite carpeta AgroTech (MB)", value=str(_storage_cfg["max_mb"]), width=220, dense=True, keyboard_type=ft.KeyboardType.NUMBER)
    storage_delete_input = ft.TextField(label="Datos/fotos por lote", value=str(_storage_cfg["eliminar_cantidad"]), width=180, dense=True, keyboard_type=ft.KeyboardType.NUMBER)
    _st_inicial = estado_almacenamiento()
    storage_status = ft.Text(
        f'Carpeta: {_st_inicial["total_bytes"]/1048576:.1f} MB / {_st_inicial["max_mb"]} MB · '
        f'Datos: {_st_inicial["datos_cantidad"]} ({_st_inicial["datos_payload_bytes"]/1024:.1f} KB) · '
        f'Fotos: {_st_inicial["fotos_cantidad"]} ({_st_inicial["fotos_bytes"]/1048576:.1f} MB)',
        size=9, color=C["gris_texto"])

    def guardar_almacenamiento_ui(e=None):
        try:
            guardar_config_almacenamiento(int(storage_max_input.value), int(storage_delete_input.value))
            aplicar_limpieza_almacenamiento()
            st = estado_almacenamiento()
            storage_status.value = (f'Carpeta: {st["total_bytes"]/1048576:.1f} MB / {st["max_mb"]} MB · '
                                    f'Datos: {st["datos_cantidad"]} ({st["datos_payload_bytes"]/1024:.1f} KB) · '
                                    f'Fotos: {st["fotos_cantidad"]} ({st["fotos_bytes"]/1048576:.1f} MB)')
            storage_status.color = C["verde"]
        except Exception as ex:
            storage_status.value = f"Error de configuración: {ex}"
            storage_status.color = C["rojo"]
        try: page.update()
        except Exception: pass

    def refrescar_horarios_desde_mqtt():
        actual = estado_config_horarios.get(nodo_id)
        if actual:
            origen_horarios_txt.value = f'Horas configuradas actualmente: {", ".join(actual["horarios"])}  |  Horas configuradas desde: {actual["origen"]}'
            try: page.update()
            except Exception: pass

    registrar_callback(refrescar_horarios_desde_mqtt)

    btn_2med = ft.Container(border_radius=8, padding=ft.Padding.all(8))
    btn_3med = ft.Container(border_radius=8, padding=ft.Padding.all(8))
    btn_nmed = ft.Container(border_radius=8, padding=ft.Padding.all(8))

    # Matriz preordenada: cada fila contiene un número fijo de medidas.
    # Esto conserva el rendimiento de ListView sin dejar una tarjeta
    # ocupando todo el ancho de la pantalla.
    # 6 columnas dan una distribución compacta y estable en escritorio.
    COLUMNAS_POR_FILA = 6

    dropdowns_container = ft.Column(
        spacing=8,
        tight=True,
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
    )
    horarios_scroll = ft.ListView(
        controls=[dropdowns_container],
        expand=True,
        spacing=0,
        auto_scroll=False,
    )

    num_display = ft.Text(
        str(num_custom["v"]), size=15,
        weight=ft.FontWeight.BOLD, color=C["verde"])

    # Cada entrada contiene dropdown de hora, dropdown de minuto,
    # contenedor visual y mensaje de error.
    refs_activos = []
    btn_enviar = None
    estado_error = None

    def horario_de_ref(ref):
        h = ref["hora"].value
        m = ref["minuto"].value
        if h is None or m is None:
            return None
        return f"{int(h):02d}:{int(m):02d}"

    def validar_horarios():
        """
        Una sola pasada O(n) para detectar duplicados.
        No reconstruye la UI; solo cambia los controles cuyo estado
        realmente cambió. Esto reduce trabajo cuando hay muchas medidas.
        """
        usados = {}
        hay_error = False

        for i, ref in enumerate(refs_activos):
            horario = horario_de_ref(ref)
            duplicado = horario is not None and horario in usados

            estado_anterior = ref.get("duplicado", False)
            if duplicado != estado_anterior:
                ref["container"].border = ft.Border.all(
                    1.5 if duplicado else 1,
                    C["rojo"] if duplicado else C["gris_borde"])
                ref["error"].value = (
                    f"Horario repetido: {horario}" if duplicado else "")
                ref["error"].visible = duplicado
                ref["duplicado"] = duplicado

            if duplicado:
                hay_error = True
            elif horario is not None:
                usados[horario] = i

        nuevo_disabled = hay_error or len(refs_activos) == 0
        if btn_enviar is not None and btn_enviar.disabled != nuevo_disabled:
            btn_enviar.disabled = nuevo_disabled

        return not hay_error

    def on_horario_change(e):
        # La protección sigue siendo inmediata, pero evitamos trabajo
        # adicional fuera de esta única validación.
        validar_horarios()
        page.update()

    def siguiente_horario_libre(horarios):
        """Busca el primer HH:MM libre para una nueva medida."""
        usados = set(_normalizar_horarios(horarios))
        for minuto_total in range(24 * 60):
            candidato = f"{minuto_total // 60:02d}:{minuto_total % 60:02d}"
            if candidato not in usados:
                return candidato
        return "23:59"

    def decrementar(e):
        quitar_medida_custom()

    def incrementar(e):
        if num_custom["v"] >= 24 * 60:
            if estado_error is not None:
                estado_error.value = (
                    "No se pueden agregar más medidas: los 1440 minutos "
                    "del día ya están ocupados.")
                estado_error.visible = True
                page.update()
            return
        agregar_medida_custom()

    selector_cantidad = ft.Row([
        ft.IconButton(
            ft.Icons.REMOVE_CIRCLE_OUTLINE,
            icon_color=C["verde"],
            on_click=decrementar,
            icon_size=18),
        ft.Container(
            width=44, height=32, border_radius=8,
            bgcolor=C["verde_suave"],
            border=ft.Border.all(1, C["verde_borde"]),
            alignment=ft.Alignment(0, 0),
            content=num_display),
        ft.IconButton(
            ft.Icons.ADD_CIRCLE_OUTLINE,
            icon_color=C["verde"],
            on_click=incrementar,
            icon_size=18),
        ft.Text(
            "medidas/dia (Personalizado)",
            size=10, color=C["gris_texto"]),
    ], spacing=2, vertical_alignment=ft.CrossAxisAlignment.CENTER)

    contenedor_custom = ft.Column(
        [ft.Container(height=2), selector_cantidad],
        spacing=4,
        visible=True)

    def hacer_dropdown(horario_val, idx_label):
        horario_val = _normalizar_horario(horario_val) or "12:00"
        hora_val, minuto_val = map(int, horario_val.split(":"))

        dd_hora = ft.Dropdown(
            width=84,
            value=str(hora_val),
            options=[ft.dropdown.Option(str(h)) for h in range(24)],
            label=f"H{idx_label}",
            text_size=11,
            dense=True,
            content_padding=ft.Padding.symmetric(horizontal=5, vertical=2))

        dd_minuto = ft.Dropdown(
            width=84,
            value=str(minuto_val),
            options=[ft.dropdown.Option(f"{m:02d}") for m in range(60)],
            label="Min",
            text_size=11,
            dense=True,
            content_padding=ft.Padding.symmetric(horizontal=5, vertical=2))

        # Compatibilidad con versiones de Flet donde on_change no es
        # argumento del constructor.
        dd_hora.on_change = on_horario_change
        dd_minuto.on_change = on_horario_change

        error = ft.Text(
            "", size=8, color=C["rojo"],
            visible=False, text_align=ft.TextAlign.CENTER)

        item = ft.Container(
            width=184,
            padding=ft.Padding.symmetric(horizontal=5, vertical=4),
            border_radius=8,
            border=ft.Border.all(1, C["gris_borde"]),
            bgcolor=C["blanco"],
            content=ft.Column([
                ft.Text(
                    f"M{idx_label}", size=8,
                    color=C["gris_texto"],
                    weight=ft.FontWeight.W_600),
                ft.Row(
                    [dd_hora, dd_minuto],
                    spacing=3,
                    tight=True,
                    alignment=ft.MainAxisAlignment.CENTER),
                error,
            ], spacing=2, horizontal_alignment=ft.CrossAxisAlignment.CENTER)
        )

        refs_activos.append({
            "hora": dd_hora,
            "minuto": dd_minuto,
            "container": item,
            "error": error,
            "duplicado": False,
        })
        return item

    def reconstruir_horarios_custom():
        """Reconstruye solo al cambiar de modo; +/- modifica incrementalmente."""
        refs_activos.clear()
        n = num_custom["v"]
        horarios = horarios_seleccionados["N"]

        while len(horarios) < n:
            horarios.append(siguiente_horario_libre(horarios))

        controles = [
            hacer_dropdown(horarios[i], i + 1) for i in range(n)
        ]

        # Distribución preordenada en filas de 6. No usamos wrap dinámico:
        # Flet solo necesita calcular la posición de estas filas ya definidas.
        filas = []
        for inicio in range(0, len(controles), COLUMNAS_POR_FILA):
            grupo = controles[inicio:inicio + COLUMNAS_POR_FILA]
            fila = ft.Row(
                grupo,
                spacing=8,
                run_spacing=8,
                vertical_alignment=ft.CrossAxisAlignment.START,
            )
            filas.append(fila)

        dropdowns_container.controls = filas
        validar_horarios()

    def agregar_medida_custom():
        """Añade una sola tarjeta, sin reconstruir las anteriores."""
        n_actual = len(refs_activos)
        if n_actual >= 24 * 60:
            return

        horarios = horarios_seleccionados["N"]
        nuevo = siguiente_horario_libre(horarios)
        horarios.append(nuevo)
        num_custom["v"] = n_actual + 1
        num_display.value = str(num_custom["v"])

        nueva_tarjeta = hacer_dropdown(nuevo, num_custom["v"])

        if (
            dropdowns_container.controls
            and len(dropdowns_container.controls[-1].controls) < COLUMNAS_POR_FILA
        ):
            # Todavía hay espacio en la última fila.
            dropdowns_container.controls[-1].controls.append(nueva_tarjeta)
        else:
            # La última fila ya tiene 6: crear la siguiente fila.
            dropdowns_container.controls.append(
                ft.Row(
                    [nueva_tarjeta],
                    spacing=8,
                    vertical_alignment=ft.CrossAxisAlignment.START,
                )
            )

        validar_horarios()
        page.update()

    def quitar_medida_custom():
        """Elimina solo la última tarjeta."""
        if len(refs_activos) <= 4:
            return

        refs_activos.pop()

        if dropdowns_container.controls:
            ultima_fila = dropdowns_container.controls[-1]
            if ultima_fila.controls:
                ultima_fila.controls.pop()
            if not ultima_fila.controls:
                dropdowns_container.controls.pop()

        horarios = horarios_seleccionados["N"]
        if horarios:
            horarios.pop()

        num_custom["v"] = len(refs_activos)
        num_display.value = str(num_custom["v"])
        validar_horarios()
        page.update()

    def actualizar_modo_ui():
        refs_activos.clear()

        for b, m in [
            (btn_2med, "2"),
            (btn_3med, "3"),
            (btn_nmed, "N")
        ]:
            if modo["v"] == m:
                b.bgcolor = C["verde_suave"]
                b.border = ft.Border.all(2, C["verde"])
            else:
                b.bgcolor = C["blanco"]
                b.border = ft.Border.all(1, C["gris_borde"])

        contenedor_custom.visible = True

        # +/- solamente modifican la cantidad en modo Personalizado.
        selector_cantidad.visible = (modo["v"] == "N")

        if modo["v"] == "N":
            reconstruir_horarios_custom()
        else:
            n = int(modo["v"])
            horarios = horarios_seleccionados[modo["v"]]
            controles = [
                hacer_dropdown(horarios[i], i + 1) for i in range(n)
            ]

            # Mismo layout compacto para los modos de cantidad fija.
            # Normalmente solo habrá 2 o 3 tarjetas, pero mantener una
            # sola rutina de distribución evita lógica duplicada.
            dropdowns_container.controls = [
                ft.Row(
                    controles[inicio:inicio + COLUMNAS_POR_FILA],
                    spacing=8,
                    vertical_alignment=ft.CrossAxisAlignment.START,
                )
                for inicio in range(0, len(controles), COLUMNAS_POR_FILA)
            ]
            validar_horarios()

        page.update()

    def cambiar_modo(nuevo_modo):
        modo["v"] = nuevo_modo
        if estado_error is not None:
            estado_error.visible = False
        actualizar_modo_ui()

    btn_2med.content = ft.Column([
        ft.Text(
            "2 Medidas", size=11,
            weight=ft.FontWeight.BOLD, color=C["negro_texto"]),
        ft.Text(
            "Ej: 10:33 y 18:05",
            size=9, color=C["gris_texto"]),
    ], spacing=2)
    btn_2med.on_click = lambda e: cambiar_modo("2")
    btn_2med.ink = True

    btn_3med.content = ft.Column([
        ft.Text(
            "3 Medidas", size=11,
            weight=ft.FontWeight.BOLD, color=C["negro_texto"]),
        ft.Text(
            "Ej: 06:15, 12:30, 18:45",
            size=9, color=C["gris_texto"]),
    ], spacing=2)
    btn_3med.on_click = lambda e: cambiar_modo("3")
    btn_3med.ink = True

    btn_nmed.content = ft.Column([
        ft.Text(
            "Personalizado", size=11,
            weight=ft.FontWeight.BOLD, color=C["negro_texto"]),
        ft.Text(
            "4 o más medidas",
            size=9, color=C["gris_texto"]),
    ], spacing=2)
    btn_nmed.on_click = lambda e: cambiar_modo("N")
    btn_nmed.ink = True

    resultado = ft.Container(
        visible=False,
        border_radius=10,
        padding=ft.Padding.all(10))

    estado_error = ft.Text(
        "",
        size=9,
        color=C["rojo"],
        weight=ft.FontWeight.W_600,
        visible=False)

    # v12: línea de estado de la confirmación del nodo
    txt_ack = ft.Text(
        "",
        size=9,
        italic=True,
        color=C["gris_texto"])

    def _esperar_confirmacion(nid, timeout=12):
        """Espera (en un hilo) el ACK del nodo y actualiza txt_ack."""
        txt_ack.value = "Esperando confirmacion del nodo..."
        txt_ack.color = C["gris_texto"]

        def _run():
            fin = time.time() + timeout
            while time.time() < fin:
                ack = estado_config_ack.get(nid)
                if ack:
                    horarios_ack = ack.get("horarios", [])
                    txt_ack.value = (
                        f'Nodo confirmo ({ack.get("recibido","")}): '
                        f'{", ".join(horarios_ack) if horarios_ack else "OK"}')
                    txt_ack.color = C["verde"]
                    try:
                        page.update()
                    except Exception:
                        pass
                    return
                time.sleep(0.3)
            txt_ack.value = (
                "Sin confirmacion: el nodo probablemente esta dormido. "
                "El horario quedo retenido y lo recibira al despertar.")
            txt_ack.color = C["naranja"]
            try:
                page.update()
            except Exception:
                pass

        threading.Thread(target=_run, daemon=True).start()

    def enviar(e):
        if not validar_horarios():
            estado_error.value = (
                "No se puede aplicar: hay dos o más medidas con el mismo horario."
            )
            estado_error.visible = True
            resultado.visible = False
            page.update()
            return

        horarios = []
        for ref in refs_activos:
            h = int(ref["hora"].value)
            m = int(ref["minuto"].value)
            horarios.append(f"{h:02d}:{m:02d}")

        # Orden cronológico.
        horarios = sorted(horarios, key=_minutos_desde_medianoche)

        # Payload compacto:
        # "horas" mantiene la clave anterior y "minutos" aporta la
        # precisión nueva. No hace falta enviar "horarios" porque el
        # ESP32 puede reconstruir HH:MM a partir de ambas listas.
        #
        # NUEVO: "fecha" manda la hora actual de esta PC (formato
        # "YYYY-MM-DD HH:MM:SS") para que el ESP32 sincronice su RTC
        # DS1307 cada vez que se envía una configuración. Antes el RTC
        # nunca se ajustaba desde aquí, así que podía quedar
        # desincronizado respecto al reloj de la interfaz.
        horas_payload = [int(h.split(":")[0]) for h in horarios]
        minutos_payload = [int(h.split(":")[1]) for h in horarios]
        # v13: el horario (retenido) ya NO lleva "fecha". La hora se manda
        # aparte, sin retain, con mqtt_publicar_hora().
        payload = json.dumps({
            "horas": horas_payload,
            "minutos": minutos_payload,
            "origen": "Interfaz de la Raspberry",
        })

        # v12: limpio la confirmación anterior y publico al tópico del nodo
        estado_config_ack.pop(nodo_id, None)
        ok = mqtt_publicar_config(payload, nodo_id)
        mqtt_publicar_hora(nodo_id)   # v13: sincroniza el DS3231 si el nodo esta en modo config
        if ok:
            guardar_config_nodo(nodo_id, horarios)
            guardar_config_sqlite(nodo_id, horarios, "Base de datos en la nube")

            resultado.bgcolor = C["verde_suave"]
            resultado.border = ft.Border.all(1.5, C["verde_borde"])
            resultado.content = ft.Row([
                ft.Icon(
                    ft.Icons.CHECK_CIRCLE,
                    color=C["verde"],
                    size=16),
                ft.Column([
                    ft.Text(
                        "Configuracion enviada!",
                        size=11,
                        weight=ft.FontWeight.BOLD,
                        color=C["verde"]),
                    ft.Text(
                        f'esp32/{nodo_id}/config (retain): {payload}',
                        size=8,
                        color=C["gris_texto"]),
                    ft.Text(
                        f'Horarios: {", ".join(horarios)}',
                        size=9,
                        color=C["negro_texto"]),
                    txt_ack,
                ], spacing=1),
            ], spacing=8)

            # v12: espero en segundo plano la confirmación del ESP32.
            # El nodo responde en esp32/<id>/configok con el horario que
            # realmente guardó. Si no responde, casi seguro está dormido:
            # el mensaje quedó retenido y lo recibirá al despertar.
            _esperar_confirmacion(nodo_id)

            estado_txt.value = (
                f'Horario activo: {len(horarios)} medidas/dia  -  '
                f'Ultima actualizacion: '
                f'{datetime.now().strftime("%Y-%m-%d %H:%M")}')
            estado_error.visible = False
        else:
            resultado.bgcolor = "#FFEBEE"
            resultado.border = ft.Border.all(1.5, C["rojo"])
            resultado.content = ft.Row([
                ft.Icon(ft.Icons.ERROR, color=C["rojo"], size=16),
                ft.Column([
                    ft.Text(
                        "Error al enviar",
                        size=11,
                        weight=ft.FontWeight.BOLD,
                        color=C["rojo"]),
                    ft.Text(
                        "Verifica que el broker MQTT esta activo.",
                        size=9,
                        color=C["gris_texto"]),
                ], spacing=1),
            ], spacing=8)

        resultado.visible = True
        page.update()

    btn_enviar = ft.FilledButton(
        content=ft.Row([
            ft.Icon(
                ft.Icons.SEND,
                size=14,
                color=ft.Colors.WHITE),
            ft.Text(
                "Aplicar\nConfiguracion",
                size=11,
                weight=ft.FontWeight.BOLD,
                color=ft.Colors.WHITE,
                text_align=ft.TextAlign.CENTER),
        ], spacing=6, alignment=ft.MainAxisAlignment.CENTER),
        style=ft.ButtonStyle(
            bgcolor=C["verde"],
            shape=ft.RoundedRectangleBorder(radius=8),
            padding=ft.Padding.symmetric(horizontal=14, vertical=12)),
        on_click=enviar,
        width=160)

    # Revalidar después de crear el botón.
    validar_horarios()

    def volver_config(e=None):
        desregistrar_callback(refrescar_horarios_desde_mqtt)
        on_volver()

    return ft.Column([
        barra_superior(
            f'Configuracion - {nodo["nombre"]}',
            on_back=volver_config),
        ft.Container(
            expand=True,
            bgcolor=C["fondo"],
            padding=ft.Padding.symmetric(horizontal=10, vertical=8),
            content=ft.Column([
                ft.Row([
                    ft.Container(
                        width=34, height=34, border_radius=10,
                        bgcolor=ft.Colors.with_opacity(
                            0.15, C["azul"]),
                        content=ft.Icon(
                            ft.Icons.SETTINGS,
                            color=C["azul"],
                            size=18),
                        alignment=ft.Alignment(0, 0)),
                    ft.Column([
                        ft.Text(
                            "Horario de Mediciones",
                            size=14,
                            weight=ft.FontWeight.BOLD,
                            color=C["negro_texto"]),
                        ft.Text(
                            "Selecciona hora y minuto para cada medicion.",
                            size=10,
                            color=C["gris_texto"]),
                    ], spacing=1),
                ], spacing=8, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                ft.Divider(color=C["gris_borde"], height=1),
                estado_txt,
                origen_horarios_txt,
                ft.Container(height=2),
                ft.Text(
                    "Selecciona cantidad de medidas:",
                    size=11,
                    weight=ft.FontWeight.W_600,
                    color=C["negro_texto"]),
                ft.Row([
                    btn_2med,
                    btn_3med,
                    btn_nmed,
                    ft.Container(expand=True),
                    btn_enviar
                ], spacing=8),
                contenedor_custom,
                estado_error,
                ft.Container(height=2),
                ft.Text(
                    "Configura los horarios:",
                    size=11,
                    weight=ft.FontWeight.W_600,
                    color=C["negro_texto"]),
                ft.Container(height=2),
                horarios_scroll,
                ft.Container(height=4),
                resultado,
                ft.Divider(color=C["gris_borde"], height=1),
                ft.Text("Almacenamiento local", size=11, weight=ft.FontWeight.BOLD, color=C["negro_texto"]),
                ft.Row([storage_max_input, storage_delete_input,
                        ft.FilledButton("Guardar límite", on_click=guardar_almacenamiento_ui)], spacing=8, wrap=True),
                storage_status,
            ], spacing=6, expand=True)),
    ], spacing=0, expand=True)


# ══════════════════════════════════════════════════════════════════════
#  PANTALLA 5 — HISTORIAL PAGINADO
# ══════════════════════════════════════════════════════════════════════
def build_historico(nodo, page, on_volver):
    nodo_id = nodo["id"]
    historial = list(reversed(cargar_historial(nodo_id)))
    total = len(historial)

    # Paginación
    pagina_actual = {"v": 0}
    registros_por_pagina = 5
    total_paginas = max(1, (total + registros_por_pagina - 1) // registros_por_pagina)

    def fila_dato(registro, idx):
        """Construye una fila del historial."""
        bg = C["blanco"] if idx % 2 == 0 else "#F7F9F7"
        ts  = registro.get("timestamp", "—")
        d   = registro.get("dht", {})
        s   = registro.get("suelo", {})
        b   = registro.get("bateria", {})
        t_dht  = f'{d.get("t","—")}°C' if d.get("t") is not None else "—"
        h_dht  = f'{d.get("h","—")}%'  if d.get("h") is not None else "—"
        N = str(s.get("N","—")); P = str(s.get("P","—")); K = str(s.get("K","—"))
        hum_s  = f'{s.get("hum","—")}%'  if s.get("hum") is not None else "—"
        temp_s = f'{s.get("temp","—")}°C' if s.get("temp") is not None else "—"
        ec_s   = str(s.get("ec","—"))
        ph_s   = str(s.get("ph","—"))
        bat    = f'{b.get("porcentaje","—")}%' if b.get("porcentaje") is not None else "—"

        def celda(txt, color=C["negro_texto"], w=80):
            return ft.Container(width=w, alignment=ft.Alignment(0,0),
                content=ft.Text(txt, size=10, color=color, text_align=ft.TextAlign.CENTER))

        return ft.Container(bgcolor=bg, border_radius=6,
            padding=ft.Padding.symmetric(horizontal=6, vertical=6),
            content=ft.Row([
                celda(ts, C["gris_texto"], w=130),
                celda(t_dht, C["naranja"]),
                celda(h_dht, C["azul"]),
                celda(N, C["verde_medio"]),
                celda(P, C["verde_medio"]),
                celda(K, C["verde_medio"]),
                celda(hum_s, C["azul"]),
                celda(temp_s, C["naranja"]),
                celda(ec_s, C["cafe"]),
                celda(ph_s, C["purpura"]),
                celda(bat, C["verde"]),
            ], spacing=4))

    def encabezado():
        def th(txt, w=80):
            return ft.Container(width=w, alignment=ft.Alignment(0,0),
                content=ft.Text(txt, size=10, weight=ft.FontWeight.BOLD, color=C["blanco"],
                    text_align=ft.TextAlign.CENTER))
        return ft.Container(bgcolor=C["verde"], border_radius=6, padding=ft.Padding.symmetric(horizontal=6,vertical=8),
            content=ft.Row([
                th("Fecha/Hora", w=130),
                th("T.Amb °C"), th("H.Amb %"),
                th("N"), th("P"), th("K"),
                th("H.Suelo"), th("T.Suelo"),
                th("EC"), th("pH"), th("Batería"),
            ], spacing=4))

    # Contenedor de filas
    filas_col = ft.Column(spacing=3)
    txt_pagina = ft.Text("", size=12, color=C["gris_texto"])

    btn_primera = ft.IconButton(icon=ft.Icons.FIRST_PAGE, icon_color=C["verde"])
    btn_anterior = ft.IconButton(icon=ft.Icons.CHEVRON_LEFT, icon_color=C["verde"])
    btn_siguiente = ft.IconButton(icon=ft.Icons.CHEVRON_RIGHT, icon_color=C["verde"])
    btn_ultima = ft.IconButton(icon=ft.Icons.LAST_PAGE, icon_color=C["verde"])

    def actualizar_tabla():
        inicio = pagina_actual["v"] * registros_por_pagina
        fin = min(inicio + registros_por_pagina, total)
        registros_pagina = historial[inicio:fin]

        filas_col.controls = [fila_dato(r, i) for i, r in enumerate(registros_pagina)]
        txt_pagina.value = f"Página {pagina_actual['v'] + 1} de {total_paginas}  ({inicio + 1}-{fin} de {total})"

        # Actualizar botones
        btn_primera.disabled = (pagina_actual["v"] == 0)
        btn_anterior.disabled = (pagina_actual["v"] == 0)
        btn_siguiente.disabled = (pagina_actual["v"] >= total_paginas - 1)
        btn_ultima.disabled = (pagina_actual["v"] >= total_paginas - 1)

        page.update()

    def ir_primera(e):
        pagina_actual["v"] = 0
        actualizar_tabla()

    def ir_anterior(e):
        if pagina_actual["v"] > 0:
            pagina_actual["v"] -= 1
            actualizar_tabla()

    def ir_siguiente(e):
        if pagina_actual["v"] < total_paginas - 1:
            pagina_actual["v"] += 1
            actualizar_tabla()

    def ir_ultima(e):
        pagina_actual["v"] = total_paginas - 1
        actualizar_tabla()

    btn_primera.on_click = ir_primera
    btn_anterior.on_click = ir_anterior
    btn_siguiente.on_click = ir_siguiente
    btn_ultima.on_click = ir_ultima

    if total == 0:
        cuerpo = ft.Container(expand=True, alignment=ft.Alignment(0,0),
            content=ft.Column([
                ft.Icon(ft.Icons.INBOX, size=60, color=C["gris_borde"]),
                ft.Text("Sin registros todavía", size=16, color=C["gris_texto"]),
                ft.Text("Los datos se guardan automáticamente al recibirlos del nodo.",
                        size=12, color=C["gris_texto"], text_align=ft.TextAlign.CENTER),
            ], alignment=ft.MainAxisAlignment.CENTER,
               horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=8))
    else:
        actualizar_tabla()
        cuerpo = ft.Column([
            ft.Container(height=4),
            encabezado(),
            ft.Container(expand=True, content=filas_col),
        ], spacing=6, expand=True)

    def exportar(e):
        """Exporta el historial a CSV."""
        ruta_csv = os.path.join(DATA_DIR, f"{nodo_id}_historial.csv")
        try:
            with open(ruta_csv, "w") as f:
                f.write("timestamp,dht_t,dht_h,N,P,K,hum_suelo,temp_suelo,ec,ph,bateria\n")
                for r in reversed(historial):
                    d = r.get("dht", {}); s = r.get("suelo", {}); b = r.get("bateria", {})
                    f.write(f'{r.get("timestamp","")},{d.get("t","")},{d.get("h","")},'
                            f'{s.get("N","")},{s.get("P","")},{s.get("K","")},'
                            f'{s.get("hum","")},{s.get("temp","")},{s.get("ec","")},{s.get("ph","")},'
                            f'{b.get("porcentaje","")}\n')
            page.overlay.append(ft.SnackBar(
                content=ft.Text(f'CSV guardado en: {ruta_csv}'),
                open=True, bgcolor=C["verde"]))
            page.update()
        except Exception as ex:
            print(f"[EXPORT] {ex}")

    def _stat_hist(label, valor, icono, color):
        return ft.Container(border_radius=10, bgcolor=C["blanco"],
            padding=ft.Padding.symmetric(horizontal=12, vertical=8),
            border=ft.Border.all(1, C["gris_borde"]),
            content=ft.Row([
                ft.Icon(icono, color=color, size=16),
                ft.Column([
                    ft.Text(valor, size=14, weight=ft.FontWeight.BOLD, color=color),
                    ft.Text(label, size=10, color=C["gris_texto"]),
                ], spacing=0),
            ], spacing=8, vertical_alignment=ft.CrossAxisAlignment.CENTER))

    return ft.Column([
        barra_superior(f'Historial — {nodo["nombre"]}',
            on_back=lambda e: on_volver(),
            extras=[
                ft.Container(
                    content=ft.Row([
                        ft.Icon(ft.Icons.STORAGE, color=C["verde_claro"], size=14),
                        ft.Text(f'{total} registros', size=11, color=ft.Colors.WHITE),
                    ], spacing=4),
                ),
                ft.IconButton(ft.Icons.DOWNLOAD, icon_color=ft.Colors.WHITE,
                    tooltip="Exportar CSV", on_click=exportar),
            ]),

        ft.Container(expand=True, bgcolor=C["fondo"],
            padding=ft.Padding.symmetric(horizontal=10, vertical=12),
            content=ft.Column([
                # Encabezado
                ft.Row([
                    ft.Container(width=40, height=40, border_radius=10,
                        bgcolor=ft.Colors.with_opacity(0.15, C["cafe"]),
                        content=ft.Icon(ft.Icons.HISTORY, color=C["cafe"], size=22),
                        alignment=ft.Alignment(0,0)),
                    ft.Column([
                        ft.Text("Historial de Mediciones", size=18, weight=ft.FontWeight.BOLD, color=C["negro_texto"]),
                        ft.Text(f'Nodo: {nodo["nombre"]} · {total} registros (máx. 500)',
                                size=11, color=C["gris_texto"]),
                    ], spacing=2),
                ], spacing=12, vertical_alignment=ft.CrossAxisAlignment.CENTER),

                ft.Divider(color=C["gris_borde"], height=1),

                # Resumen
                ft.Row([
                    _stat_hist("Registros", str(total), ft.Icons.LIST_ALT, C["azul"]),
                    _stat_hist("Última lectura",
                               historial[0].get("timestamp","—")[:10] if historial else "—",
                               ft.Icons.ACCESS_TIME, C["verde"]),
                ], spacing=10) if total > 0 else ft.Container(),

                ft.Container(height=4),

                # Tabla con scroll horizontal
                ft.Container(expand=True,
                    content=ft.Row([
                        ft.Container(expand=True, content=cuerpo)
                    ], scroll=ft.ScrollMode.AUTO)),

                # Controles de paginación
                ft.Container(height=8) if total > 0 else ft.Container(),
                ft.Row([
                    btn_primera,
                    btn_anterior,
                    ft.Container(expand=True, content=ft.Row([txt_pagina],
                        alignment=ft.MainAxisAlignment.CENTER)),
                    btn_siguiente,
                    btn_ultima,
                ], vertical_alignment=ft.CrossAxisAlignment.CENTER) if total > 0 else ft.Container(),

            ], spacing=8, expand=True)),
    ], spacing=0, expand=True)

# ══════════════════════════════════════════════════════════════════════
#  APLICACIÓN PRINCIPAL
# ══════════════════════════════════════════════════════════════════════
def main(page: ft.Page):
    page.title = "AgroTech Dashboard v3.1"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.padding = 0
    try:
        page.window.width  = 800
        page.window.height = 480
        page.window.full_screen = True
    except Exception:
        pass

    cont = ft.Container(expand=True)

    # Persistencia SQLite: fuente de verdad para la API web.
    sqlite_iniciar()
    aplicar_limpieza_almacenamiento()

    # Sincronización HTTP con el servidor en la nube.
    sincronizador_iniciar()

    # Iniciar MQTT global desde el principio (sensores/status/config).
    def global_update():
        """Callback para actualizaciones globales de MQTT."""
        page.update()

    mqtt_iniciar_global(global_update)

    # Sincronización de configuraciones de horarios desde main.py.
    # Se inicia después de MQTT para que los configok del ESP32 puedan
    # ser recibidos desde el primer momento.
    sincronizador_configuraciones_iniciar()

    # API REST + página HTML para clientes externos.
    fastapi_iniciar()

    # Servidor HTTP de fotos del ESP32-S3 CAM.
    http_photo_server_iniciar()

    def mostrar_inicio():
        cont.content = build_inicio(on_inicio=lambda e: mostrar_nodos())
        page.update()

    def mostrar_nodos():
        cont.content = build_nodos(on_volver=mostrar_inicio, on_sel=mostrar_dashboard)
        page.update()

    def mostrar_dashboard(nodo):
        cont.content = build_dashboard(
            nodo=nodo, page=page,
            on_volver=mostrar_nodos,
            on_config=lambda: mostrar_config(nodo),
            on_historico=lambda: mostrar_historico(nodo))
        page.update()

    def mostrar_config(nodo):   
        camara_detener()
        cont.content = build_config(
            nodo=nodo, page=page,
            on_volver=lambda: mostrar_dashboard(nodo))
        page.update()

    def mostrar_historico(nodo):
        cont.content = build_historico(
            nodo=nodo, page=page,
            on_volver=lambda: mostrar_dashboard(nodo))
        page.update()

    page.add(cont)
    mostrar_inicio()

if __name__ == "__main__":
    ft.run(main, view=ft.AppView.FLET_APP)
