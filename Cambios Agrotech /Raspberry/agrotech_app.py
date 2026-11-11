"""
AgroTech Dashboard — Compatible con Flet 0.84+
v3.2 — Cambios principales:
- Suscripción simultánea a todos los nodos desde el inicio
- Navegación por ventanas sin scroll táctil
- Horarios personalizables (hora y minuto)
- Historial paginado (10 registros por página)
- NUEVO: Recepción de fotografías por HTTP (reemplaza MQTT+Base64).
  MQTT se conserva para sensores, status y configuración de horario.
"""

import flet as ft
import paho.mqtt.client as mqtt
import json
import threading
import base64
import urllib.request
import os
from datetime import datetime
from typing import Optional
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# ══════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ══════════════════════════════════════════════════════════════════════
MQTT_BROKER = "localhost"
MQTT_PORT   = 1883

# NUEVO: servidor HTTP que recibe las fotos enviadas por la ESP32-S3 CAM.
# Debe coincidir con PHOTO_SERVER_URL configurado en el firmware del nodo.
HTTP_PHOTO_PORT = 8080
HTTP_PHOTO_PATH = "/upload"

DATA_DIR    = os.path.expanduser("~/agrotech/data")
os.makedirs(DATA_DIR, exist_ok=True)

NODOS = [
    {"id":"nodo1","nombre":"Nodo 1 · Parcela A","topic":"esp32/nodo1/data","camera_ip":"192.168.137.20","activo":True},
    {"id":"nodo2","nombre":"Nodo 2 · Parcela B","topic":"esp32/nodo2/data","camera_ip":"192.168.137.21","activo":True}, #Aqui se desactivo manualmente el nodo
    {"id":"nodo3","nombre":"Nodo 3 · Parcela C","topic":"esp32/nodo3/data","camera_ip":"192.168.137.22","activo":True}, #Aqui se desactivo manualmente el nodo
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
        dato["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        historial.append(dato)
        # Guardar máximo 500 registros por nodo
        if len(historial) > 500:
            historial = historial[-500:]
        with open(ruta, "w") as f:
            json.dump(historial, f, indent=2)
    except Exception as e:
        print(f"[DATA] Error guardando: {e}")

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

estado_mqtt = {"conectado": False}
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
            # Identificar de qué nodo viene el mensaje
            nodo_id = topic_to_nodo.get(msg.topic)
            if not nodo_id:
                return  # Topic desconocido

            data = json.loads(msg.payload.decode("utf-8"))
            registro = {}

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
                guardar_dato(nodo_id, registro)
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

def mqtt_publicar_config(payload: str) -> bool:
    """Publica configuración al topic esp32/config. (Sin cambios: sigue siendo MQTT)."""
    global _mqtt_client
    try:
        if _mqtt_client and _mqtt_client.is_connected():
            _mqtt_client.publish("esp32/config", payload)
            return True
        else:
            # Conexión temporal solo para publicar
            tmp = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            tmp.connect(MQTT_BROKER, MQTT_PORT)
            tmp.publish("esp32/config", payload)
            tmp.disconnect()
            return True
    except Exception as e:
        print(f"[CONFIG] Error publicando: {e}")
        return False

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
    """
    Lógica única de persistencia y actualización de UI para una foto recibida.
    Reemplaza lo que antes hacía cam_on_end() al decodificar Base64 desde MQTT;
    ahora recibe directamente los bytes binarios del POST HTTP.
    """
    try:
        nombre  = f"cam_{nodo_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
        ruta_jpg = os.path.join(DATA_DIR, nombre)
        with open(ruta_jpg, "wb") as f:
            f.write(img_bytes)
        print(f"[CAM] JPEG guardado: {ruta_jpg}")

        clean_b64 = base64.b64encode(img_bytes).decode("utf-8")
        hora      = datetime.now().strftime("%H:%M:%S")
        tam_kb    = len(img_bytes) // 1024

        # ── Persistir en estado_nodos (siempre, independiente de la UI) ──
        if nodo_id in estado_nodos:
            estado_nodos[nodo_id]["ultima_imagen_b64"] = clean_b64
            estado_nodos[nodo_id]["ultima_imagen_ts"]  = hora
            estado_nodos[nodo_id]["imagen_gen"] = estado_nodos[nodo_id].get("imagen_gen", 0) + 1
        else:
            print(f"[CAM] Advertencia: nodo_id '{nodo_id}' no existe en estado_nodos")

        # ── Actualizar UI en vivo si el dashboard está abierto en ese nodo ──
        ctrl = _cam_buffer.get("img_ctrl")
        txt  = _cam_buffer.get("txt_ctrl")
        pg   = _cam_buffer.get("page")
        if ctrl and pg and _cam_buffer.get("nodo_id") == nodo_id:
            ctrl.src = f"data:image/jpeg;base64,{clean_b64}"
            if txt:
                txt.value  = f"Última foto: {hora}  —  {tam_kb} KB"
                txt.italic = False
            try:
                pg.update()
            except Exception:
                pass
            print("[CAM] Imagen mostrada en dashboard activo")
        else:
            print(f"[CAM] Imagen guardada en estado_nodos[{nodo_id}] (dashboard cerrado o es otro nodo)")

        # Notificar callbacks para que cualquier vista activa reaccione
        notificar_actualizacion()

    except Exception as e:
        print(f"[CAM] Error procesando imagen: {e}")
        txt = _cam_buffer.get("txt_ctrl")
        pg  = _cam_buffer.get("page")
        if txt and pg:
            txt.value  = f"Error al recibir imagen: {e}"
            txt.italic = True
            try:
                pg.update()
            except Exception:
                pass

# ══════════════════════════════════════════════════════════════════════
#  SERVIDOR HTTP — Recepción de fotos del ESP32-S3 CAM
#  Reemplaza el flujo anterior de MQTT + Base64 fragmentado (cam/start,
#  cam/data, cam/end). El ESP32 ahora hace un POST binario directo aquí.
# ══════════════════════════════════════════════════════════════════════
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
        cfg = cargar_config_nodo(nodo["id"])
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
    cfg = cargar_config_nodo(nodo_id)
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
        f'Ultima actualizacion: {cfg.get("actualizado", "--")}',
        size=9, color=C["gris_texto"], italic=True)

    btn_2med = ft.Container(border_radius=8, padding=ft.Padding.all(8))
    btn_3med = ft.Container(border_radius=8, padding=ft.Padding.all(8))
    btn_nmed = ft.Container(border_radius=8, padding=ft.Padding.all(8))

    # Lista vertical en lugar de Row(wrap=True): evita recalcular
    # continuamente el ajuste de cientos de tarjetas.
    dropdowns_container = ft.ListView(
        expand=True,
        spacing=6,
        auto_scroll=False,
    )
    horarios_scroll = dropdowns_container

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

        dropdowns_container.controls = [
            hacer_dropdown(horarios[i], i + 1) for i in range(n)
        ]
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

        dropdowns_container.controls.append(
            hacer_dropdown(nuevo, num_custom["v"])
        )
        validar_horarios()
        page.update()

    def quitar_medida_custom():
        """Elimina solo la última tarjeta."""
        if len(refs_activos) <= 4:
            return

        refs_activos.pop()
        if dropdowns_container.controls:
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
            dropdowns_container.controls = [
                hacer_dropdown(horarios[i], i + 1) for i in range(n)
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
        horas_payload = [int(h.split(":")[0]) for h in horarios]
        minutos_payload = [int(h.split(":")[1]) for h in horarios]
        payload = json.dumps({
            "horas": horas_payload,
            "minutos": minutos_payload,
        })

        ok = mqtt_publicar_config(payload)
        if ok:
            guardar_config_nodo(nodo_id, horarios)

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
                        f'esp32/config: {payload}',
                        size=8,
                        color=C["gris_texto"]),
                    ft.Text(
                        f'Horarios: {", ".join(horarios)}',
                        size=9,
                        color=C["negro_texto"]),
                ], spacing=1),
            ], spacing=8)

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

    return ft.Column([
        barra_superior(
            f'Configuracion - {nodo["nombre"]}',
            on_back=lambda e: on_volver()),
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

    # Iniciar MQTT global desde el principio (sensores/status/config)
    def global_update():
        """Callback para actualizaciones globales de MQTT."""
        page.update()

    mqtt_iniciar_global(global_update)

    # NUEVO: iniciar servidor HTTP de fotos (reemplaza MQTT+Base64 para cámara)
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
