// ============================================================
// NODOS - INVERNADERO UQ - ESP32-S3 CAM + DS3231
//
// Cambios para esta version de codigo ya que solo se le agrego cambio al envio de fotos y envio de datos simulados.
//  - Datos SIMULADOS de sensores (DHT22, suelo NPK y bateria) que se
//    publican por MQTT en esp32/nodo2/data justo antes de enviar la
//    foto, con el mismo JSON que usa simulador_esp32.py:
//      {"dht":{"t":..,"h":..},
//       "suelo":{"N":..,"P":..,"K":..,"hum":..,"temp":..,"ec":..,"ph":..},
//       "bateria":{"porcentaje":..}}
//  - Para las pruebas como no se contaron con los sensores se agrego una funcio de envio de datos simulados donde
//   se puede activar/desactivar con #define SIMULAR_SENSORES (mas abajo).
//    Cuando tengas los sensores reales, comenta esa linea o eliminarla para que se puedan medir con los senores reales.
//
//
// TABLA DE SEÑALES DEL LED (GPIO42)
//  3 parpadeos 200 ms ....... arranque / test de LED
//  parpadeo doble rapido .... ventana de 4 s para entrar a config
//  1 parpadeo largo 700 ms .. WiFi conectado
//  2 parpadeos 120 ms ....... MQTT conectado
//  parpadeo lento 500/500 ... MODO CONFIG activo (esperando horario)
//  5 parpadeos 80 ms ........ horario recibido y guardado OK
//  LED fijo encendido ....... ciclo de medicion / foto en curso
//  2 parpadeos 100 ms ....... foto enviada con HTTP 200
//  4 parpadeos 80 ms ........ servidor respondio != 200
//   

#include "esp_camera.h"
#include <WiFi.h>
#include <PubSubClient.h>
#include <Wire.h>
#include "RTClib.h"
#include "esp_sleep.h"
#include "driver/rtc_io.h"      
#include <HTTPClient.h>

// ---- Sensores deshabilitados (se conservan comentados para las pruebas de datos simulados) ----
// #include <ModbusMaster.h>
// #include "DHT.h"
// #include <Adafruit_INA219.h>

// ============================================================
// MODO PRUEBA
// Descomentado = en CADA arranque hace el diagnostico y manda una
// foto de prueba. Comentalo cuando ya funcione todo.
// ============================================================
//define MODO_PRUEBA

// ============================================================
// IDENTIDAD DEL NODO  (antes el firmware no lo decia en ningun lado)
// ============================================================
#define NODO_ID       "nodo2"
#define TOPIC_STATUS  "esp32/nodo2/status"
#define TOPIC_DATA    "esp32/nodo2/data"
#define TOPIC_CONFIG  "esp32/nodo2/config"   // topico propio del nodo
#define TOPIC_CFG_ALL "esp32/config"         // topico global
#define TOPIC_CFG_OK  "esp32/nodo2/configok" // confirmacion hacia la interfaz
#define TOPIC_TIME    "esp32/nodo2/time"     
#define MQTT_CLIENT_ID "esp32_s3_nodo2"

// ============================================================
// PINES
// ============================================================
#define PIN_BOTON   15      // boton a GND, con pull-up interno
#define LED_PIN     42      // LED indicador

#define I2C_SDA     47      //RTC DS3231
#define I2C_SCL     36

// ---- RS485 / DHT / INA219 deshabilitados ----
// #define TX_PIN 18
// #define RX_PIN 16
// #define DE_RE  17
// #define PIN_CONTROL 15
// #define DHTPIN 41
// #define DHTTYPE DHT22
// const float VOLT_MAX = 12.6f;
// const float VOLT_MIN = 10.5f;

// ============================================================
// RED
// ============================================================
const char* WIFI_SSID = "JJD";
const char* WIFI_PASS = "JJDE0701";

// IMPORTANTE: esta IP debe ser la IP LAN ACTUAL de la PC donde corre todo el servicio
// ipconfig / ip a antes de compilar.
const char* MQTT_HOST = "10.103.235.61";
const uint16_t MQTT_PORT = 1883;
const char* PHOTO_SERVER_URL = "http://10.103.235.61:8081/upload";

WiFiClient   espClient;
PubSubClient mqtt(espClient);
RTC_DS3231   rtc;

// ============================================================
// MEMORIA RTC (sobrevive al deep sleep)
// ============================================================
RTC_DATA_ATTR int  horasConfig[24];     // Tiempo en horas
RTC_DATA_ATTR int  minutosConfig[24];   // Tiempo en minutos
RTC_DATA_ATTR int  numHoras   = 0;
RTC_DATA_ATTR bool modoConfig = false;
RTC_DATA_ATTR uint32_t ultimaMedicion = 0;   
RTC_DATA_ATTR float bateriaSim = 0.0f;       //bateria simulada (sobrevive al deep sleep)

const long TOL_SEG = 120;   // tolerancia de +/- 2 min alrededor del horario

// ============================================================
// SENSORES SIMULADOS
// Descomentado = se publican datos aleatorios en TOPIC_DATA en cada
// ciclo de medicion. Comentalo cuando conectes los sensores reales.
// ============================================================
#define SIMULAR_SENSORES

// Rangos (mismos que simulador_esp32.py)
#define SIM_DHT_T_MIN    18.0f
#define SIM_DHT_T_MAX    32.0f
#define SIM_DHT_H_MIN    45.0f
#define SIM_DHT_H_MAX    90.0f
#define SIM_N_MIN        20
#define SIM_N_MAX        180
#define SIM_P_MIN        10
#define SIM_P_MAX        100
#define SIM_K_MIN        30
#define SIM_K_MAX        250
#define SIM_SHUM_MIN     25.0f
#define SIM_SHUM_MAX     85.0f
#define SIM_STEMP_MIN    17.0f
#define SIM_STEMP_MAX    30.0f
#define SIM_EC_MIN       200
#define SIM_EC_MAX       1800
#define SIM_PH_MIN       5.0f
#define SIM_PH_MAX       7.5f
#define SIM_BAT_MIN      55.0f
#define SIM_BAT_MAX      100.0f

// Bandera que pone el callback cuando llega un horario valido
volatile bool horarioRecibido = false;

// Prototipos (para no depender del auto-prototipado del IDE)
void callback(char* topic, byte* payload, unsigned int length);
bool wifiConnect();
bool mqttConnect(int intentos = 3);
void enviarFoto();

// ============================================================
// LED
// ============================================================
void ledBlink(int veces, int ms_on, int ms_off) {
  for (int i = 0; i < veces; i++) {
    digitalWrite(LED_PIN, HIGH); delay(ms_on);
    digitalWrite(LED_PIN, LOW);  delay(ms_off);
  }
}

void testLED() {
  Serial.println("Test LED GPIO42 - 3 parpadeos...");
  ledBlink(3, 200, 200);
}

// ============================================================
// WIFI  (ahora devuelve bool)
// ============================================================
bool wifiConnect() {
  if (WiFi.status() == WL_CONNECTED) return true;
  Serial.print("Conectando WiFi...");
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  unsigned long t = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - t < 12000) {
    delay(300); Serial.print(".");
  }
  bool ok = (WiFi.status() == WL_CONNECTED);
  Serial.println(ok ? " OK" : " FALLO");
  if (ok) {
    Serial.print("  IP del nodo: "); Serial.println(WiFi.localIP());
    ledBlink(1, 700, 200);          // SEÑAL: WiFi conectado
  }
  return ok;
}

// ============================================================
// MQTT con limite de intentos
// ============================================================
void mqttSetup() {
  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setCallback(callback);
  mqtt.setBufferSize(2048);
  mqtt.setKeepAlive(30);
  mqtt.setSocketTimeout(5);
}

bool mqttConnect(int intentos) {
  if (!wifiConnect()) return false;
  mqttSetup();
  for (int i = 0; i < intentos && !mqtt.connected(); i++) {
    Serial.printf("Conectando MQTT (%d/%d)...", i + 1, intentos);
    if (mqtt.connect(MQTT_CLIENT_ID)) {
      mqtt.subscribe(TOPIC_CONFIG);
      mqtt.subscribe(TOPIC_CFG_ALL);
      mqtt.subscribe(TOPIC_TIME);
      Serial.println(" OK");
      ledBlink(2, 120, 120);        // SEÑAL: MQTT conectado
      return true;
    }
    Serial.printf(" fallo rc=%d\n", mqtt.state());
    delay(1500);
  }
  if (!mqtt.connected()) Serial.println("MQTT no disponible, continuo sin el.");
  return mqtt.connected();
}

void mqttPublish(const char* topic, const char* msg) {
  if (mqtt.connected()) {
    mqtt.publish(topic, msg);
    mqtt.loop();
    delay(120);
  }
}

// ============================================================
// JSON DE CONFIGURACION
// La interfaz manda:
// {"horas":[10,18],"minutos":[30,0],"fecha":"2026-09-21 14:05:33"}
// ============================================================
static int extraerLista(const String& msg, const char* clave, int* destino, int maxN) {
  String key = String("\"") + clave + "\"";
  int idx = msg.indexOf(key);
  if (idx == -1) return -1;
  int start = msg.indexOf('[', idx);
  int end   = msg.indexOf(']', start);
  if (start == -1 || end == -1) return -1;

  String arr = msg.substring(start + 1, end);
  int n = 0, pos = 0;
  while (pos <= (int)arr.length() && n < maxN) {
    int comma = arr.indexOf(',', pos);
    if (comma == -1) comma = arr.length();
    String num = arr.substring(pos, comma);
    num.trim();
    if (num.length() > 0) destino[n++] = num.toInt();
    pos = comma + 1;
  }
  return n;
}

static void sincronizarRTC(const String& msg) {
  int idx = msg.indexOf("\"fecha\"");
  if (idx == -1) return;
  int q1 = msg.indexOf('"', msg.indexOf(':', idx));
  int q2 = msg.indexOf('"', q1 + 1);
  if (q1 == -1 || q2 == -1) return;

  String f = msg.substring(q1 + 1, q2);   // "YYYY-MM-DD HH:MM:SS"
  if (f.length() < 19) return;

  int yy = f.substring(0, 4).toInt();
  int mo = f.substring(5, 7).toInt();
  int dd = f.substring(8, 10).toInt();
  int hh = f.substring(11, 13).toInt();
  int mi = f.substring(14, 16).toInt();
  int ss = f.substring(17, 19).toInt();

  if (yy > 2020) {
    rtc.adjust(DateTime(yy, mo, dd, hh, mi, ss));
    Serial.printf("RTC sincronizado -> %04d-%02d-%02d %02d:%02d:%02d\n",
                  yy, mo, dd, hh, mi, ss);
  }
}

void callback(char* topic, byte* payload, unsigned int length) {
  String msg = "";
  for (unsigned int i = 0; i < length; i++) msg += (char)payload[i];
  Serial.printf("\nMQTT [%s]: %s\n", topic, msg.c_str());

  // ---- v13: la hora llega por su propio topico, sin retain ----
  if (strcmp(topic, TOPIC_TIME) == 0) {
    if (modoConfig) sincronizarRTC(msg);
    else Serial.println("Hora ignorada (solo se acepta en MODO CONFIG)");
    return;
  }

  int h[24], m[24];
  int nH = extraerLista(msg, "horas", h, 24);
  if (nH <= 0) { Serial.println("Clave 'horas' no encontrada o vacia"); return; }
  int nM = extraerLista(msg, "minutos", m, 24);

  numHoras = 0;
  for (int i = 0; i < nH; i++) {
    int hora = h[i];
    int minu = (nM == nH) ? m[i] : 0;   // si no vienen minutos, :00
    if (hora >= 0 && hora <= 23 && minu >= 0 && minu <= 59) {
      horasConfig[numHoras]   = hora;
      minutosConfig[numHoras] = minu;
      numHoras++;
    }
  }

  // NUNCA se toca el RTC aqui. Este mensaje es retenido y llega
  // en cada despertar con la fecha en que se configuro (vieja).

  Serial.printf("Horario guardado (%d): ", numHoras);
  for (int i = 0; i < numHoras; i++)
    Serial.printf("%02d:%02d%s", horasConfig[i], minutosConfig[i],
                  i < numHoras - 1 ? ", " : "\n");

  // Confirmacion hacia la interfaz (publish directo, sin mqtt.loop()
  // dentro del callback para evitar reentrada)
  char ack[256];
  int  p = snprintf(ack, sizeof(ack), "{\"nodo\":\"%s\",\"ok\":true,\"horarios\":[", NODO_ID);
  for (int i = 0; i < numHoras && p < (int)sizeof(ack) - 16; i++)
    p += snprintf(ack + p, sizeof(ack) - p, "\"%02d:%02d\"%s",
                  horasConfig[i], minutosConfig[i], i < numHoras - 1 ? "," : "");
  snprintf(ack + p, sizeof(ack) - p, "]}");
  mqtt.publish(TOPIC_CFG_OK, ack);

  horarioRecibido = true;   // el bucle de config lo usa para el parpadeo
}

// ============================================================
// HORARIOS
// ============================================================
// Distancia en segundos entre "actual" (seg. desde medianoche) y el
// horario i. Positivo = falta; negativo = ya paso. Rango +/- 12 h.
static long distanciaAlHorario(long actual, int i) {
  long prog = horasConfig[i] * 3600L + minutosConfig[i] * 60L;
  long d = prog - actual;
  if (d >  43200L) d -= 86400L;
  if (d < -43200L) d += 86400L;
  return d;
}

static long segundosDelDia(const DateTime& now) {
  return now.hour() * 3600L + now.minute() * 60L + now.second();
}

// Devuelve el indice del horario que toca atender ahora (dentro de
// +/- TOL_SEG) o -1. Evita repetir la foto si ya se hizo hace < 5 min.
int horarioActivo(const DateTime& now) {
  long dt = (long)now.unixtime() - (long)ultimaMedicion;
  if (ultimaMedicion != 0 && dt >= 0 && dt < 300) return -1;

  long actual = segundosDelDia(now);
  for (int i = 0; i < numHoras; i++) {
    long d = distanciaAlHorario(actual, i);
    if (d >= -TOL_SEG && d <= TOL_SEG) return i;
  }
  return -1;
}

// Cuantos segundos dormir. El timer interno del ESP32 tiene error de
// varios %, asi que si falta mucho se duerme solo el 90 % y se vuelve
// a calcular con el DS3231; cerca del horario se despierta 25 s antes.
uint64_t segundosDeSueno(const DateTime& now) {
  if (numHoras == 0) return 3600ULL;
  long actual = segundosDelDia(now);
  long mejor  = 2 * 86400L;
  for (int i = 0; i < numHoras; i++) {
    long d = distanciaAlHorario(actual, i);
    if (d <= 0) d += 86400L;              // ya paso hoy -> manana
    if (d < mejor) mejor = d;
  }
  Serial.printf("Proximo horario en %ld s\n", mejor);
  if (mejor > 300) return (uint64_t)(mejor - mejor / 10);
  if (mejor > 30)  return (uint64_t)(mejor - 25);
  return 1ULL;
}

// Espera activa (leyendo el DS3231) hasta que llegue la hora exacta.
void esperarHoraExacta(int idx) {
  while (true) {
    DateTime n = rtc.now();
    long d = distanciaAlHorario(segundosDelDia(n), idx);
    if (d <= 0 || d > TOL_SEG) break;
    Serial.printf("Faltan %ld s para el horario...\n", d);
    if (mqtt.connected()) mqtt.loop();
    delay(d > 2 ? 1000 : 200);
  }
}

// ============================================================
// CAMARA
// ============================================================
bool initCamera() {
  camera_config_t config;
  config.ledc_channel  = LEDC_CHANNEL_0;
  config.ledc_timer    = LEDC_TIMER_0;
  config.pin_d0 = 5;  config.pin_d1 = 3;
  config.pin_d2 = 2;  config.pin_d3 = 4;
  config.pin_d4 = 6;  config.pin_d5 = 8;
  config.pin_d6 = 9;  config.pin_d7 = 11;
  config.pin_xclk     = 10;
  config.pin_pclk     = 7;
  config.pin_vsync    = 13;
  config.pin_href     = 12;
  config.pin_sccb_sda = 21;
  config.pin_sccb_scl = 14;
  config.pin_pwdn     = -1;
  config.pin_reset    = -1;
  config.xclk_freq_hz = 20000000;
  config.pixel_format = PIXFORMAT_JPEG;
  config.frame_size   = FRAMESIZE_QVGA;
  config.jpeg_quality = 12;

  // ============================================================
  // FIX: "frame buffer malloc failed"
  // El buffer de la foto necesita PSRAM (la RAM interna normal del
  // ESP32-S3 es muy chica para un frame JPEG). Si la placa tiene
  // PSRAM Y esta habilitada en Herramientas > PSRAM del Arduino IDE,
  // psramFound() devuelve true y usamos 2 buffers en PSRAM para
  // mas margen. Si no hay PSRAM disponible, bajamos calidad/tamano
  // para que quepa en la RAM interna en vez de fallar.
  // ============================================================
  if (psramFound()) {
    config.fb_location = CAMERA_FB_IN_PSRAM;
    config.fb_count     = 2;
    config.grab_mode    = CAMERA_GRAB_LATEST;
    Serial.println("PSRAM detectada: usando 2 buffers en PSRAM.");
  } else {
    config.fb_location = CAMERA_FB_IN_DRAM;
    config.fb_count     = 1;
    config.frame_size   = FRAMESIZE_QQVGA;  // 160x120: cabe en RAM interna
    config.jpeg_quality = 15;
    config.grab_mode    = CAMERA_GRAB_WHEN_EMPTY;
    Serial.println("AVISO: sin PSRAM. Resolucion reducida a QQVGA para que quepa en RAM interna.");
    Serial.println("       Revisa Herramientas > PSRAM en el Arduino IDE (debe decir 'OPI PSRAM').");
  }

  esp_err_t err = esp_camera_init(&config);
  if (err != ESP_OK) { Serial.printf("Error camara: 0x%x\n", err); return false; }
  Serial.println("Camara OK");
  return true;
}

// ============================================================
// ENVÍO DE FOTOS POR HTTP:
// En este apartado se agregó el envío de las fotos por medio de HTTP.
// De esta forma se cambia el formato anterior para no enviarlas en Base64,
// logrando un sistema mucho más ligero y menos exigente para el nodo.
// ============================================================
void enviarFoto() {
  Serial.println("Capturando foto...");

  camera_fb_t* warm = esp_camera_fb_get();   
  if (warm) esp_camera_fb_return(warm);
  delay(200);

  camera_fb_t* fb = esp_camera_fb_get();
  if (!fb) { Serial.println("Error: no se pudo obtener frame"); return; }

  if (!wifiConnect()) {
    Serial.println("Sin WiFi, no se puede enviar la foto");
    esp_camera_fb_return(fb);
    ledBlink(1, 800, 0);
    return;
  }

  HTTPClient http;
  String url = String(PHOTO_SERVER_URL) + "?nodo=" + NODO_ID + "&ts=" + String(millis());
  http.begin(url);
  http.addHeader("Content-Type", "image/jpeg");
  http.setTimeout(10000);

  Serial.printf("Enviando foto (%d bytes) a %s ...\n", fb->len, url.c_str());
  int httpCode = http.POST(fb->buf, fb->len);

  if (httpCode > 0) {
    Serial.printf("Respuesta HTTP: %d\n", httpCode);
    if (httpCode == 200) { Serial.println("Foto enviada correctamente"); ledBlink(2, 100, 100); }
    else                 { Serial.println("Codigo distinto de 200");     ledBlink(4, 80, 80);   }
  } else {
    Serial.printf("Error al enviar: %s\n", http.errorToString(httpCode).c_str());
    ledBlink(1, 800, 0);
  }

  http.end();
  esp_camera_fb_return(fb);
}

// ============================================================
// MODO CONFIG
// Espera el JSON de horario de la interfaz. Sale con el boton o
// por timeout (3 min) para no agotar la bateria si se olvida.
// ============================================================
void entrarModoConfig() {
  modoConfig = true;
  horarioRecibido = false;

  Serial.println("\n=== MODO CONFIG ACTIVO ===");
  Serial.printf("Publica en %s o %s -> {\"horas\":[10,18],\"minutos\":[30,0]}\n",
                TOPIC_CONFIG, TOPIC_CFG_ALL);
  Serial.println("Presiona el boton otra vez para salir.\n");

  mqttConnect(5);
  mqttPublish(TOPIC_STATUS, "config_on");

  unsigned long inicio   = millis();
  unsigned long tLed     = 0;
  bool          ledOn    = false;
  const unsigned long TIMEOUT_MS = 180000UL;   // 3 minutos

  while (millis() - inicio < TIMEOUT_MS) {
    if (!mqtt.connected()) mqttConnect(1);
    mqtt.loop();                                // <-- ahora corre muy seguido

    // Parpadeo lento no bloqueante (500/500)
    if (millis() - tLed >= 500) {
      tLed = millis();
      ledOn = !ledOn;
      digitalWrite(LED_PIN, ledOn ? HIGH : LOW);
    }

    // Aviso visual de horario recibido
    if (horarioRecibido) {
      horarioRecibido = false;
      digitalWrite(LED_PIN, LOW);
      ledBlink(5, 80, 80);                      // SEÑAL: horario guardado OK
      Serial.println("Horario aplicado. Puedes salir con el boton.");
    }

    // Salida por boton
    if (digitalRead(PIN_BOTON) == LOW) {
      delay(50);
      if (digitalRead(PIN_BOTON) == LOW) {
        while (digitalRead(PIN_BOTON) == LOW) delay(10);
        delay(60);
        break;
      }
    }
    delay(5);
  }

  digitalWrite(LED_PIN, LOW);
  modoConfig = false;
  mqttPublish(TOPIC_STATUS, "config_off");
  Serial.println("Modo config desactivado.\n");
}

// ============================================================
// DATOS SIMULADOS DE SENSORES
// Genera valores aleatorios dentro de los rangos y los publica por
// MQTT con el mismo JSON que espera la interfaz (igual al simulador
// en Python). Reemplaza a leerSensor/leerDHT/leerBateria/publicarTodo
// mientras no haya sensores fisicos.
// ============================================================
#ifdef SIMULAR_SENSORES

// Float aleatorio en [minimo, maximo] usando el RNG por hardware
static float randFloat(float minimo, float maximo) {
  return minimo + (maximo - minimo) * ((esp_random() % 100001UL) / 100000.0f);
}

// Entero aleatorio en [minimo, maximo]
static int randInt(int minimo, int maximo) {
  return minimo + (int)(esp_random() % (uint32_t)(maximo - minimo + 1));
}

// La bateria simulada va bajando poco a poco entre ciclos (como en el .py)
static float bateriaSimulada() {
  if (bateriaSim < SIM_BAT_MIN || bateriaSim > SIM_BAT_MAX)
    bateriaSim = randFloat(SIM_BAT_MIN, SIM_BAT_MAX);   // primer ciclo

  bateriaSim -= randFloat(0.0f, 0.12f);                 // consumo lento
  if (bateriaSim < 20.0f) bateriaSim += randFloat(0.5f, 2.0f);
  if (bateriaSim < SIM_BAT_MIN) bateriaSim = SIM_BAT_MIN;
  if (bateriaSim > SIM_BAT_MAX) bateriaSim = SIM_BAT_MAX;
  return bateriaSim;
}

void publicarDatosSimulados() {
  float dhtT   = randFloat(SIM_DHT_T_MIN, SIM_DHT_T_MAX);
  float dhtH   = randFloat(SIM_DHT_H_MIN, SIM_DHT_H_MAX);
  int   sN     = randInt(SIM_N_MIN, SIM_N_MAX);
  int   sP     = randInt(SIM_P_MIN, SIM_P_MAX);
  int   sK     = randInt(SIM_K_MIN, SIM_K_MAX);
  float sHum   = randFloat(SIM_SHUM_MIN, SIM_SHUM_MAX);
  float sTemp  = randFloat(SIM_STEMP_MIN, SIM_STEMP_MAX);
  int   sEC    = randInt(SIM_EC_MIN, SIM_EC_MAX);
  float sPH    = randFloat(SIM_PH_MIN, SIM_PH_MAX);
  float bat    = bateriaSimulada();

  char json[256];
  snprintf(json, sizeof(json),
    "{\"dht\":{\"t\":%.1f,\"h\":%.1f},"
    "\"suelo\":{\"N\":%d,\"P\":%d,\"K\":%d,\"hum\":%.1f,\"temp\":%.1f,\"ec\":%d,\"ph\":%.2f},"
    "\"bateria\":{\"porcentaje\":%.1f}}",
    dhtT, dhtH, sN, sP, sK, sHum, sTemp, sEC, sPH, bat);

  Serial.printf("Datos simulados -> %s\n  %s\n", TOPIC_DATA, json);

  // Si el MQTT se cayo, un intento de reconexion
  if (!mqtt.connected()) mqttConnect(2);

  if (mqtt.connected()) {
    bool ok = mqtt.publish(TOPIC_DATA, json);
    mqtt.loop();
    delay(120);
    Serial.println(ok ? "Datos simulados publicados OK" : "Error al publicar datos simulados");
  } else {
    Serial.println("Sin MQTT: datos simulados no enviados");
  }
}
#endif

// ============================================================
// CICLO DE MEDICION (foto + futuros sensores)
// ============================================================
void cicloMedicion() {
  Serial.println("Iniciando ciclo de medicion...");
  digitalWrite(LED_PIN, HIGH);

  Wire.begin(I2C_SDA, I2C_SCL);
  bool cam_ok = initCamera();

  // ---- Sensores deshabilitados ----
  // leerSensor(); leerDHT(); leerBateria(); publicarTodo();

  // v14: datos simulados (se publican justo antes de la foto)
  #ifdef SIMULAR_SENSORES
    publicarDatosSimulados();
  #endif

  if (cam_ok) enviarFoto();
  else Serial.println("Foto omitida (camara no disponible)");

  digitalWrite(LED_PIN, LOW);
  ultimaMedicion = rtc.now().unixtime();   // v13: anti-doble-foto
  Serial.println("Ciclo terminado.");
}

// ============================================================
// MODO PRUEBA
// ============================================================
void modoPrueba() {
  Serial.println("\n===== MODO PRUEBA =====");
  digitalWrite(LED_PIN, HIGH);

  // Diagnostico de PSRAM (relacionado con el fix de la camara)
  Serial.printf("  PSRAM detectada : %s\n", psramFound() ? "SI" : "NO");
  if (psramFound())
    Serial.printf("  PSRAM libre     : %u KB\n", ESP.getFreePsram() / 1024);

  Wire.begin(I2C_SDA, I2C_SCL);
  bool rtc_ok = rtc.begin();
  Serial.printf("  RTC DS3231 : %s\n", rtc_ok ? "OK" : "FALLO");
  if (rtc_ok) {
    DateTime now = rtc.now();
    Serial.printf("  Hora RTC   : %04d-%02d-%02d %02d:%02d:%02d\n",
                  now.year(), now.month(), now.day(),
                  now.hour(), now.minute(), now.second());
    if (rtc.lostPower())
      Serial.println("  AVISO: el RTC perdio la hora. Manda una config desde la interfaz para sincronizarlo.");
  }

  bool cam_ok = initCamera();
  Serial.printf("  Camara     : %s\n", cam_ok ? "OK" : "FALLO");
  Serial.printf("  WiFi       : %s\n", WiFi.status() == WL_CONNECTED ? "OK" : "FALLO");
  Serial.printf("  MQTT       : %s\n", mqtt.connected() ? "OK" : "FALLO");

  #ifdef SIMULAR_SENSORES
    publicarDatosSimulados();    // v14
  #endif

  if (cam_ok) enviarFoto();

  digitalWrite(LED_PIN, LOW);
  Serial.println("===== FIN PRUEBA =====\n");
}

// ============================================================
// DEEP SLEEP
// ============================================================
void dormir(uint64_t segundos) {
  Serial.printf("Durmiendo %llu s (%.1f min)...\n", segundos, segundos / 60.0);
  Serial.flush();

  WiFi.disconnect(true);
  WiFi.mode(WIFI_OFF);
  digitalWrite(LED_PIN, LOW);

  // Pull-up del boton mantenido durante el deep sleep (imprescindible)
  rtc_gpio_pullup_en((gpio_num_t)PIN_BOTON);
  rtc_gpio_pulldown_dis((gpio_num_t)PIN_BOTON);

  // Si el boton quedo presionado, espero a que lo suelten para no
  // despertar de inmediato en un bucle.
  while (digitalRead(PIN_BOTON) == LOW) delay(10);

  esp_sleep_enable_ext0_wakeup((gpio_num_t)PIN_BOTON, 0);  // LOW = presionado
  esp_sleep_enable_timer_wakeup(segundos * 1000000ULL);
  esp_deep_sleep_start();
}

// ============================================================
// VOID SETUP
// ============================================================
void setup() {
  Serial.begin(115200);
  delay(500);

  pinMode(LED_PIN, OUTPUT);
  digitalWrite(LED_PIN, LOW);
  pinMode(PIN_BOTON, INPUT_PULLUP);

  // El pin sale del estado de deep sleep "congelado": lo libero
  rtc_gpio_deinit((gpio_num_t)PIN_BOTON);
  pinMode(PIN_BOTON, INPUT_PULLUP);

  esp_sleep_wakeup_cause_t causa = esp_sleep_get_wakeup_cause();
  Serial.printf("\n=== NODO %s ===  causa de despertar: %d\n", NODO_ID, (int)causa);

  testLED();                                   // 3 parpadeos

  //  I2C + RTC ANTES de la red
  Wire.begin(I2C_SDA, I2C_SCL);
  bool rtcOk = rtc.begin();
  Serial.printf("RTC DS3231: %s\n", rtcOk ? "OK" : "FALLO");
  if (rtcOk) {
    DateTime t = rtc.now();
    Serial.printf("Hora RTC al despertar: %04d-%02d-%02d %02d:%02d:%02d\n",
                  t.year(), t.month(), t.day(), t.hour(), t.minute(), t.second());
  }

  // --- Red ---
  wifiConnect();
  mqttConnect(3);
  mqttPublish(TOPIC_STATUS, "hola");
  // Da tiempo a que llegue el horario retenido del broker
  for (int i = 0; i < 10; i++) { mqtt.loop(); delay(50); }

  // ============================================================
  // ¿Hay que entrar a MODO CONFIG?
  //   a) desperto por el boton (EXT0), o
  //   b) primer arranque / reset y el usuario presiona el boton
  //      en la ventana de 4 s
  // ============================================================
  bool entrarConfig = (causa == ESP_SLEEP_WAKEUP_EXT0);

  if (!entrarConfig && causa != ESP_SLEEP_WAKEUP_TIMER) {
    Serial.println("Ventana de 4 s: presiona el boton para entrar a MODO CONFIG...");
    unsigned long t0 = millis();
    while (millis() - t0 < 4000) {
      ledBlink(2, 60, 60); delay(300);         // parpadeo doble = ventana abierta
      if (digitalRead(PIN_BOTON) == LOW) { entrarConfig = true; break; }
      mqtt.loop();
    }
    digitalWrite(LED_PIN, LOW);
  }

  if (entrarConfig) {
    while (digitalRead(PIN_BOTON) == LOW) delay(10);   // antirrebote
    delay(60);
    entrarModoConfig();
  }
  else {
    // ------- Despertar normal: ¿toca medir? -------
    DateTime now = rtc.now();
    Serial.printf("Hora RTC: %02d:%02d:%02d\n", now.hour(), now.minute(), now.second());

    #ifdef MODO_PRUEBA
      modoPrueba();
    #else
      if (numHoras == 0) {
        Serial.println("Sin horario configurado. Usa el boton + la interfaz.");
      } else {
        int idx = horarioActivo(now);
        if (idx >= 0) {
          Serial.printf("Toca medir (horario %02d:%02d).\n", horasConfig[idx], minutosConfig[idx]);
          esperarHoraExacta(idx);            // v13: foto a la hora exacta
          cicloMedicion();
        } else {
          Serial.println("No toca medir -> a dormir.");
        }
      }
    #endif
  }

  // --- Despedida ---
  if (!mqtt.connected()) mqttConnect(2);
  mqttPublish(TOPIC_STATUS, "chao");

  // --- Dormir hasta el proximo horario ---
  DateTime ahora = rtc.now();
  uint64_t segs  = segundosDeSueno(ahora);
  dormir(segs);
}

// ============================================================
// VOID LOOP
// ============================================================
void loop() {}