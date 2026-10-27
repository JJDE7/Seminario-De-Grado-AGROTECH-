SISTEMA DE MONITOREO DE INVERNADERO - NODO 2
ESP32-S3 con sensores de suelo, ambiente, bateria y camara
============================================================


DESCRIPCION GENERAL
-------------------
Este firmware controla un nodo de monitoreo agricola basado en el microcontrolador
ESP32-S3. El nodo se despierta cada hora, revisa si la hora actual coincide con el
horario programado, toma mediciones de los sensores, las publica por MQTT a una
Raspberry Pi y vuelve a dormir. Todo el sistema esta disenado para funcionar con
bateria y panel solar, por eso el ahorro de energia es una prioridad.


COMPONENTES DEL SISTEMA
-----------------------
- ESP32-S3: microcontrolador principal con WiFi integrado
- Sensor de suelo NPK (RS485/Modbus): mide pH, humedad, temperatura, EC, N, P y K
- DHT22: mide temperatura y humedad del aire
- INA219: mide voltaje y corriente de la bateria
- DS3231: modulo reloj de tiempo real con bateria de respaldo
- OV2640: camara que captura fotos del cultivo
- Raspberry Pi: servidor con broker MQTT (Mosquitto) en IP 192.168.1.101


COMO FUNCIONA EL CICLO DE VIDA DEL FIRMWARE
--------------------------------------------
El ESP32 no usa el loop() de Arduino. En cambio, todo ocurre en setup():

  1. Se inicializa el LED y se hace un test de 3 parpadeos.
  2. Se conecta al WiFi y MQTT, y publica "hola" para avisar que desperto.
  3. Si MODO_PRUEBA esta activo, ejecuta el diagnostico completo.
  4. Publica "chao" para avisar que va a dormir.
  5. Revisa por que se desperto (boton o temporizador).
  6. Si desperto por temporizador, compara la hora actual con el horario guardado.
     Si corresponde medir, ejecuta el ciclo completo de sensores y publicacion.
  7. Configura las fuentes de despertar y entra en deep sleep por 1 hora.

Al despertar del deep sleep, el chip se resetea y vuelve a ejecutar setup().
El loop() queda vacio porque nunca se llega a el.


DEEP SLEEP Y AHORRO DE ENERGIA
--------------------------------
El ESP32 activo consume entre 100 y 250 mA. En deep sleep consume menos de 0.01 mA.
Para un sistema con bateria de 12V y panel solar pequeno, esto es la diferencia entre
durar dias o durar meses.

Hay dos formas de despertar al chip:
  - Timer interno: cada 3600 segundos (1 hora exacta).
  - Pin externo EXT0: cuando el usuario presiona el boton en GPIO0.

La variable "causa" guarda el motivo del despertar y el firmware toma decisiones
distintas segun sea timer o boton.


MEMORIA RTC: DATOS QUE SOBREVIVEN AL DEEP SLEEP
-------------------------------------------------
Las variables marcadas con RTC_DATA_ATTR se guardan en una region especial de RAM
que el chip mantiene encendida incluso en deep sleep. Se usan tres:

  - horasConfig[24]: arreglo con las horas en que se debe medir (ej: 6, 12, 18)
  - numHoras: cantidad de horas guardadas en el arreglo
  - modoConfig: bandera que indica si el modo de configuracion esta activo

Sin este mecanismo, el horario se perderia cada vez que el chip duerme y habria que
reconfigurarlo en cada ciclo.


SENSOR DE SUELO POR RS485 Y MODBUS RTU
----------------------------------------
El sensor mide siete parametros del suelo:
  - pH: acidez/alcalinidad (rango tipico 4.0 a 9.0)
  - Humedad volumetrica del suelo (en porcentaje)
  - Temperatura del suelo (en grados Celsius)
  - EC (conductividad electrica, en uS/cm): indica concentracion de sales y nutrientes
  - Nitrogeno (N), Fosforo (P) y Potasio (K) en mg/kg

El protocolo de comunicacion es Modbus RTU sobre un bus fisico RS485.
El ESP32 actua como maestro (hace preguntas) y el sensor como esclavo (responde).
La comunicacion ocurre por Serial2 a 9600 baudios.

El pin GPIO17 (DE_RE) controla la direccion del bus RS485, que es half-duplex:
  - HIGH: el modulo esta transmitiendo (enviando la pregunta)
  - LOW: el modulo esta escuchando (esperando la respuesta)

La libreria ModbusMaster llama automaticamente a preTransmission() y postTransmission()
para cambiar este pin antes y despues de cada comunicacion.

Los valores que entrega el sensor son enteros que hay que escalar:
  - pH: dividir por 100 (el sensor entrega 650 para un pH de 6.50)
  - Humedad y temperatura: dividir por 10

Para reducir el error por ruido electrico, se toman 6 muestras de cada variable
y se calcula el promedio. Si alguna lectura falla, no se incluye en el calculo.
Esto se logra con contadores independientes (c_ph, c_hum, etc.) que solo aumentan
cuando la lectura fue exitosa.


SENSOR DHT22
------------
Mide temperatura y humedad del aire ambiente. Se conecta a GPIO41.
Necesita al menos 2 segundos para estabilizarse despues de encenderse, por eso
siempre hay un delay(2000) despues de dht.begin().

Si la lectura falla, las funciones readTemperature() y readHumidity() devuelven NaN
(Not a Number). Antes de publicar, se verifica con isnan() y se reemplaza por 0
para no generar un JSON invalido.


BATERIA CON INA219
------------------
El INA219 es un sensor de voltaje y corriente con interfaz I2C en direccion 0x45.
Se calibra con setCalibration_16V_400mA() para el rango de operacion del sistema.

El porcentaje de carga se calcula por interpolacion lineal:
  porcentaje = ((voltaje - VOLT_MIN) / (VOLT_MAX - VOLT_MIN)) * 100

Donde VOLT_MIN es 10.5V (bateria vacia) y VOLT_MAX es 12.6V (bateria llena).
La funcion constrain() asegura que el resultado no salga del rango 0-100%.

Si el porcentaje baja del 20%, el firmware imprime una advertencia. Esto se podria
extender para enviar una alerta por MQTT a la Raspberry Pi.


MQTT: COMUNICACION CON LA RASPBERRY PI
----------------------------------------
El broker MQTT es Mosquitto corriendo en la Raspberry Pi (192.168.1.101:1883).
La libreria PubSubClient gestiona la conexion desde el ESP32.

Topicos que usa el nodo:
  - esp32/nodo2/status  -> publica "hola" al despertar y "chao" antes de dormir
  - esp32/nodo2/data    -> publica el JSON con todos los datos de sensores
  - cam/start           -> avisa el tamano total de la foto antes de enviarla
  - cam/data            -> fragmentos de 512 caracteres de la foto en Base64
  - cam/end             -> senal de que la foto termino
  - esp32/config        -> escucha este topico para recibir el horario

El buffer MQTT se configura en 2048 bytes (mqtt.setBufferSize(2048)) porque el
JSON con todos los sensores supera el limite predeterminado de 256 bytes.


CAMARA OV2640: CAPTURA Y ENVIO DE FOTOS
-----------------------------------------
La camara captura imagenes JPEG en resolucion QVGA (320x240 pixeles).
Se eligio esta resolucion porque es suficiente para ver el estado del cultivo
y genera archivos pequenos que se pueden transmitir por MQTT.

MQTT no soporta datos binarios directamente, por eso la imagen JPEG se convierte
a Base64, un esquema que representa bytes usando solo 64 caracteres de texto.
La conversion aumenta el tamano aproximadamente un 33%.

La foto se envia en fragmentos de 512 caracteres para no superar el buffer MQTT.
El receptor (Raspberry Pi) debe escuchar los tres topicos (cam/start, cam/data,
cam/end) y ensamblar los fragmentos para reconstruir la imagen.


CONFIGURACION DEL HORARIO SIN REPROGRAMAR
------------------------------------------
El horario de medicion se puede cambiar de forma remota sin conectar el ESP32
al computador. El proceso es:

  1. Presionar el boton fisico (GPIO0) para activar el modo configuracion.
     El LED empieza a parpadear lentamente (500ms encendido / 500ms apagado).

  2. Desde cualquier computador en la red, publicar el horario en MQTT:
     mosquitto_pub -h 192.168.1.101 -t esp32/config -m '{"horas":[6,12,18]}'

  3. El ESP32 recibe el mensaje en la funcion callback() y extrae las horas
     del JSON usando busqueda de cadenas (indexOf, substring). No se usa una
     libreria JSON para ahorrar memoria.

  4. Presionar el boton de nuevo para salir del modo configuracion.
     El nodo entra en deep sleep con el nuevo horario guardado en memoria RTC.


MODO PRUEBA
-----------
Al inicio del codigo hay una directiva:
  #define MODO_PRUEBA

Mientras esta linea exista, cada vez que el ESP32 encienda ejecutara modoPrueba(),
que hace un diagnostico completo del sistema en 5 pasos: hardware, red, sensor de
suelo, DHT22 y bateria, y publicacion MQTT con foto.

Para desactivarlo en produccion basta comentar esa linea:
  // #define MODO_PRUEBA

Esto es mas limpio que borrar el codigo de prueba, porque se puede volver a activar
facilmente durante el mantenimiento del nodo.


ESTRUCTURA DEL JSON PUBLICADO
-------------------------------
El topico esp32/nodo2/data recibe un JSON con esta estructura:

{
  "dht":    { "t": 24.5, "h": 68.2 },
  "suelo":  { "N": 45, "P": 32, "K": 78, "hum": 42.1, "temp": 22.3, "ec": 310, "ph": 6.50 },
  "bateria":{ "porcentaje": 87.4, "voltaje": 12.31, "corriente_mA": 45.2 }
}

Si algun sensor fallo, su valor aparece como 0.


COMANDOS UTILES PARA MONITOREAR DESDE LA RASPBERRY PI
-------------------------------------------------------
Escuchar todos los mensajes del nodo:
  mosquitto_sub -h 192.168.1.101 -t "esp32/#" -t "cam/#" -v

Escuchar solo los datos del sensor:
  mosquitto_sub -h 192.168.1.101 -t esp32/nodo2/data -v

Enviar un nuevo horario de medicion:
  mosquitto_pub -h 192.168.1.101 -t esp32/config -m '{"horas":[6,12,18]}'


============================================================
Proyecto Invernadero UQ - Nodo 2
============================================================