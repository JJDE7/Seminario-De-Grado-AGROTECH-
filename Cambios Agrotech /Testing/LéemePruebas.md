# AgroTech – Simulador ESP32 para pruebas de comunicación y Machine Learning

## Descripción

Este programa permite emular el comportamiento de uno o varios nodos ESP32 pertenecientes al sistema **AgroTech**, generando y transmitiendo datos simulados de sensores y fotografías de plantas para realizar pruebas de integración y funcionamiento.

Su propósito principal es verificar la correcta comunicación entre los nodos, el servidor y los componentes de almacenamiento, además de evaluar la respuesta del modelo de Machine Learning ante diferentes escenarios de comportamiento de las plantas de tomate, no debe ser desplegado junto con la integración final del sistema.

El simulador permite modificar dinámicamente las condiciones de los sensores desde una consola interactiva, facilitando la generación de escenarios normales y anómalos sin necesidad de intervenir físicamente los dispositivos ESP32.

## Características principales

- **Simulación de sensores:** generación de mediciones ambientales, parámetros del suelo, nutrientes, batería y radiación solar.
- **Comunicación MQTT:** publicación periódica de datos mediante los tópicos `esp32/<nodo>/data`.
- **Envío de fotografías HTTP:** transmisión periódica de imágenes mediante solicitudes POST al servidor.
- **Simulación de múltiples nodos:** posibilidad de configurar los identificadores de los nodos ESP32 que se desean emular.
- **Pruebas de Machine Learning:** generación de escenarios normales, anomalías individuales y condiciones ambientales o nutricionales correlacionadas.
- **Consola interactiva:** modificación de las condiciones simuladas en tiempo de ejecución.
- **Recepción de configuraciones:** escucha de configuraciones de horarios mediante MQTT y reconstrucción de los horarios recibidos.
- **Confirmación de configuraciones:** publicación de respuestas en los tópicos `esp32/<nodo>/configok`, simulando la confirmación de recepción por parte de los ESP32.
- **Control del piramómetro:** posibilidad de incluir o excluir los datos de radiación solar en las publicaciones MQTT.

## Arquitectura de comunicación

El simulador utiliza dos mecanismos de comunicación independientes:

| Protocolo | Función | Dirección |
|---|---|---|
| MQTT | Envío de mediciones de sensores | Simulador → Servidor |
| HTTP POST | Envío de fotografías | Simulador → Servidor |
| MQTT | Recepción de configuraciones de horarios | Servidor → Simulador |
| MQTT | Confirmación de configuraciones | Simulador → Servidor |

### Tópicos MQTT

| Tópico | Descripción |
|---|---|
| `esp32/<nodo>/data` | Publicación de mediciones simuladas. |
| `esp32/config` | Recepción de configuraciones generales de horarios. |
| `esp32/<nodo>/config` | Recepción de configuraciones individuales. |
| `esp32/<nodo>/configok` | Confirmación de recepción de horarios. |

### Transmisión de fotografías

Las fotografías se envían mediante solicitudes HTTP POST a:

`http://<servidor>:8080/upload?nodo=<nodo>`

El simulador selecciona aleatoriamente una imagen de la carpeta configurada y la transmite utilizando su tipo MIME correspondiente.

## Parámetros simulados

El programa genera valores aleatorios dentro de rangos definidos para cada escenario.

| Grupo | Parámetros |
|---|---|
| Ambiente | Temperatura y humedad ambiental. |
| Suelo | Humedad y temperatura del suelo. |
| Nutrientes | Nitrógeno (N), fósforo (P), potasio (K), conductividad eléctrica (EC) y pH. |
| Hardware | Porcentaje de batería. |
| Radiación | Radiación solar medida por el piramómetro. |

## Modos de simulación

Una de las funcionalidades principales es la posibilidad de modificar los parámetros generados en tiempo real para representar diferentes condiciones de una planta de tomate.

### Condición normal

- `sana`: genera mediciones dentro de los rangos establecidos para una planta en condiciones normales.

### Anomalías ambientales

- `ambiente_alta`: temperatura y humedad ambiental elevadas.
- `ambiente_baja`: temperatura y humedad ambiental reducidas.
- `ambiente_caliente_seco`: temperatura elevada y humedad ambiental reducida.
- `ambiente_frio_humedo`: temperatura reducida y humedad ambiental elevada.

### Anomalías del suelo

- `suelo_alta`: humedad y temperatura del suelo elevadas.
- `suelo_baja`: humedad y temperatura del suelo reducidas.
- `suelo_caliente_seco`: temperatura elevada y humedad reducida.
- `suelo_frio_humedo`: temperatura reducida y humedad elevada.

### Anomalías nutricionales

- `nutrientes_alta`: concentraciones elevadas de NPK, EC y pH.
- `nutrientes_baja`: concentraciones reducidas de NPK, EC y pH.
- `nutrientes_exceso`: exceso de nutrientes, elevada salinidad y reducción del pH.
- `nutrientes_lavado`: concentraciones reducidas de nutrientes y EC, con incremento del pH.

### Anomalías de radiación

- `radiacion_alta`: radiación solar elevada.
- `radiacion_baja`: radiación solar reducida.

**Nota:** estos escenarios representan condiciones sintéticas para pruebas de software y Machine Learning. No constituyen diagnósticos agronómicos ni garantizan que las combinaciones generadas representen todos los comportamientos fisiológicos reales del cultivo.

## Instalación y configuración

### Requisitos

- Python 3.x
- Broker MQTT accesible desde el equipo donde se ejecuta el simulador.
- Servidor AgroTech con recepción MQTT y HTTP habilitada.
- Carpeta con fotografías de plantas en alguno de los siguientes formatos: JPG, JPEG, PNG, BMP o WEBP.

### Dependencias

Instalar la biblioteca necesaria mediante:

```bash
pip install paho-mqtt
```

Las demás bibliotecas utilizadas pertenecen a la biblioteca estándar de Python.

### Configuración inicial

Antes de ejecutar el programa, es necesario modificar las variables de configuración ubicadas al inicio del archivo:

```python
MQTT_BROKER = "localhost"
MQTT_PORT = 1883

HTTP_SERVER_HOST = "127.0.0.1"
HTTP_PHOTO_PORT = 8080
HTTP_PHOTO_PATH = "/upload"

IMAGE_DIR = r"C:\Users\Usuario\Desktop\fotos de plantas para agrotech"

SIMULATED_NODES = [
    "nodo2"
]

SENSOR_INTERVAL_SECONDS = 15
PHOTO_INTERVAL_SECONDS = 15
SEND_PHOTO_ON_START = True
```

**Consideraciones:**

- `MQTT_BROKER`: dirección del broker MQTT.
- `HTTP_SERVER_HOST`: dirección del servidor encargado de recibir las fotografías.
- `IMAGE_DIR`: ruta local de las imágenes utilizadas en la simulación.
- `SIMULATED_NODES`: identificadores de los nodos que se desean emular.
- `SENSOR_INTERVAL_SECONDS`: intervalo de publicación de mediciones.
- `PHOTO_INTERVAL_SECONDS`: intervalo de envío de fotografías.
- `SEND_PHOTO_ON_START`: determina si se envía una fotografía al iniciar el simulador.

También se pueden modificar los parámetros relacionados con la confirmación de horarios:

```python
CONFIRM_HORARIOS = True
CONFIRM_DELAY_SECONDS = (1.0, 3.0)
CONFIRM_PROBABILITY = 1.0
```

Estos valores permiten habilitar las confirmaciones, establecer un retraso aleatorio y controlar la probabilidad de respuesta.

## Ejecución

Una vez configurados los parámetros, ejecutar:

```bash
python simulador_esp32_con_recepcion_v4.py
```

Al iniciar, el programa establece la conexión MQTT, inicia los hilos encargados de transmitir mediciones y fotografías y habilita la consola interactiva.

## Consola interactiva

Los comandos pueden introducirse directamente mientras el simulador está ejecutándose, sin interrumpir los procesos de transmisión.

| Comando | Función |
|---|---|
| `sana` | Restablece las condiciones normales. |
| `ambiente_alta` | Activa una anomalía ambiental elevada. |
| `ambiente_baja` | Activa una anomalía ambiental reducida. |
| `ambiente_caliente_seco` | Simula condiciones ambientales calientes y secas. |
| `ambiente_frio_humedo` | Simula condiciones ambientales frías y húmedas. |
| `suelo_alta` | Activa una anomalía elevada en el suelo. |
| `suelo_baja` | Activa una anomalía reducida en el suelo. |
| `suelo_caliente_seco` | Simula un suelo caliente y seco. |
| `suelo_frio_humedo` | Simula un suelo frío y húmedo. |
| `nutrientes_alta` | Simula concentraciones elevadas de nutrientes. |
| `nutrientes_baja` | Simula concentraciones reducidas de nutrientes. |
| `nutrientes_exceso` | Simula exceso de fertilizante y salinidad. |
| `nutrientes_lavado` | Simula lixiviación y pérdida de nutrientes. |
| `radiacion_alta` | Simula radiación solar elevada. |
| `radiacion_baja` | Simula radiación solar reducida. |
| `pira on` | Habilita el envío de datos del piramómetro. |
| `pira off` | Excluye los datos del piramómetro. |
| `estado` | Muestra el modo de simulación actual. |
| `salir` | Detiene el simulador. |

## Recepción y confirmación de horarios

Además de transmitir información, el simulador permite comprobar el funcionamiento del mecanismo de configuración remota del sistema.

Escucha los tópicos generales e individuales de configuración, recibe los parámetros JSON y reconstruye los horarios a partir de los campos `horas` y, opcionalmente, `minutos`.

Ejemplo de configuración recibida:

```json
{
    "horas": [6, 12, 18],
    "minutos": [0, 30, 0]
}
```

El simulador interpreta esta configuración como:

- 06:00
- 12:30
- 18:00

Después de procesarla correctamente, actualiza el estado interno del nodo simulado y genera una confirmación en:

`esp32/<nodo>/configok`

La respuesta incluye el identificador del nodo, el estado de confirmación y los horarios recibidos.

## Aplicación en las pruebas del sistema AgroTech

Este simulador fue desarrollado como una herramienta auxiliar para realizar pruebas puntuales de integración del sistema AgroTech.

Su utilización permite comprobar:

- La recepción de mediciones procedentes de los nodos mediante MQTT.
- El procesamiento y almacenamiento de los datos recibidos por el servidor.
- La recepción y asociación de fotografías con sus respectivos nodos.
- El funcionamiento de la configuración remota de horarios.
- La respuesta del modelo de Machine Learning ante condiciones normales y anomalías simuladas.
- La generación de alertas y la identificación de patrones anómalos a partir de las mediciones transmitidas.

Al permitir cambiar las condiciones de simulación durante la ejecución, facilita la evaluación de diferentes escenarios sin necesidad de modificar el firmware ni realizar intervenciones físicas sobre los dispositivos.

## Consideraciones

- Los datos generados son sintéticos y no proceden de sensores físicos.
- Los rangos utilizados son configurables y están definidos dentro del código.
- Las fotografías se seleccionan aleatoriamente de la carpeta indicada.
- El simulador requiere que el broker MQTT y los servicios HTTP correspondientes estén disponibles.
- La confirmación de horarios es una emulación del comportamiento esperado de los nodos ESP32, no una confirmación procedente de un dispositivo físico.
- Las pruebas del modelo de Machine Learning dependen de la configuración y los mecanismos de análisis implementados en el servidor AgroTech.

---

**Proyecto:** AgroTech – Sistema de medición y monitorización de plantas de tomate.

**Herramienta:** Simulador de nodos ESP32 para pruebas de comunicación, integración y evaluación de Machine Learning.
