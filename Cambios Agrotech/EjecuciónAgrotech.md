# AgroTech Dashboard + FastAPI

Guía de instalación y ejecución del servidor AgroTech en una **Raspberry Pi** o en un equipo externo con **Windows**.

Esta guía corresponde al archivo Python entregado para el proyecto. El programa integra una interfaz de escritorio desarrollada con Flet, recepción de datos mediante MQTT, recepción de fotografías por HTTP, una API REST con FastAPI, almacenamiento local en SQLite/JSON y clasificación de anomalías mediante un modelo Random Forest.

---

## 1. ¿Qué necesita el sistema?

### Requisitos generales

- Python **3.12 o superior** recomendado para instalar con `--upgrade` las versiones más recientes de las dependencias. Flet requiere Python 3.10 o superior; la compatibilidad de scikit-learn depende de su versión y las versiones más recientes pueden exigir Python 3.12 o posterior.
- Conexión a Internet para instalar las dependencias y para utilizar la sincronización con el servidor en la nube.
- Una sesión de escritorio con interfaz gráfica para abrir la ventana de Flet. El código inicia Flet en modo de aplicación de escritorio (`ft.AppView.FLET_APP`), por lo que no está preparado para ejecutarse como una aplicación completamente headless sin modificarlo.
- Un broker MQTT ejecutándose en el mismo equipo, o una dirección MQTT distinta configurada en el código.
- Para recibir comunicaciones de los ESP32 desde la red local, el equipo debe ser accesible mediante su dirección IP y los puertos correspondientes deben estar permitidos en el firewall.

En Raspberry Pi se recomienda utilizar **Raspberry Pi OS de 64 bits**. La disponibilidad de paquetes binarios de NumPy, SciPy y scikit-learn depende de la arquitectura y de la versión del sistema; en algunas configuraciones ARM, `pip` puede intentar compilar dependencias desde el código fuente.

### Servicios y puertos utilizados

| Servicio | Dirección/puerto predeterminado | Función |
|---|---|---|
| Broker MQTT | `localhost:1883` | Recibe los mensajes de sensores y permite publicar configuraciones para los ESP32. |
| Receptor de fotografías HTTP | `0.0.0.0:8080/upload` | Recibe las imágenes enviadas por las cámaras ESP32-CAM. |
| API REST FastAPI | `0.0.0.0:8000` | Expone los datos almacenados, fotografías, alertas y configuraciones. |
| Interfaz Flet | Ventana local | Permite visualizar los nodos y administrar el sistema. |

La API puede consultarse desde el propio equipo en `http://127.0.0.1:8000/docs`. Desde otro dispositivo de la misma red, se utiliza la IP del equipo que ejecuta AgroTech, por ejemplo `http://192.168.1.20:8000/docs`.

> **Importante sobre MQTT:** el código utiliza inicialmente `MQTT_BROKER = "localhost"`. Esto significa que AgroTech busca el broker en el mismo equipo donde se ejecuta el programa. Si el broker está en otro dispositivo, hay que cambiar esa constante por la IP o el nombre de host del broker. Los ESP32 tampoco deben usar `localhost`: deben conectarse a la dirección IP del equipo donde está Mosquitto.

---

## 2. Dependencias de Python

Instala los siguientes paquetes. Esta es la lista utilizada por el proyecto:

```bash
python -m pip install --upgrade paho-mqtt flet requests numpy pandas joblib scikit-learn fastapi pydantic uvicorn
```

| Paquete | Uso en AgroTech |
|---|---|
| `paho-mqtt` | Comunicación del servidor con el broker MQTT y los ESP32. |
| `flet` | Interfaz gráfica de escritorio. |
| `requests` | Peticiones HTTP y sincronización con el servidor en la nube. |
| `numpy` | Operaciones numéricas utilizadas por el entorno de análisis. |
| `pandas` | Preparación de los datos que recibe el modelo de Machine Learning. |
| `joblib` | Carga del modelo Random Forest guardado en disco. |
| `scikit-learn` | Clasificación de anomalías mediante Machine Learning. |
| `fastapi` | Construcción de la API REST local. |
| `pydantic` | Validación de los datos recibidos por la API. |
| `uvicorn` | Servidor que ejecuta la API FastAPI. |

`scikit-learn` instala también sus dependencias necesarias, entre ellas SciPy y `threadpoolctl`; no es necesario agregarlas manualmente a la orden anterior.

### Módulos que no requieren instalación con pip

El programa también utiliza módulos incluidos en Python, como `sqlite3`, `json`, `os`, `threading`, `time`, `math`, `base64`, `datetime`, `typing`, `http.server` y `urllib`. No deben instalarse por separado.

### Dependencia externa: Mosquitto

`paho-mqtt` es la biblioteca cliente de Python; **no es el broker MQTT**. Para que AgroTech pueda conectarse a `localhost:1883`, debe estar instalado y ejecutándose un broker, por ejemplo Eclipse Mosquitto.

---

## 3. Instalación y ejecución en Raspberry Pi

Los siguientes pasos están pensados para Raspberry Pi OS de 64 bits con escritorio. Aunque el programa se ejecuta directamente en la Raspberry Pi, las bibliotecas de Python se instalan en un entorno virtual del proyecto.

### Paso 1. Actualizar el sistema e instalar los componentes del sistema operativo

Abre una terminal en la Raspberry Pi y ejecuta:

```bash
sudo apt update
sudo apt upgrade -y
sudo apt install -y python3 python3-full python3-pip python3-venv mosquitto mosquitto-clients
```

Si utilizas una instalación mínima sin entorno gráfico, instala y configura un escritorio compatible antes de ejecutar la interfaz Flet. Una instalación de escritorio estándar normalmente ya incluye las bibliotecas gráficas requeridas; en un sistema mínimo puede ser necesario instalar GTK 3:

```bash
sudo apt install -y libgtk-3-0
```

### Paso 2. Iniciar el broker MQTT

Activa Mosquitto para que se inicie automáticamente y arráncalo ahora:

```bash
sudo systemctl enable --now mosquitto
```

Comprueba su estado:

```bash
sudo systemctl status mosquitto
```

El servicio debe aparecer como `active (running)`.

### Paso 3. Copiar el código a la Raspberry Pi

Crea una carpeta para el proyecto:

```bash
mkdir -p ~/agrotech_app
cd ~/agrotech_app
```

Copia dentro de esa carpeta el archivo Python del servidor. Por ejemplo, si conservas el nombre original del archivo entregado, será:

```text
agrotech_appv11_fastapi_V1_cloud_V5.3_ML (13D RForest).py
```

También puedes renombrarlo a `agrotech.py` para facilitar los comandos siguientes.

### Paso 4. Crear el entorno de Python e instalar las dependencias

En Raspberry Pi OS Bookworm y versiones posteriores, el Python del sistema está marcado como un entorno administrado por el sistema operativo. Por eso, `pip` no permite instalar paquetes directamente en él de forma predeterminada. No utilices `sudo pip install` ni `--break-system-packages` para evitar esa protección.

Crea un entorno virtual dentro de la carpeta del proyecto:

```bash
python3 -m venv .venv
```

Actívalo:

```bash
source .venv/bin/activate
```

Actualiza `pip` e instala las dependencias de AgroTech:

```bash
python -m pip install --upgrade pip
python -m pip install --upgrade paho-mqtt flet requests numpy pandas joblib scikit-learn fastapi pydantic uvicorn
```

Cuando el entorno esté activo, la terminal normalmente mostrará `(.venv)` al inicio de la línea. Los comandos `python` y `pip` utilizarán las versiones instaladas en ese entorno.

### Paso 5. Ejecutar AgroTech

Desde la carpeta donde se encuentra el archivo Python y con `.venv` todavía activo, ejecuta una de estas opciones.

Si renombraste el archivo:

```bash
python agrotech.py
```

Si conservaste el nombre original:

```bash
python "agrotech_appv11_fastapi_V1_cloud_V5.3_ML (13D RForest).py"
```

Se abrirá la interfaz de Flet. Durante el arranque, el programa inicializará SQLite y pondrá en marcha sus procesos de MQTT, sincronización, FastAPI y recepción de fotografías.

### Paso 6. Volver a ejecutar el programa en el futuro

Cada vez que abras una terminal nueva:

```bash
cd ~/agrotech_app
source .venv/bin/activate
python agrotech.py
```

Cuando termines de utilizarlo, puedes cerrar la aplicación y salir del entorno virtual con:

```bash
deactivate
```

---

## 4. Instalación y ejecución en Windows mediante `venv`

Este procedimiento permite ejecutar el mismo servidor en un PC Windows para desarrollo, pruebas o simulación. El equipo debe contar con Python de 64 bits y acceso a la red donde se encuentren los ESP32, si se van a conectar dispositivos físicos.

### Paso 1. Instalar Python

Instala Python **3.12 o superior de 64 bits** desde la página oficial:

<https://www.python.org/downloads/windows/>

Durante la instalación, activa la opción **Add Python to PATH** si aparece.

Comprueba que Python está disponible. En PowerShell o CMD ejecuta:

```powershell
py --version
```

### Paso 2. Instalar Mosquitto

Descarga el instalador de Windows desde la página oficial de Eclipse Mosquitto:

<https://mosquitto.org/download/>

Instálalo y asegúrate de que el broker esté en ejecución. Para una prueba manual, abre una terminal donde esté disponible el ejecutable `mosquitto` y ejecuta:

```powershell
mosquitto -v
```

Deja esa terminal abierta mientras realizas la prueba. Si Mosquitto se instaló como servicio de Windows, también puedes comprobar su estado desde la aplicación **Servicios** (`services.msc`).

### Paso 3. Crear la carpeta del proyecto

Abre PowerShell y crea o selecciona la carpeta donde guardarás el código:

```powershell
mkdir C:\AgroTech
cd C:\AgroTech
```

Copia el archivo `.py` del servidor en esa carpeta. Opcionalmente, renómbralo a `agrotech.py`.

### Paso 4. Crear el entorno virtual

Desde PowerShell, dentro de la carpeta del proyecto, ejecuta:

```powershell
py -3.12 -m venv .venv
```

Si tienes otra versión compatible de Python instalada, puedes utilizar `py -m venv .venv`. Comprueba que sea compatible con las versiones de scikit-learn y del resto de paquetes que se vayan a instalar.

Activa el entorno virtual:

```powershell
.\.venv\Scripts\Activate.ps1
```

Si PowerShell bloquea la ejecución del archivo de activación, puedes abrir **CMD** en la carpeta del proyecto y ejecutar:

```cmd
.venv\Scripts\activate.bat
```

### Paso 5. Instalar las dependencias

Con el entorno virtual activado, ejecuta:

```powershell
python -m pip install --upgrade pip
python -m pip install --upgrade paho-mqtt flet requests numpy pandas joblib scikit-learn fastapi pydantic uvicorn
```

La instalación puede tardar más en equipos con conexiones lentas, debido al tamaño de algunas bibliotecas científicas.

### Paso 6. Ejecutar AgroTech

Si renombraste el archivo:

```powershell
python agrotech.py
```

Si conservaste el nombre original:

```powershell
python ".\agrotech_appv11_fastapi_V1_cloud_V5.3_ML (13D RForest).py"
```

La interfaz Flet se abrirá en una ventana de escritorio. FastAPI y el receptor HTTP quedarán ejecutándose en segundo plano mientras la aplicación permanezca abierta.

### Paso 7. Volver a ejecutar el programa

En una nueva terminal, activa de nuevo el entorno virtual antes de ejecutar el programa:

```powershell
cd C:\AgroTech
.\.venv\Scripts\Activate.ps1
python agrotech.py
```

Para salir del entorno virtual:

```powershell
deactivate
```

---

## 5. Verificar la instalación

Con el entorno virtual activado, ejecuta este comando para comprobar que las bibliotecas principales pueden importarse:

```bash
python -c "import flet, paho.mqtt.client, requests, numpy, pandas, joblib, sklearn, fastapi, pydantic, uvicorn; print('Dependencias de AgroTech instaladas correctamente')"
```

Si no aparece un error de importación, las dependencias de Python están disponibles.

Cuando AgroTech esté iniciado, también puedes comprobar la API abriendo en el navegador del mismo equipo:

```text
http://127.0.0.1:8000/docs
```

Desde otro equipo de la red local, sustituye `127.0.0.1` por la IP del equipo que ejecuta AgroTech.

---

## 6. Archivos y almacenamiento

El programa crea y utiliza automáticamente la siguiente carpeta dentro del directorio personal del usuario que ejecuta Python:

```text
~/agrotech/data/
```

En una Raspberry Pi suele corresponder a `/home/<usuario>/agrotech/data/`. En Windows corresponde normalmente a `C:\Users\<usuario>\agrotech\data\`.

En esta carpeta se almacenan, entre otros:

- `agrotech.db`: base de datos SQLite con mediciones, fotografías, alertas y configuraciones.
- Archivos JSON de historial y configuración de los nodos.
- Fotografías recibidas desde los ESP32-CAM.
- `storage_config.json`: configuración del límite de almacenamiento.
- `rf_model.joblib`: modelo de Machine Learning, cuando se utiliza la clasificación de anomalías.

### Modelo Random Forest

Para habilitar la clasificación de anomalías, coloca el archivo del modelo con el nombre exacto `rf_model.joblib` en:

```text
~/agrotech/data/rf_model.joblib
```

Si el archivo no existe, el servidor puede iniciar y realizar las demás funciones, pero la evaluación de anomalías mediante Random Forest se omite.

Es recomendable utilizar una versión de scikit-learn compatible con aquella con la que se entrenó y guardó el modelo. Los modelos serializados con `joblib` no garantizan compatibilidad entre versiones distintas de scikit-learn; si aparecen errores al cargar o ejecutar el modelo, comprueba la versión de entrenamiento y considera volver a exportarlo en el entorno de destino.

---

## 7. Conexión con los ESP32 y acceso desde la red

Para que los ESP32 puedan enviar información al equipo que ejecuta AgroTech:

1. Conecta el equipo y los ESP32 a la misma red local, o asegúrate de que exista una ruta de red entre ellos.
2. Identifica la dirección IP local del equipo servidor.
   - En Raspberry Pi puedes ejecutar `hostname -I`.
   - En Windows puedes ejecutar `ipconfig`.
3. Configura los ESP32 para conectarse al broker MQTT mediante la IP del equipo que aloja Mosquitto y para enviar las fotografías al puerto HTTP `8080` del equipo servidor, en la ruta `/upload`.
4. Permite en el firewall únicamente el tráfico necesario desde la red de confianza hacia los puertos `1883` (MQTT), `8080` (fotografías HTTP) y, si otros clientes utilizarán la API, `8000` (FastAPI).

No configures los ESP32 para conectarse a `localhost`: esa dirección siempre representa el propio dispositivo desde el que se realiza la conexión.

En Mosquitto, la configuración predeterminada puede limitar las conexiones a la propia máquina. Para aceptar ESP32 desde la red local, el broker debe configurarse para escuchar en la interfaz de red correspondiente y utilizar autenticación y permisos adecuados. No expongas un broker MQTT anónimo ni la API directamente a Internet.

---

## 8. Solución de problemas comunes

| Problema | Posible causa y solución |
|---|---|
| `ModuleNotFoundError` | El entorno virtual no está activo o las dependencias no se instalaron en él. Actívalo y repite la instalación. |
| `externally-managed-environment` en Raspberry Pi | Se intentó instalar con `pip` en el Python administrado por Raspberry Pi OS. Crea y activa `.venv` siguiendo la sección de Raspberry Pi. |
| Error de conexión MQTT o `Connection refused` | Mosquitto no está iniciado, no escucha en el puerto `1883` o `MQTT_BROKER` apunta a una dirección incorrecta. |
| Los ESP32 no logran conectarse al broker | Comprueba la IP del servidor, el listener de Mosquitto, la autenticación, la red y las reglas del firewall. |
| No se reciben fotografías | Verifica la IP del servidor, el puerto `8080`, la ruta `/upload` y que el firewall permita la conexión. |
| No abre `http://127.0.0.1:8000/docs` | Comprueba que AgroTech siga abierto y que el puerto `8000` no esté ocupado por otro proceso. |
| Flet no puede abrir una ventana en Raspberry Pi | El programa necesita una sesión gráfica de escritorio. Ejecutarlo desde una consola SSH sin servidor gráfico puede producir errores de pantalla. |
| No aparecen alertas de Machine Learning | Verifica que `rf_model.joblib` esté en `~/agrotech/data/` y que sea compatible con la versión instalada de scikit-learn. |
| Error de puerto ocupado | Comprueba si otro proceso utiliza `1883`, `8080` o `8000`. Si cambias un puerto en el código, actualiza también los dispositivos o clientes que lo utilizan. |

---

## 9. Consideraciones de seguridad y configuración

- El archivo Python contiene la URL del servidor en la nube y una clave API configurada directamente en el código. **No publiques esa clave ni subas el archivo con credenciales activas a repositorios públicos.** Antes de compartir el proyecto, conviene rotar la clave expuesta y trasladar los secretos a variables de entorno o a un archivo de configuración excluido del control de versiones.
- La API local escucha en `0.0.0.0:8000`, por lo que puede aceptar conexiones desde otras interfaces de red. Limita el acceso mediante firewall y no la publiques directamente en Internet sin añadir los mecanismos de autenticación y protección correspondientes.
- La sincronización con la nube requiere conexión a Internet. La base de datos y los archivos locales se almacenan en el equipo que ejecuta el programa.
- Mantén una copia de seguridad periódica de `~/agrotech/data/`, especialmente de `agrotech.db`, los historiales y las fotografías.

---

## 10. Referencias oficiales

- Raspberry Pi Documentation — uso de Python y entornos virtuales: <https://www.raspberrypi.com/documentation/computers/os.html#use-python-on-a-raspberry-pi>
- Flet — instalación y requisitos de Python: <https://flet.dev/docs/getting-started/installation/>
- scikit-learn — instalación y dependencias: <https://scikit-learn.org/stable/install.html>
- Eclipse Mosquitto — descargas e instalación: <https://mosquitto.org/download/>

