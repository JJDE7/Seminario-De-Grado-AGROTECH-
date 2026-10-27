# AgroTech - Dashboard, API REST y Machine Learning

Plataforma de software para el monitoreo agrícola, desarrollada para gestionar la recepción, almacenamiento, análisis y visualización de datos provenientes de nodos de sensores en campo. Este repositorio consolida el trabajo asignado al **Equipo B**, enfocándose en el backend, la interfaz de usuario y la detección de anomalías mediante Inteligencia Artificial.

El sistema abandona el uso de WebSockets en favor de un flujo más robusto y estandarizado utilizando MQTT para telemetría en tiempo real y una API REST (FastAPI) para la integración y consulta de datos.

## Rol y Responsabilidades del Equipo B

El desarrollo de este módulo cumple con el cronograma de tareas asignadas al equipo:

*   **Revisión inicial:** Revisión de software y hardware del sistema desarrollado previamente.
*   **Almacenamiento y Comunicación:** Almacenamiento de datos mediante SQLite y evolución de la comunicación (reemplazando WebSockets por MQTT/HTTP).
*   **Gestión de API:** Lectura y escritura API desde la Raspberry Pi.
*   **Gestión de Alertas y Configuración:** Generación de alertas automatizadas y configuración del sistema por interfaz (FastAPI + Flet).
*   **Inteligencia Artificial:** Análisis de datos a través de un algoritmo de Machine Learning.
*   **Cierre:** Integración y pruebas finales, seguido de la presentación y documentación del proyecto.

## Arquitectura del Sistema

El flujo de información conecta los dispositivos de campo con el usuario final a través de múltiples capas:

*   **Sistema físico (Raspberry Pi + Nodos):** Los nodos basados en ESP32 recolectan información de sensores (ambiente, suelo, piramómetro) y capturan imágenes. Transmiten los datos de telemetría a la Raspberry Pi utilizando MQTT y envían las fotos mediante peticiones HTTP (POST).
*   **Servidor Web (Dashboard + API):** La Raspberry Pi aloja una API REST desarrollada con FastAPI y una base de datos local (SQLite) para almacenar mediciones, configuraciones y el historial de fotos. Además, ejecuta un hilo de sincronización que replica los datos hacia un servidor en la nube.
*   **Dashboard Web (Usuario):** Interfaz gráfica desarrollada con Flet que consume los datos locales. Permite la visualización de sensores en tiempo real, consulta del historial, configuración remota de los horarios de hibernación/medición de los nodos y visualización de alertas.

## Características Principales

*   **Dashboard Interactivo:** Visualización en tiempo real de temperatura, humedad, NPK, conductividad (EC), pH, radiación solar y nivel de batería.
*   **Detección de Anomalías (Machine Learning):** Integra un modelo *Isolation Forest* entrenado localmente. Evalúa 9 dimensiones de datos de manera simultánea para detectar patrones inusuales y clasificar las alertas según su severidad (Advertencia o Crítica).
*   **Sincronización Cloud:** Mecanismo automático para respaldar mediciones, alertas y fotografías en un servidor externo (`pythonanywhere`), garantizando la disponibilidad de la información fuera de la red local.
*   **Simulador de Nodos Integrado:** Incluye un script (`simulador_esp32_con_recepcion_v3.py`) con una consola interactiva (CLI) para emular el envío de datos sanos o anómalos, facilitando las pruebas del algoritmo de Machine Learning sin depender del hardware físico.

## Tecnologías Usadas

*   **Lenguaje:** Python 3.10+
*   **Interfaz Gráfica:** Flet
*   **Backend & API:** FastAPI, Uvicorn
*   **Comunicación:** Paho MQTT (Telemetría), Requests (Sincronización REST)
*   **Machine Learning:** Scikit-learn (Isolation Forest), Numpy, Joblib
*   **Base de Datos:** SQLite3
