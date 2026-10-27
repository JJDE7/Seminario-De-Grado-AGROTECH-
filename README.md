# AgroTech - Dashboard, API REST y Machine Learning

Esta plataforma de software está diseñada para centralizar el monitoreo agrícola: recibe, procesa y analiza los datos de los sensores en campo para presentarlos de forma clara e interactiva. Este repositorio consolida el trabajo del Equipo B, enfocado en la construcción del backend, la interfaz de usuario y el sistema de detección de anomalías basado en Machine Learning (Isolation Forest).

Para garantizar una transmisión fluida, utilizamos MQTT para la telemetría de los sensores y HTTP para el envío de imágenes. Además, nos integramos con una API REST en FastAPI (desarrollada por el Equipo A y desplegada en PythonAnywhere). Esta API actúa como el puente principal del sistema, conectando la información procesada con nuestro dashboard para que el usuario pueda visualizar el estado de sus cultivos mediante gráficas y alertas en tiempo real.

Entorno de Simulación y Pruebas
Ante los retos logísticos con el hardware físico, garantizamos la solidez del software construyendo un entorno de pruebas simulado. Mediante scripts de generación de datos, entornos virtuales y ejecución por terminal, logramos emular el comportamiento completo de los nodos en campo. Esto nos permitió someter a estrés la comunicación, validar la API y comprobar todas las funcionalidades de la interfaz de usuario con éxito, dejando la plataforma lista para su futura integración física.

## Rol y Responsabilidades del Equipo B

El desarrollo de este módulo cumple con el cronograma de tareas asignadas al equipo:

*   **Revisión inicial:** Revisión de software y hardware del sistema desarrollado previamente esto con el fin de comprender lo realizado en el semestre pasado para así tener una base del sistema y demas.
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

## Equipo de trabajo: El  Equipo B esta conformado por las siguientes personas.
 * Juan Jose Devia Espinosa
 * Oscar Mauricio Cabezas Riascos
 * Diego Alejandro Vallejo Rojas
