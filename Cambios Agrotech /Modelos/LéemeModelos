# Modelos de Clasificación y Detección de Anomalías - AgroTech

Este repositorio contiene tres modelos basados en el algoritmo **Random Forest**, desarrollados para el monitoreo y detección de anomalías en variables agronómicas y condiciones de suelo/cultivo.

Los tres modelos comparten exactamente la misma arquitectura, hiperparámetros y pipeline de entrenamiento, diferenciándose únicamente en la naturaleza y distribución de los conjuntos de datos (*datasets*) empleados durante su fase de ajuste.

---

## Arquitectura y Comparativa de Modelos

A continuación, se detallan las características técnicas, el alcance y las limitaciones conocidas de cada artefacto entrenado:

| Modelo                                                                                    | Conjunto de Datos (Dataset)                                                                 | Comportamiento y Rendimiento                                                | Limitaciones Conocidas                                                                                                               |
| :---                                                                                      | :---                                                                                        | :---                                                                        | :---                                                                                                                                 |
| **1. Específico (Monocultivo)**: "Random Forest 13D (Datos de estación fija).joblib"      | Datos reales recolectados de una única especie vegetal.                                     | Alta precisión dentro del dominio específico de la planta objetivo.         | Presenta una tasa moderada de **falsos negativos**, omitiendo ocasionalmente anomalías sutiles fuera de los patrones más frecuentes. |
| **2. Generalista (Multicultivo)**: "Random Forest 13D (Datos de estación móvil).joblib"   | Dataset ampliado con registros de múltiples especies vegetales.                             | Buena capacidad de generalización entre distintos tipos de cultivo.         | Baja sensibilidad ante combinaciones ambientales atípicas o de borde (ej. **suelos simultáneamente húmedos y calientes**).           |
| **3. Sintético (Basado en Reglas)**: "Random Forest 13D (Datos sintéticos).joblib"        | Datos generados aleatoriamente bajo reglas deterministas que emulan el comportamiento real. | Rendimiento óptimo y altamente consistente frente a los umbrales definidos. | Comportamiento **estrictamente determinista** y baja tolerancia a pequeñas variaciones o ruido natural de los sensores.              |

---

## Despliegue e Integración

El sistema está diseñado para cargar un único modelo serializado en formato `joblib` de manera intercambiable sin necesidad de modificar el código fuente de inferencia.

### Instrucciones de activación

1. Selecciona el archivo del modelo que mejor se adapte a tu entorno de producción o pruebas.
2. Copia el archivo seleccionado dentro del directorio de datos del proyecto:

   ...\agrotech\data\

3. Renombrar el archivo seleccionado a: "rf_model.joblib"

Nota: asegurarse de que exista únicamente un archivo activo con el nombre rf_model.joblib en la ruta especificada antes de inicializar el servicio de predicción.

## Recomendaciones de Selección

* Entornos controlados o con reglas estrictas de umbral: Utilizar el Modelo 3 (Sintético).

* Monitoreo de una sola especie donde se priorice evitar falsas alarmas: Utilizar el Modelo 1 (Específico).

* Despliegues heterogéneos con múltiples cultivos en condiciones climáticas estándar: Utilizar el Modelo 2 (Generalista).

##  Recomendación del equipo de trabajo

El sistema que mejor se ajustó al funiconamiento esperado fue "Random Forest 13D (Datos de estación móvil).joblib", es un buen punto de inicio en caso de querer realizar pruebas generales.
