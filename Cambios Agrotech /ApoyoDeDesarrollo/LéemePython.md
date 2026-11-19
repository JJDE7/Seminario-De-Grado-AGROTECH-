# AgroTech – Notebooks de desarrollo y experimentación de Machine Learning

## Descripción

Esta carpeta contiene diferentes notebooks de Python (`.ipynb`) desarrollados como herramientas de apoyo durante la implementación del componente de Machine Learning del sistema AgroTech.

Su propósito es documentar y facilitar los procesos de experimentación, preparación de datos, entrenamiento de modelos y evaluación de la viabilidad de diferentes enfoques de aprendizaje automático para el análisis de las condiciones de las plantas de tomate.

Los notebooks fueron utilizados durante distintas etapas del desarrollo, permitiendo comparar alternativas de entrenamiento, explorar diferentes conjuntos de datos y evaluar el comportamiento de los algoritmos antes de incorporarlos al sistema principal.

## Contenido de la carpeta

Los archivos se pueden agrupar según su propósito:

### Entrenamiento y desarrollo de modelos

- **Entrenamiento Datos fijos Isolation y Random.ipynb:** experimentación y entrenamiento utilizando conjuntos de datos fijos.
- **Entrenamiento Datos móviles Isolation y Random.ipynb:** experimentación y entrenamiento utilizando conjuntos de datos móviles.
- **Entrenamiento con datos sintéticos.ipynb:** experimentación con información sintética para evaluar alternativas de entrenamiento.

Estos tres notebooks representan los procesos más relevantes de esta carpeta, ya que dieron paso al desarrollo de los modelos de Random Forest que posteriormente se incorporaron al repositorio, dentro de la carpeta `Modelos`.

### Evaluación y experimentación complementaria

- **Prueba puntual Isolation Forest.ipynb:** pruebas específicas relacionadas con el algoritmo Isolation Forest.
- **Script de evaluación de viabilidad de variables para la tabla de datos móviles.ipynb:** análisis orientado a determinar la viabilidad de utilizar diferentes variables en los procesos de entrenamiento con datos móviles.
- **Conversor de excel a base de datos.ipynb:** herramienta auxiliar para la preparación y conversión de información.

## Relación con los modelos del sistema

Esta carpeta contiene principalmente los experimentos y procesos de desarrollo previos a la integración de los modelos definitivos.

Los resultados obtenidos durante estas pruebas permitieron seleccionar y desarrollar los modelos de Random Forest utilizados posteriormente en AgroTech.

Los modelos resultantes se encuentran organizados en la carpeta:

`Modelos/`

Por tanto, es importante distinguir entre los notebooks de esta carpeta, utilizados como herramientas de experimentación y desarrollo, y los modelos almacenados en `modelos/`, destinados a su utilización dentro del sistema.

## Consideraciones

- Los notebooks documentan diferentes etapas del desarrollo del componente de Machine Learning.
- No todos los archivos representan modelos utilizados directamente en el sistema final.
- Algunos notebooks corresponden a pruebas exploratorias o evaluaciones de viabilidad.
- Las explicaciones específicas, procedimientos y resultados de cada experimento se encuentran documentados dentro de sus respectivos notebooks.

---

**Proyecto:** AgroTech – Sistema de medición y monitorización de plantas de tomate.

**Propósito:** Documentación de experimentos, entrenamiento y evaluación de modelos de Machine Learning.
