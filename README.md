# NYC Taxi Data Pipeline: End-to-End en Azure Synapse Analytics

Este proyecto implementa una solución empresarial integral de Ingeniería de Datos y Analítica Avanzada utilizando el ecosistema de **Microsoft Azure Synapse Analytics**. El pipeline automatiza la extracción, ingesta, limpieza, enriquecimiento y modelado predictivo del dataset público de los Taxis de Nueva York (*TLC Trip Record Data*), aplicando principios de **Arquitectura Medallón** (Raw, Silver, Gold) y gestionando de forma proactiva la calidad de los datos y el gobierno del Data Lakehouse.

## Características Principales
* **Orquestación Híbrida Optimizada:** Combinación eficiente de actividades de copia nativas de Azure Synapse (alto rendimiento a bajo coste) con procesamiento distribuido de Big Data mediante **Apache Spark (PySpark)**.
* **Gobierno y Data Quality proactivo:** Implementación de un framework de calidad que incluye aislamiento automático en cuarentena de registros inválidos, auditoría condicional de fallos de red y scripts recursivos de mantenimiento de integridad.
* **Machine Learning Integrado (Capa Gold):** Modelado analítico automatizado mediante regresiones lineales distribuidas con transformaciones logarítmicas para predicción (*forecasting*) de demanda y KPIs de negocio a 3 meses vista.

---

## Arquitectura y Flujo de Trabajo

El pipeline está diseñado bajo un enfoque ETL/ELT robusto y automatizado mediante un Pipeline de Synapse (`NycTaxi.json`). El flujo de ejecución unificado funciona de la siguiente manera:

```text
[01_nyctaxiscrap] -> Genera Catálogo de URLs (JSON)
       │
   [Lookup1] ───────> Lee Catálogo Maestro
       │
   [ForEach1] ──────> Paraleliza la Descarga HTTP (Batch de 3)
       │
       ├─── (Éxito) ──> [99_LimpiarCarpetasVacias] -> Remueve directorios huérfanos/0 bytes
       │                     │
       └─── (Fallo) ──> [03_ArchivosRejected] ────> Registra fallos de descarga condicionales (ej. 403)
                             │
                      [02_Cuarentena] ────────────> Capa Raw: Firewall de datos, Schema-on-Read y Cast estricto
                             │
                      [05_Processed] ─────────────> Capa Gold: Agrupación de KPIs y ML (Linear Regression Forecasting)
```

1. **Planificación y Catálogo (`01_nyctaxiscrap.ipynb`):** Aplica reglas de negocio históricas (como la ausencia de taxis verdes antes de agosto de 2013) para generar un índice dinámico y exacto de las URLs a descargar (`lista_urls.json`).
2. **Ingesta Escalable (`Lookup` + `ForEach` + `Copy Data`):** Actividades nativas de Synapse leen el catálogo y ejecutan descargas paralelas (bloques de 3 hilos concurrentes) desde el servidor HTTP origen, inyectando los datos con compresión Snappy en Azure Data Lake Storage Gen2 (ADLS Gen2).
3. **Control de Ingesta Condicional (`03_ArchivosRejected.ipynb`):** Si un hilo de copia falla (ej. error de red o cambios de permisos HTTP 403), el flujo se desvía dinámicamente hacia un módulo auditor que reconcilia la lista maestra y genera un registro de fallos (`fallos_descarga.json`).
4. **Mantenimiento Estructural (`99_LimpiarCarpetasVacias.ipynb`):** Script de saneamiento que purga de forma recursiva carpetas temporales, vacías o archivos corruptos de 0 bytes para evitar la degradación del Data Lake.
5. **Firewall de Calidad de Datos (`02_Cuarentena.ipynb`):** Implementa una estrategia de aislamiento mensual. Aplica un *Schema-on-Read* y estandarización inmediata de tipado (`CAST`), evaluando la presencia de nulos en columnas core. Los datos limpios fluyen hacia la zona de procesamiento y las filas corruptas se aíslan.
6. **Modelado Avanzado y ML (`05_Processed.ipynb`):** Consume los datos validados para entrenar modelos predictivos individuales por cada KPI clave mediante Spark MLlib, proyectando resultados analíticos estables a futuro.

> *Nota: El enriquecimiento analítico corporativo de distritos (`04_AnalisisTaxi.ipynb`) y los análisis profundos de integridad (`06_ComprobarParquets.ipynb`) se desacoplan del pipeline de ingesta principal para optimizar recursos de cómputo.*

---

# Estructura del Repositorio
La organización del código sigue los estándares profesionales de la industria, aislando datos muestra, configuraciones de infraestructura y código productivo numerado secuencialmente:

```text
       NYC_Taxi_Azure/
       ├── assets/                                 
       │   └── pipeline_architecture.png           # Captura visual de la orquestación en Synapse
       ├── data/
       │   └── sample/
       │       └── green_tripdata_2026-01.parquet  # Muestra de datos representativa para pruebas locales
       ├── src/
       │   ├── notebooks/
       │   │   ├── 01_nyctaxiscrap.ipynb           # Extracción: Generación de catálogo maestro JSON
       │   │   ├── 02_Cuarentena.ipynb             # Capa Raw: Validación, tipado estricto y aislamiento
       │   │   ├── 03_ArchivosRejected.ipynb       # Ingesta: Gestión y registro indexado de errores de copia
       │   │   ├── 04_AnalisisTaxi.ipynb           # Capa Silver: Estandarización histórica y cruce geográfico
       │   │   ├── 05_Processed.ipynb              # Capa Gold: Modelos de Machine Learning y Forecasting
       │   │   ├── 06_ComprobarParquets.ipynb      # Auditoría: Análisis recursivo ad-hoc de integridad física
       │   │   └── 99_LimpiarCarpetasVacias.ipynb  # Mantenimiento: Purga dinámica de metadatos y nulos
       │   └── pipelines/
       │       ├── NycTaxi.json                    # Código declarativo del Pipeline de Synapse (actividades/flujos)
       │       └── manifest.json                   # Manifiesto de metadatos y conectores del Workspace
       ├── requirements.txt                        # Dependencias de software del entorno de desarrollo
       └── README.md                               # Documentación principal del proyecto
```

---

## Desglose Técnico de los Notebooks

### Capa Raw e Ingesta
* **`01_nyctaxiscrap.ipynb`**: Inicializa la sesión Spark y automatiza la creación de un mapeador temporal estructurado en JSON con destino a la ruta física del Data Lake. Controla rangos cronológicos completos (2009 - 2026).
* **`02_Cuarentena.ipynb`**: Actúa como la primera línea de defensa de datos. Desactiva configuraciones de lectura vectorizada de Parquet de Spark para asegurar máxima estabilidad y compatibilidad con esquemas legados (2009). Ejecuta funciones de coalescencia de columnas geográficas y fechas antes de segregar el dataframe en `df_validos` y `df_rechazados`.
* **`03_ArchivosRejected.ipynb`**: Automatiza auditorías post-ingesta. Lee fragmentos binarios del catálogo maestro y valida mediante bloques `try-except` la existencia y peso real de cada objeto inyectado en ADLS Gen2, previniendo la propagación de fallos silenciosos.

### Capa Silver: Enriquecimiento Corporativo
* **`04_AnalisisTaxi.ipynb`**: Responsable de consolidar el modelo semántico intermedio. Resuelve cambios históricos críticos de nomenclatura en el dataset (ej. unificación de coordenadas GPS antiguas con los IDs de zonas modernos de la TLC). Realiza la descarga dinámica del diccionario geográfico oficial de Nueva York (`taxi_zone_lookup.csv`) y ejecuta un **doble Left Join distribuido** para enriquecer cada viaje con los nombres reales de los distritos (*Boroughs*) y barrios (*Zones*) de Origen y Destino.

### Capa Gold: Analítica Avanzada e Inteligencia Predictiva
* **`05_Processed.ipynb`**: Núcleo predictivo del proyecto. Implementa técnicas avanzadas de Feature Engineering y Machine Learning distribuido:
    * **Limpieza de Outliers de Negocio:** Filtra viajes con duraciones ilógicas (menores a 30 segundos o mayores a 5 horas), distancias negativas e importes inferiores a la tarifa mínima base de NYC ($2.50).
    * **Ventanas de Tiempo Móviles:** Agrupa y ordena cronológicamente la información generando índices temporales para aislar ventanas de entrenamiento óptimas.
    * **Spark MLlib Pipeline:** Implementa un bucle iterativo automatizado para entrenar modelos de **Regresión Lineal** individuales por cada KPI de negocio (`Total_Viajes`, `Ingresos_Totales`, `Distancia_Media`). Aplica transformaciones logarítmicas (`log(y + 1)`) sobre los targets para estabilizar la varianza del modelo, realizando predicciones futuras precisas mediante la reversión exponencial (`exp(pred) - 1`) y calculando los porcentajes reales de error de desviación absoluta.

### Herramientas de Operación y Soporte
* **`06_ComprobarParquets.ipynb`**: Script recursivo de bajo nivel que escanea el sistema de archivos del Data Lake buscando e imprimiendo alertas visuales sobre corrupciones físicas o bytes nulos.
* **`99_LimpiarCarpetasVacias.ipynb`**: Utilidad que consume las APIs nativas del entorno de Synapse (`notebookutils.mssparkutils.fs`) para borrar directorios físicos huérfanos generados tras limpiezas manuales o particionados vacíos.

---

## Tecnologías Utilizadas

* **Azure Synapse Analytics:** Plataforma unificada para la orquestación mediante Pipelines empresariales y administración de metadatos.
* **Synapse Spark Pools (v3.5):** Clústeres autoevaluados de Apache Spark distribuidos orientados al procesamiento de Big Data (PySpark) y MLlib.
* **Azure Data Lake Storage Gen2 (ADLS Gen2):** Sistema de archivos jerárquico securizado mediante identidades administradas (RBAC de Azure).
* **Formato Parquet (Snappy):** Almacenamiento columnar altamente eficiente optimizado para consultas analíticas de gran volumen.

---

## Despliegue y Ejecución

### Requisitos Previos en Azure
1. Tener un área de trabajo de **Azure Synapse Analytics** activa con un clúster Apache Spark habilitado (Spark versión 3.5 recomendado).
2. Contar con una cuenta de almacenamiento **ADLS Gen2** vinculada con el contenedor principal configurado (en este pipeline se hace uso del contenedor `nyctaxi-raw` y el endpoint `stnycanalysis.dfs.core.windows.net`).
3. Crear un **Linked Service de tipo HTTP** apuntando al repositorio fuente público del TLC, mapeando el parámetro dinámico `NombreArchivo`.

### Pasos para la Implementación
1. **Configurar el Entorno:** Si deseas replicar o evaluar metodologías analíticas de manera local o en un contenedor externo, instala las dependencias definidas utilizando pip:
   ```bash
     pip install -r requirements.txt
   ```

2. **Importar Elementos:** Carga los archivos dentro de `src/notebooks/` en la sección de *Develop* de tu Synapse Workspace y despliega el archivo del pipeline `NycTaxi.json` en la pestaña de *Integrate*.
3. **Publicar y Ejecutar:** Realiza un *Publish All* de los cambios en tu espacio de trabajo para guardar las actividades y desencadena una ejecución manual (*Trigger Now*) de tu pipeline `NycTaxi` para iniciar la ingesta automatizada.

---

## Sobre los Datos de Muestra

Para garantizar el cumplimiento de restricciones de tamaño de plataformas de control de versiones sin comprometer la reproducibilidad del entorno, se ha incluido una pequeña muestra física de datos correspondiente a la flota de taxis verdes (`green_tripdata_2026-01.parquet`) en la carpeta `/data/sample/`. Los procesamientos masivos multianuales de la flota de taxis amarillos se ejecutan de manera nativa en la nube escalando clústeres elásticos y se omiten del repositorio de código Git.
