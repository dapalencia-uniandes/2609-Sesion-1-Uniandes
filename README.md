<div align="center">

<img src="assets/logo.png" alt="Talento Para TI" width="320"><br/><br/>

# Databricks para Ingeniería y Analítica de Datos — Uniandes

**E-commerce: abandono de carrito y detección de fraude**

</div>

---

Demo práctico del curso, construido como secuencia continua sobre un único caso de negocio.

Cada sesión lee y transforma las tablas que dejó la sesión anterior — no son demos aislados. El dataset sintético (clientes, productos, sesiones web, eventos de carrito, transacciones) se genera una sola vez en `00_Setup` y acompaña las 16 sesiones hasta el modelo de Machine Learning final.

**Dos reglas de diseño se aplican en los 19 archivos:**
1. **Guía paso a paso real:** toda celda de código está precedida por una celda markdown "ANTES DE EJECUTAR ESTA CELDA" con tres campos — **QUÉ HACE**, **PARA QUÉ** y **CÓMO LO HACE** — nunca una celda de código queda sin explicar.
2. **100% Databricks Free Edition:** solo Compute Serverless, Unity Catalog por defecto, 1 SQL Warehouse 2X-Small y Jobs con hasta 5 tasks concurrentes por cuenta — los límites reales de la capa gratuita. Ninguna celda requiere una feature de pago. La sintaxis está alineada a la documentación oficial vigente (por ejemplo, Lakeflow Declarative Pipelines usa `from pyspark import pipelines as dp`, no el módulo `dlt` heredado).

## Antes de la Sesión 1

Todos los **widgets y parámetros de configuración del curso viven en `00_Setup`** — ningún notebook de sesión declara sus propios widgets; todos heredan las variables con `%run ../00_Setup/00_Configuracion`.

1. Abrir `00_Setup/00_Configuracion.ipynb`, revisar/ajustar los widgets (catalog, entorno, volúmenes de datos sintéticos) y ejecutarlo una vez.
2. Ejecutar `00_Setup/01_Generacion_Datos_Sinteticos.ipynb` — crea el catalog, los 3 schemas Medallion (`bronze`/`silver`/`gold`), un Volume para Auto Loader, y las 5 tablas Bronze.
3. A partir de ahí, seguir el orden de carpetas/notebooks de la tabla siguiente. Cada notebook indica su prerequisito en el encabezado.

## Mapa de sesiones

| Semana | Sesión | Notebook | Tema |
|---|---|---|---|
| — | Setup | `00_Setup/00_Configuracion.ipynb` + `01_Generacion_Datos_Sinteticos.ipynb` | Widgets, catalog, schemas Medallion, datos sintéticos |
| 1 | 1 | `Modulo_1_Fundamentos_Databricks/01_Bienvenida_Arquitectura_Lakehouse.ipynb` | Lakehouse vs Data Warehouse/Data Lake, workspace, Compute Serverless |
| 1 | 2 | `Modulo_1_Fundamentos_Databricks/02_Entorno_UnityCatalog_GitFolders.ipynb` | Magics, Catalog Explorer, namespace UC, Git folders |
| 2 | 3 | `Modulo_2_Procesamiento_Spark/03_Transformacion_DataFrame_SparkSQL.ipynb` | DataFrame API vs Spark SQL, primera capa Silver |
| 2 | 4 | `Modulo_2_Procesamiento_Spark/04_Joins_Agregaciones_FuncionesVentana.ipynb` | Broadcast joins, embudo de conversión, funciones de ventana |
| 2 | 5 | `Modulo_2_Procesamiento_Spark/05_Particionamiento_SparkUI_ShuffleTuning.ipynb` | Planes de ejecución, shuffle partitions/AQE, cierre M2 |
| 3 | 6 | `Modulo_3_Almacenamiento_DeltaLake/06_DeltaLake_ACID_TimeTravel_Merge.ipynb` | ACID, Time Travel, MERGE INTO |
| 3 | 7 | `Modulo_3_Almacenamiento_DeltaLake/07_Optimize_ZOrder_Vacuum.ipynb` | OPTIMIZE, ZORDER, VACUUM, SQL Warehouse |
| 3 | 8 | `Modulo_3_Almacenamiento_DeltaLake/08_Lakeflow_DeclarativePipelines_AutoLoader.ipynb` (+ `08b_Pipeline_DLT_Definicion.py`) | Auto Loader, Lakeflow Declarative Pipelines, cierre M3 |
| 4 | 9 | `Modulo_4_Gobernanza_UnityCatalog/09_Catalogs_Schemas_Permisos.ipynb` | Managed vs external, GRANT/REVOKE |
| 4 | 10 | `Modulo_4_Gobernanza_UnityCatalog/10_Linaje_Auditoria.ipynb` | Linaje automático, System Tables, auditoría, cierre M4 |
| 4 | 11 | `Modulo_5_Ingenieria_Datos/11_Workflows_Jobs_ComputeServerless.ipynb` | Anatomía de un Job, SDK, Compute Serverless para Jobs |
| 5 | 12 | `Modulo_5_Ingenieria_Datos/12_Pipeline_Medallion_BronzeSilverGold.ipynb` (+ `12b_...py`) | Pipeline declarativo Bronze→Silver→Gold completo |
| 5 | 13 | `Modulo_5_Ingenieria_Datos/13_Workflows_MultiTarea_Alertas.ipynb` | Workflow multi-tarea (fan-out/fan-in), alertas, cierre M5 |
| 5 | 14 | `Modulo_6_Machine_Learning/14_MLflow_FeatureEngineering_DeteccionFraude.ipynb` | Feature Engineering en UC, MLflow Tracking, sklearn/Spark ML |
| 6 | 15 | `Modulo_6_Machine_Learning/15_ProyectoFinal_Kickoff.ipynb` | Proyecto final: rúbrica, scaffold, trabajo guiado |
| 6 | 16 | `Modulo_6_Machine_Learning/16_ProyectoFinal_ModelRegistry_ModelServing.ipynb` | Model Registry, Model Serving (CPU), cierre del curso |

## Estructura del repositorio

```
00_Setup/                              <- config central + widgets (único lugar con dbutils.widgets)
Modulo_1_Fundamentos_Databricks/
Modulo_2_Procesamiento_Spark/
Modulo_3_Almacenamiento_DeltaLake/     <- incluye 08b_Pipeline_DLT_Definicion.py (recurso Pipeline)
Modulo_4_Gobernanza_UnityCatalog/
Modulo_5_Ingenieria_Datos/             <- incluye 12b_Pipeline_Medallion_BronzeSilverGold.py (recurso Pipeline)
Modulo_6_Machine_Learning/
```

Los archivos `*b_...py` (`08b`, `12b`) son **recursos de tipo Pipeline** (Lakeflow Declarative Pipelines) — no se ejecutan con `%run`, se despliegan desde `Workflows > Lakeflow Declarative Pipelines > Create Pipeline` apuntando a ese archivo como source code.
