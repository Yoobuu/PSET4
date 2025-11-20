# PSet 4 —

Paulo Cantos.

## Estructura

- `docker-compose.yml`: servicios `postgres`, `spark-notebook` y `obt-builder`.
- `.env.example`: plantilla con todas las variables requeridas (credenciales, rutas de Parquet, años/servicios, parámetros del builder). Copiar a `.env` antes de iniciar.
- `obt_builder/`: Dockerfile + `build_obt.py`, el CLI exigido en la sección 8.2 del PDF.
- `notebooks/notebooks/01_ingesta_parquet_raw.ipynb`: backfill 2015–2025 para yellow/green hacia `raw.*`.
- `notebooks/notebooks/ml_total_amount_regression-Copy1.ipynb`: notebook de ML (pendiente renombrarlo a `ml_total_amount_regression.ipynb` para la entrega final).
- `scripts/`: SQL inicial y jars JDBC.
- `evidence/`: carpeta donde guardo logs/capturas de ingesta, builder y ML.

## Requisitos previos

- Docker Engine 24+ con plugin Compose.
- Espacio en disco (~30 GB) para los Parquet y el volumen de Postgres.
- Conexión estable para descargar los datasets desde `https://d37ci6vzurychx.cloudfront.net/trip-data` si no están en cache local.

## Configuración de variables

1. `cp .env.example .env`
2. Editar:
   - Credenciales de Postgres (`PG_*`, `PG_SCHEMA_*`).
   - Parámetros de ingesta (`PARQUET_BASE_URL`, `PARQUET_LOCAL_PATH`, `YEARS` en formato `YYYY-YYYY`, `SERVICES`, `INGEST_RUN_ID`).
   - Defaults del obt-builder (`OBT_MODE`, `OBT_YEAR_START/END`, `OBT_MONTHS`, `OBT_SERVICES`, `OBT_RUN_ID`, `OBT_OVERWRITE`).

Los notebooks y scripts leen estas variables con `python-dotenv`, así que no hace falta editarlos manualmente.

## Levantar servicios base

```bash
docker compose up -d postgres spark-notebook
```

- Postgres levanta con healthcheck (`pset4_postgres`).
- `spark-notebook` expone Jupyter en `http://localhost:${JUPYTER_PORT:-8888}` (token deshabilitado) y monta los jars en `/opt/jars`.

## Ingesta RAW (Spark → Postgres)

1. Entrar al notebook `01_ingesta_parquet_raw.ipynb` desde Jupyter.
2. Verificar que `PARQUET_LOCAL_PATH` apunta a `./data/trip-data` (ya montado) o que `PARQUET_BASE_URL` está accesible.
3. Ejecutar todo. El notebook elimina la partición previa antes de insertar (`DELETE FROM raw.<service>_taxi_trip WHERE source_year/month`).
4. Tablas destino:
   - `raw.yellow_taxi_trip`
   - `raw.green_taxi_trip`
   - `raw.taxi_zone_lookup`
5. Guardar el log de la consola (conteos por mes/año/servicio + run_id) en `evidence/ingesta.log`.

> Sugerencia: si quiero reingestar un subconjunto, ajusto `YEARS` y `SERVICES` en `.env` y vuelvo a ejecutar las celdas de la sección “Loop principal”.

## Construir `analytics.obt_trips`

`build_obt.py` cumple con todos los flags obligatorios:

```bash
docker compose run --rm obt-builder --help
```

### Comando que usará el profesor

```bash
docker compose run --rm obt-builder --full-rebuild
```

- `--full-rebuild` = `--mode full --overwrite true` → trunca `analytics.obt_trips` y procesa 2015–2025, 12 meses, yellow+green.
- El CLI imprime reintentos, filas insertadas por partición y un resumen final (total de particiones, total de filas, duración).
- Guardar el output en `evidence/obt_full.log`.

### Ejecución by-partition

```bash
docker compose run --rm obt-builder \
  --mode by-partition \
  --year-start 2023 --year-end 2023 \
  --months 1 2 \
  --services yellow \
  --run-id reparacion-2023q1 \
  --overwrite false
```

Esto permite reruns selectivos sin truncar la tabla completa. El `run_id` del builder se escribe en `analytics.obt_trips.run_id` para trazabilidad.

## Notebook de ML

- Archivo actual: `notebooks/notebooks/ml_total_amount_regression-Copy1.ipynb` (antes de publicar lo renombro a `ml_total_amount_regression.ipynb`).
- Lectura de datos vía SQL sobre `analytics.obt_trips` (solo features disponibles al pickup para evitar leakage).
- Modelos propios: Linear, Ridge (L2), Lasso (L1), Elastic Net (L1+L2) con SGD mini-batch y `random_state=42`.
- Modelos sklearn equivalentes: `LinearRegression`, `Ridge`, `Lasso`, `ElasticNet` (pendiente incorporar `SGDRegressor` antes de entregar, según PDF).
- Exporto tablas y gráficos a `notebooks/notebooks/exports/` (ej. `model_comparison_summary.csv`, `loss_curve_ridge_poly.png`).
- Evidencias requeridas: tabla RMSE/MAE/R² (validación y test), tiempos de entrenamiento, gráficas de residuales y errores por bucket.

## Carpeta `evidence/`

Para la entrega final debo incluir:

1. Log de ingesta (counts por mes + run_id).
2. Log del obt-builder `--full-rebuild`.
3. Tabla comparativa de los 8 modelos (propios vs sklearn) y capturas de residuales/buckets.

## Validaciones previas a entregar

1. Revisar conteos en Postgres (`SELECT COUNT(*) FROM analytics.obt_trips WHERE source_year=2022;`).
2. Confirmar que los archivos exportados coinciden con lo descrito en el README.
3. Ejecutar `docker compose down` para apagar contenedores cuando termine.

## Troubleshooting rápido

- Ajustar recursos Spark si la JVM muere (`.config("spark.driver.memory", "4g")`).
- Si el builder falla repetidamente en una partición, revisar el stacktrace (se imprime porque `safe_build_month` captura la excepción) y corregir los datos RAW antes de reintentar.
- Asegurar que los Parquet sigan la nomenclatura TLC (`<service>_tripdata_YYYY-MM.parquet`).

## Reproducibilidad

- Todas las semillas están fijadas en 42 (tanto en los estimadores scratch como en sklearn).
- Ingesta y builder son idempotentes: siempre se borra la partición antes de escribir, y existe `--overwrite` para truncar completo.
- `run_id` del builder se propaga hasta `analytics.obt_trips` para auditar ejecuciones.

## Nota final

- Comando de evaluación oficial: `docker compose run obt-builder --full-rebuild` (ya configurado como entrypoint).
- Mantener sincronizados `.env` y `.env.example` cuando cambie la configuración para que el revisor pueda replicar todo sin sorpresas.
