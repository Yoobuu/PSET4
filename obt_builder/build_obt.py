#!/usr/bin/env python3
"""CLI para construir analytics.obt_trips usando Spark desde RAW."""

import argparse
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Iterable, List, Optional

from pyspark.sql import SparkSession

RAW_SCHEMA = os.getenv("PG_SCHEMA_RAW", "raw")
ANALYTICS_SCHEMA = os.getenv("PG_SCHEMA_ANALYTICS", "analytics")

ALLOWED_SERVICES = ("yellow", "green")
ALL_MONTHS = list(range(1, 13))
DEFAULT_YEAR_START = int(os.getenv("OBT_YEAR_START", "2015"))
DEFAULT_YEAR_END = int(os.getenv("OBT_YEAR_END", "2025"))


def _split_csv_tokens(values: Optional[Iterable[str]]) -> List[str]:
    tokens: List[str] = []
    if not values:
        return tokens
    for value in values:
        if value is None:
            continue
        for chunk in str(value).split(","):
            chunk = chunk.strip()
            if chunk:
                tokens.append(chunk)
    return tokens


def _parse_months(tokens: Iterable[str]) -> List[int]:
    months: List[int] = []
    for token in tokens:
        try:
            month = int(token)
        except ValueError as exc:
            raise ValueError(f"Mes inválido '{token}'.") from exc
        if month < 1 or month > 12:
            raise ValueError(f"Mes fuera de rango '{month}'. Use valores 1-12.")
        if month not in months:
            months.append(month)
    return months


def _parse_services(tokens: Iterable[str]) -> List[str]:
    services: List[str] = []
    for token in tokens:
        service = token.strip().lower()
        if service not in ALLOWED_SERVICES:
            raise ValueError(
                f"Servicio inválido '{token}'. Valores permitidos: {', '.join(ALLOWED_SERVICES)}."
            )
        if service not in services:
            services.append(service)
    return services


def _str2bool(value: Optional[str]) -> Optional[bool]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "t", "yes", "y"}:
        return True
    if normalized in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError(f"Valor booleano inválido '{value}'. Usa true/false.")


def _default_run_id() -> str:
    return f"obt-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def get_spark():
    return (
        SparkSession.builder.appName("obt-builder")
        .config("spark.jars", "/opt/jars/postgresql-42.7.4.jar")
        .config("spark.driver.extraClassPath", "/opt/jars/postgresql-42.7.4.jar")
        .getOrCreate()
    )


def build_obt_month(
    spark,
    service: str,
    year: int,
    month: int,
    builder_run_id: str,
) -> int:
    """
    Construye la capa analytics.obt_trips para un (service, year, month)
    a partir de raw.<service>_taxi_trip.

    service: 'yellow' o 'green'
    year, month: partición a procesar

    Propiedades:
    - Idempotente: borra antes las filas de ese (service, year, month) en analytics.obt_trips.
    - Lee desde RAW vía JDBC.
    - Junta con raw.taxi_zone_lookup.
    - Calcula columnas derivadas (hour, dow, duration, speed, tip_pct, etc.).
    - Escribe en analytics.obt_trips vía JDBC (append).
    - Devuelve el número de filas insertadas (según Postgres).
    """
    from datetime import timedelta
    import time
    import psycopg2
    from pyspark.sql import functions as F

    # --- Config Postgres desde env (igual que en ingest_month) ---
    PG_HOST = os.getenv("PG_HOST", "postgres")
    PG_PORT = os.getenv("PG_PORT", "5432")
    PG_DB   = os.getenv("PG_DB", "nyctaxi")
    PG_USER = os.getenv("PG_USER", "pset")
    PG_PWD  = os.getenv("PG_PASSWORD", "pset_password")

    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?reWriteBatchedInserts=true"
    jdbc_props = {
        "driver": "org.postgresql.Driver",
        "user": PG_USER,
        "password": PG_PWD,
        "fetchsize": "10000",
    }
    analytics_table = f"{ANALYTICS_SCHEMA}.obt_trips"

    print("")
    print("=" * 80)
    print(f"[OBT] Construyendo analytics.obt_trips para {service.upper()} {year}-{month:02d}")
    t0 = time.time()

    # ------------------------------------------------------------------
    # 1) Borrar previamente la partición en analytics.obt_trips (idempotencia)
    # ------------------------------------------------------------------
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PWD,
    )
    cur = conn.cursor()
    cur.execute(
        f"""
        DELETE FROM {analytics_table}
        WHERE service_type = %s
          AND source_year  = %s
          AND source_month = %s;
        """,
        (service, int(year), int(month)),
    )
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"[OBT] Borradas filas previas en analytics.obt_trips: {deleted}")

    # ------------------------------------------------------------------
    # 2) Leer RAW para este (service, year, month) desde raw.<service>_taxi_trip
    #    Filtrando duraciones negativas (dropoff < pickup)
    # ------------------------------------------------------------------
    raw_table = f"{RAW_SCHEMA}.{service}_taxi_trip"
    raw_query = f"""
        (SELECT *
         FROM {raw_table}
         WHERE service_type = '{service}'
           AND source_year  = {int(year)}
           AND source_month = {int(month)}
           AND dropoff_datetime >= pickup_datetime) AS t
    """

    raw_df = (
        spark.read
             .jdbc(url=jdbc_url, table=raw_query, properties=jdbc_props)
    )

    if raw_df.rdd.isEmpty():
        print(f"[OBT] No hay filas en RAW para {service} {year}-{month:02d}. Nada que hacer.")
        return 0

    # ------------------------------------------------------------------
    # 3) Leer lookup de zonas
    # ------------------------------------------------------------------
    zones_df = (
        spark.read
             .jdbc(
                 url=jdbc_url,
                 table=f"{RAW_SCHEMA}.taxi_zone_lookup",
                 properties=jdbc_props,
             )
    )

    pu_zones_df = (
        zones_df
        .select(
            F.col("locationid").alias("pu_location_id"),
            F.col("zone").alias("pu_zone"),
            F.col("borough").alias("pu_borough"),
        )
    )

    do_zones_df = (
        zones_df
        .select(
            F.col("locationid").alias("do_location_id"),
            F.col("zone").alias("do_zone"),
            F.col("borough").alias("do_borough"),
        )
    )

    # ------------------------------------------------------------------
    # 4) Enriquecer con derivadas y dimensiones básicas
    # ------------------------------------------------------------------
    df = (
        raw_df
        # tiempo
        .withColumn("pickup_hour", F.hour("pickup_datetime").cast("smallint"))
        .withColumn("pickup_dow", F.dayofweek("pickup_datetime").cast("smallint"))
        .withColumn("month", F.col("source_month").cast("smallint"))
        .withColumn("year", F.col("source_year").cast("smallint"))

        # claves de zona
        .withColumn("pu_location_id", F.col("PULocationID").cast("int"))
        .withColumn("do_location_id", F.col("DOLocationID").cast("int"))

        # vendor
        .withColumn("vendor_id", F.col("VendorID").cast("int"))
        .withColumn(
            "vendor_name",
            F.when(F.col("VendorID") == 1, "Creative Mobile Technologies")
             .when(F.col("VendorID") == 2, "VeriFone Inc")
             .otherwise("Other"),
        )

        # rate_code
        .withColumn("rate_code_id", F.col("RatecodeID").cast("int"))
        .withColumn(
            "rate_code_desc",
            F.when(F.col("RatecodeID") == 1, "Standard rate")
             .when(F.col("RatecodeID") == 2, "JFK")
             .when(F.col("RatecodeID") == 3, "Newark")
             .when(F.col("RatecodeID") == 4, "Nassau or Westchester")
             .when(F.col("RatecodeID") == 5, "Negotiated fare")
             .when(F.col("RatecodeID") == 6, "Group ride")
             .otherwise("Other"),
        )

        # payment_type + desc
        .withColumn("payment_type", F.col("payment_type").cast("int"))
        .withColumn(
            "payment_type_desc",
            F.when(F.col("payment_type") == 1, "Credit card")
             .when(F.col("payment_type") == 2, "Cash")
             .when(F.col("payment_type") == 3, "No charge")
             .when(F.col("payment_type") == 4, "Dispute")
             .when(F.col("payment_type") == 5, "Unknown")
             .when(F.col("payment_type") == 6, "Voided trip")
             .otherwise("Other"),
        )

        # derivadas
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0,
        )
        .withColumn(
            "avg_speed_mph",
            F.when(
                (F.col("trip_distance") > 0) & (F.col("trip_duration_min") > 0),
                F.col("trip_distance") / (F.col("trip_duration_min") / 60.0),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "tip_pct",
            F.when(F.col("fare_amount") > 0,
                   (F.col("tip_amount") / F.col("fare_amount")) * 100.0
            ).otherwise(F.lit(None).cast("double")),
        )
    )

    # run_id del builder para rastrear este run
    df = df.withColumn("run_id", F.lit(builder_run_id))

    # trip_type: solo existe en green; si no existe, ponemos NULL
    if "trip_type" in df.columns:
        df = df.withColumn("trip_type", F.col("trip_type").cast("int"))
    else:
        df = df.withColumn("trip_type", F.lit(None).cast("int"))

    # Join con zonas pickup y dropoff
    df = (
        df
        .join(pu_zones_df, on="pu_location_id", how="left")
        .join(do_zones_df, on="do_location_id", how="left")
    )

    # airport_fee: existe solo en yellow; para green la rellenamos con NULL
    if "airport_fee" not in df.columns:
        df = df.withColumn("airport_fee", F.lit(None).cast("double"))

    # ------------------------------------------------------------------
    # 5) Seleccionar columnas finales en el orden del DDL de analytics.obt_trips
    # ------------------------------------------------------------------
    obt_df = (
        df.select(
            # Tiempo
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_hour",
            "pickup_dow",
            "month",
            "year",

            # Ubicación
            "pu_location_id",
            "pu_zone",
            "pu_borough",
            "do_location_id",
            "do_zone",
            "do_borough",

            # Servicio / Códigos
            "service_type",
            "vendor_id",
            "vendor_name",
            "rate_code_id",
            "rate_code_desc",
            "payment_type",
            "payment_type_desc",
            "trip_type",

            # Viaje / Montos
            F.col("passenger_count").cast("int").alias("passenger_count"),
            "trip_distance",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            F.col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
            F.col("airport_fee").cast("double").alias("airport_fee"),
            "total_amount",
            "store_and_fwd_flag",

            # Derivadas
            "trip_duration_min",
            "avg_speed_mph",
            "tip_pct",

            # Metadatos
            "run_id",
            "source_year",
            "source_month",
            "ingested_at_utc",
        )
    )

    # ------------------------------------------------------------------
    # 6) Escribir en analytics.obt_trips vía JDBC (sin count() previo)
    # ------------------------------------------------------------------
    write_props = {
        "user": PG_USER,
        "password": PG_PWD,
        "driver": "org.postgresql.Driver",
        "batchsize": "10000",
    }

    (
        obt_df
        .write
        .mode("append")
        .jdbc(
            url=jdbc_url,
            table=analytics_table,
            properties=write_props,
        )
    )

    # Conteo más barato: lo hacemos en Postgres después de escribir
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PWD,
    )
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM {analytics_table}
        WHERE service_type = %s
          AND source_year  = %s
          AND source_month = %s;
        """,
        (service, int(year), int(month)),
    )
    cnt = cur.fetchone()[0]
    cur.close()
    conn.close()

    elapsed = time.time() - t0
    print(f"[OBT] {service.upper()} {year}-{month:02d} COMPLETADO en {timedelta(seconds=int(elapsed))}")
    print(f"[OBT] Filas en analytics.obt_trips para {service} {year}-{month:02d}: {cnt}")

    return cnt


def truncate_obt_table():
    import psycopg2

    PG_HOST = os.getenv("PG_HOST", "postgres")
    PG_PORT = os.getenv("PG_PORT", "5432")
    PG_DB = os.getenv("PG_DB", "nyctaxi")
    PG_USER = os.getenv("PG_USER", "pset")
    PG_PWD = os.getenv("PG_PASSWORD", "pset_password")
    analytics_table = f"{ANALYTICS_SCHEMA}.obt_trips"

    print(f"[INFO] --overwrite activo. Truncando {analytics_table}...")
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PWD,
    )
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {analytics_table};")
    conn.commit()
    cur.close()
    conn.close()
    print(f"[INFO] Tabla {analytics_table} truncada.")


def safe_build_month(spark, service, year, month, builder_run_id, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] Procesando {service} {year}-{month:02d} (intento {attempt}/{max_retries})")
            t0 = time.time()

            rows_inserted = build_obt_month(spark, service, year, month, builder_run_id)

            dt = time.time() - t0
            print(f"[OK] {service} {year}-{month:02d} completado en {dt:.2f}s")
            return rows_inserted

        except Exception:
            print(f"[WARN] Falló {service} {year}-{month:02d} en intento {attempt}")
            traceback.print_exc()
            time.sleep(1)

    print(f"[ERROR] {service} {year}-{month:02d} falló definitivamente.")
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Constructor del OBT (analytics.obt_trips)"
    )
    parser.add_argument("--mode", choices=["full", "by-partition"], help="Modo de ejecución")
    parser.add_argument("--year-start", type=int, help="Año inicial a procesar")
    parser.add_argument("--year-end", type=int, help="Año final a procesar")
    parser.add_argument(
        "--months",
        nargs="+",
        help="Meses a procesar (lista o valores separados por coma, ej. 1 2 o 1,2)",
    )
    parser.add_argument(
        "--services",
        nargs="+",
        help="Servicios a procesar (yellow, green o ambos)",
    )
    parser.add_argument("--run-id", help="Identificador del run del builder")
    parser.add_argument(
        "--overwrite",
        nargs="?",
        type=_str2bool,
        const=True,
        help="true/false para recrear analytics.obt_trips antes de escribir",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Alias de conveniencia para --mode full + --overwrite true",
    )

    args = parser.parse_args()

    if getattr(args, "full_rebuild", False):
        args.mode = "full"

    mode = (args.mode or os.getenv("OBT_MODE") or "by-partition").lower()
    if mode not in {"full", "by-partition"}:
        parser.error("--mode debe ser 'full' o 'by-partition'.")
    args.mode = mode

    year_start = args.year_start if args.year_start is not None else DEFAULT_YEAR_START
    year_end = args.year_end if args.year_end is not None else DEFAULT_YEAR_END

    if args.mode == "full":
        year_start = DEFAULT_YEAR_START
        year_end = DEFAULT_YEAR_END

    if year_start > year_end:
        parser.error("--year-start debe ser <= --year-end.")
    args.year_start = year_start
    args.year_end = year_end

    months_tokens = _split_csv_tokens(args.months)
    env_months_raw = os.getenv("OBT_MONTHS")
    env_months_tokens = _split_csv_tokens([env_months_raw]) if env_months_raw else []
    try:
        months = _parse_months(months_tokens) if months_tokens else None
        if months is None:
            months = _parse_months(env_months_tokens) if env_months_tokens else ALL_MONTHS.copy()
    except ValueError as exc:
        parser.error(str(exc))
    if args.mode == "full":
        months = ALL_MONTHS.copy()
    if not months:
        parser.error("Debe especificar al menos un mes válido.")
    args.months = sorted(months)

    services_tokens = _split_csv_tokens(args.services)
    env_services_raw = os.getenv("OBT_SERVICES")
    env_services_tokens = _split_csv_tokens([env_services_raw]) if env_services_raw else []
    try:
        services = (
            _parse_services(services_tokens)
            if services_tokens
            else _parse_services(env_services_tokens)
            if env_services_tokens
            else list(ALLOWED_SERVICES)
        )
    except ValueError as exc:
        parser.error(str(exc))
    if args.mode == "full":
        services = list(ALLOWED_SERVICES)
    if not services:
        parser.error("Debe especificar al menos un servicio válido.")
    args.services = services

    run_id = args.run_id or os.getenv("OBT_RUN_ID")
    args.run_id = run_id or _default_run_id()

    overwrite_cli = args.overwrite
    if overwrite_cli is None:
        env_overwrite = os.getenv("OBT_OVERWRITE")
        if env_overwrite is not None:
            try:
                overwrite_cli = _str2bool(env_overwrite)
            except ValueError as exc:
                parser.error(str(exc))
    args.overwrite = bool(overwrite_cli) if overwrite_cli is not None else False
    if args.mode == "full":
        args.overwrite = True

    return args



def main():
    args = parse_args()
    spark = get_spark()

    print("[INFO] Iniciando obt-builder...")
    print(f"[INFO] Modo: {args.mode}")
    print(f"[INFO] Run ID: {args.run_id}")
    print(f"[INFO] Overwrite analytics.obt_trips: {args.overwrite}")
    print(f"[INFO] Servicios: {', '.join(args.services)}")
    print(f"[INFO] Años: {args.year_start}–{args.year_end}")
    print(f"[INFO] Meses: {', '.join(f'{m:02d}' for m in args.months)}")

    if args.overwrite:
        truncate_obt_table()

    summary = []
    run_start = time.time()

    for service in args.services:
        for year in range(args.year_start, args.year_end + 1):
            for month in args.months:
                partition_start = time.time()
                rows = safe_build_month(spark, service, year, month, args.run_id)
                elapsed = time.time() - partition_start
                if rows is None:
                    print(f"[ERROR] Se detuvo la construcción para {service} {year}-{month:02d}")
                    spark.stop()
                    sys.exit(1)
                summary.append(
                    {
                        "service": service,
                        "year": year,
                        "month": month,
                        "rows": rows,
                        "elapsed": elapsed,
                    }
                )

    total_elapsed = time.time() - run_start
    total_rows = sum(item["rows"] for item in summary)

    print("\n[INFO] Resumen de particiones procesadas:")
    for item in summary:
        print(
            f"    - {item['service']} {item['year']}-{item['month']:02d}: "
            f"{item['rows']} filas en {item['elapsed']:.2f}s"
        )
    print(f"[INFO] Total particiones: {len(summary)}")
    print(f"[INFO] Total filas insertadas: {total_rows}")
    print(f"[INFO] Duración total: {total_elapsed:.2f}s")

    print("[INFO] Construcción completada.")
    spark.stop()


if __name__ == "__main__":
    main()
