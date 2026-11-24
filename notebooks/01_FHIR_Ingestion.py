import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, IntegerType, LongType, StringType, StructField, StructType,
    TimestampType,
)

spark.conf.set("spark.sql.session.timeZone", "UTC")

BASE_URL = "https://hapi.fhir.org/baseR4"
RESOURCES = ("Patient", "Encounter", "Observation", "Condition")
PAGE_SIZE = 100
INITIAL_LOOKBACK_DAYS = 3
WATERMARK_OVERLAP_MINUTES = 10
REQUEST_TIMEOUT = (10, 60)
TABLE_SCHEMA = "dbo"
RAW_ROOT = "Files/raw"

RUN_ID = str(uuid.uuid4())
RUN_STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

BRONZE_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("resource_type", StringType(), False),
    StructField("resource_id", StringType(), True),
    StructField("resource_last_updated", StringType(), True),
    StructField("resource_json", StringType(), False),
    StructField("resource_content_hash", StringType(), False),
    StructField("extraction_timestamp", TimestampType(), False),
    StructField("api_url_or_params", StringType(), False),
    StructField("api_called_at", TimestampType(), False),
    StructField("data_saved_at", TimestampType(), False),
    StructField("raw_file_path", StringType(), False),
    StructField("ingestion_date", DateType(), False),
])

AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("resource_name", StringType(), False),
    StructField("stage", StringType(), False),
    StructField("page_number", IntegerType(), True),
    StructField("request_url", StringType(), True),
    StructField("api_called_at", TimestampType(), True),
    StructField("response_received_at", TimestampType(), True),
    StructField("data_saved_at", TimestampType(), True),
    StructField("response_status", IntegerType(), True),
    StructField("record_count", LongType(), True),
    StructField("raw_file_path", StringType(), True),
    StructField("status", StringType(), False),
    StructField("error_message", StringType(), True),
])

STATE_SCHEMA = StructType([
    StructField("resource_name", StringType(), False),
    StructField("last_successful_at", TimestampType(), False),
    StructField("run_id", StringType(), False),
    StructField("updated_at", TimestampType(), False),
])

retry = Retry(
    total=4,
    connect=4,
    read=4,
    status=4,
    backoff_factor=1,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset({"GET"}),
    respect_retry_after_header=True,
)
http = requests.Session()
http.mount("https://", HTTPAdapter(max_retries=retry))
http.headers.update({"Accept": "application/fhir+json"})

def table(name: str) -> str:
    return f"{TABLE_SCHEMA}.{name}"


def table_exists(name: str) -> bool:
    return spark.catalog.tableExists(name)


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def format_fhir_time(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_control_tables() -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table("ingestion_log")} (
            run_id STRING, resource_name STRING, stage STRING,
            page_number INT, request_url STRING,
            api_called_at TIMESTAMP, response_received_at TIMESTAMP,
            data_saved_at TIMESTAMP, response_status INT,
            record_count BIGINT, raw_file_path STRING,
            status STRING, error_message STRING
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table("ingestion_state")} (
            resource_name STRING, last_successful_at TIMESTAMP,
            run_id STRING, updated_at TIMESTAMP
        ) USING DELTA
    """)


def audit_event(
    resource: str,
    stage: str,
    status: str,
    *,
    page_number: Optional[int] = None,
    request_url: Optional[str] = None,
    api_called_at: Optional[datetime] = None,
    response_received_at: Optional[datetime] = None,
    data_saved_at: Optional[datetime] = None,
    response_status: Optional[int] = None,
    record_count: Optional[int] = None,
    raw_file_path: Optional[str] = None,
    error_message: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "run_id": RUN_ID,
        "resource_name": resource,
        "stage": stage,
        "page_number": page_number,
        "request_url": request_url,
        "api_called_at": api_called_at,
        "response_received_at": response_received_at,
        "data_saved_at": data_saved_at,
        "response_status": response_status,
        "record_count": record_count,
        "raw_file_path": raw_file_path,
        "status": status,
        "error_message": error_message[:2000] if error_message else None,
    }


def append_audit(events: List[Dict[str, Any]]) -> None:
    if events:
        (spark.createDataFrame(events, AUDIT_SCHEMA)
         .write.format("delta").mode("append")
         .saveAsTable(table("ingestion_log")))


def window_start(resource: str) -> datetime:
    state = (
        spark.table(table("ingestion_state"))
        .filter(F.col("resource_name") == resource)
        .select("last_successful_at")
        .first()
    )
    if state and state["last_successful_at"]:
        return state["last_successful_at"] - timedelta(
            minutes=WATERMARK_OVERLAP_MINUTES
        )
    return RUN_STARTED_AT - timedelta(days=INITIAL_LOOKBACK_DAYS)


def update_watermark(resource: str) -> None:
    source = spark.createDataFrame([{
        "resource_name": resource,
        "last_successful_at": RUN_STARTED_AT,
        "run_id": RUN_ID,
        "updated_at": now_utc(),
    }], STATE_SCHEMA)

    (DeltaTable.forName(spark, table("ingestion_state"))
     .alias("target")
     .merge(source.alias("source"),
            "target.resource_name = source.resource_name")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())

def save_raw(resource: str, page_number: int, body: str) -> str:
    date_folder = RUN_STARTED_AT.strftime("%Y-%m-%d")
    folder = f"{RAW_ROOT}/{resource.lower()}/{date_folder}/{RUN_ID}"
    notebookutils.fs.mkdirs(folder)

    path = f"{folder}/page-{page_number:05d}.json"
    if not notebookutils.fs.put(path, body, overwrite=False):
        raise IOError(f"Failed to save raw API response: {path}")
    return path


def make_bronze_record(
    resource_name: str,
    item: Dict[str, Any],
    request_url: str,
    called_at: datetime,
    saved_at: datetime,
    raw_path: str,
) -> Dict[str, Any]:
    resource_json = json.dumps(
        item, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return {
        "run_id": RUN_ID,
        "resource_type": item.get("resourceType", resource_name),
        "resource_id": item.get("id"),
        "resource_last_updated": (item.get("meta") or {}).get("lastUpdated"),
        "resource_json": resource_json,
        "resource_content_hash": hashlib.sha256(
            resource_json.encode("utf-8")
        ).hexdigest(),
        "extraction_timestamp": RUN_STARTED_AT,
        "api_url_or_params": request_url,
        "api_called_at": called_at,
        "data_saved_at": saved_at,
        "raw_file_path": raw_path,
        "ingestion_date": saved_at.date(),
    }


def next_url(bundle: Dict[str, Any]) -> Optional[str]:
    for link in bundle.get("link", []):
        if link.get("relation") == "next":
            return link.get("url")
    return None


def fetch_resource(
    resource: str,
    start_at: datetime,
    events: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/{resource}"
    params: Optional[List[Tuple[str, str]]] = [
        ("_lastUpdated", f"ge{format_fhir_time(start_at)}"),
        ("_count", str(PAGE_SIZE)),
    ]
    seen_urls = set()
    records = []
    page_number = 0

    while url:
        if url in seen_urls:
            raise RuntimeError(f"Pagination loop detected for {resource}")
        seen_urls.add(url)
        page_number += 1
        called_at = now_utc()
        response = None
        received_at = None
        raw_path = None
        request_url = url

        try:
            response = http.get(url, params=params, timeout=REQUEST_TIMEOUT)
            received_at = now_utc()
            request_url = response.url
            response.raise_for_status()

            # Persist the response body before parsing; do not reserialize it.
            body = response.content.decode("utf-8")
            raw_path = save_raw(resource, page_number, body)
            saved_at = now_utc()

            bundle = json.loads(body)
            if bundle.get("resourceType") != "Bundle":
                raise ValueError(
                    f"Expected FHIR Bundle, got {bundle.get('resourceType')!r}"
                )

            page_records = []
            for entry in bundle.get("entry") or []:
                item = entry.get("resource")
                if isinstance(item, dict):
                    page_records.append(make_bronze_record(
                        resource, item, request_url, called_at, saved_at, raw_path
                    ))
            records.extend(page_records)

            events.append(audit_event(
                resource, "api", "SUCCESS",
                page_number=page_number,
                request_url=request_url,
                api_called_at=called_at,
                response_received_at=received_at,
                data_saved_at=saved_at,
                response_status=response.status_code,
                record_count=len(page_records),
                raw_file_path=raw_path,
            ))
            url = next_url(bundle)
            params = None
        except Exception as exc:
            events.append(audit_event(
                resource, "api", "FAILED",
                page_number=page_number,
                request_url=request_url,
                api_called_at=called_at,
                response_received_at=received_at,
                response_status=response.status_code if response else None,
                raw_file_path=raw_path,
                error_message=str(exc),
            ))
            raise

    return records

def write_bronze(resource: str, records: List[Dict[str, Any]]) -> DataFrame:
    bronze = spark.createDataFrame(records, schema=BRONZE_SCHEMA)
    writer = bronze.write.format("delta").mode("append")
    if not table_exists(table(f"bronze_{resource.lower()}")):
        writer = writer.partitionBy("ingestion_date")
    writer.saveAsTable(table(f"bronze_{resource.lower()}"))
    return bronze


def build_silver_source(bronze: DataFrame) -> DataFrame:
    latest = Window.partitionBy("resource_id").orderBy(
        F.col("resource_last_updated").desc_nulls_last(),
        F.col("data_saved_at").desc(),
    )
    return (
        bronze.filter(F.col("resource_id").isNotNull())
        .withColumn("_rank", F.row_number().over(latest))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .withColumn("valid_from", F.col("extraction_timestamp"))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("scd_updated_at", F.col("extraction_timestamp"))
    )


def upsert_scd2(resource: str, source: DataFrame) -> None:
    target_name = table(f"silver_{resource.lower()}")
    if not table_exists(target_name):
        source.limit(0).write.format("delta").mode("overwrite").saveAsTable(target_name)

    condition = "target.resource_id = source.resource_id AND target.is_current = true"

    # Close the prior version when the incoming content hash has changed.
    (DeltaTable.forName(spark, target_name)
     .alias("target")
     .merge(source.alias("source"), condition)
     .whenMatchedUpdate(
         condition="target.resource_content_hash <> source.resource_content_hash",
         set={
             "is_current": "false",
             "valid_to": "source.valid_from",
             "scd_updated_at": "source.scd_updated_at",
         },
     )
     .execute())

    # Insert new resources and the replacement version of changed resources.
    (DeltaTable.forName(spark, target_name)
     .alias("target")
     .merge(source.alias("source"), condition)
     .whenNotMatchedInsertAll()
     .execute())

GOLD_FIELDS = {
    "Patient": {
        "active": "$.active", "gender": "$.gender", "birth_date": "$.birthDate",
        "given_name": "$.name[0].given[0]", "family_name": "$.name[0].family",
        "city": "$.address[0].city", "country": "$.address[0].country",
    },
    "Encounter": {
        "patient_reference": "$.subject.reference", "status": "$.status",
        "class_code": "$.class.code", "class_display": "$.class.display",
        "start_time": "$.period.start", "end_time": "$.period.end",
        "service_provider_reference": "$.serviceProvider.reference",
    },
    "Observation": {
        "patient_reference": "$.subject.reference",
        "encounter_reference": "$.encounter.reference",
        "status": "$.status", "code_text": "$.code.text",
        "code": "$.code.coding[0].code",
        "code_system": "$.code.coding[0].system",
        "value_quantity": "$.valueQuantity.value",
        "value_unit": "$.valueQuantity.unit",
        "value_string": "$.valueString",
        "effective_datetime": "$.effectiveDateTime",
    },
    "Condition": {
        "patient_reference": "$.subject.reference",
        "encounter_reference": "$.encounter.reference",
        "clinical_status": "$.clinicalStatus.coding[0].code",
        "verification_status": "$.verificationStatus.coding[0].code",
        "code_text": "$.code.text", "code": "$.code.coding[0].code",
        "onset_datetime": "$.onsetDateTime", "recorded_date": "$.recordedDate",
    },
}


def materialize_gold(resource: str) -> int:
    silver_name = table(f"silver_{resource.lower()}")
    gold_name = table(f"gold_{resource.lower()}")
    current = spark.table(silver_name).filter(F.col("is_current"))

    columns = [
        F.col("resource_id"),
        F.col("resource_last_updated"),
        F.col("resource_content_hash"),
        F.col("valid_from"),
        F.col("valid_to"),
    ]
    columns.extend(
        F.get_json_object("resource_json", path).alias(name)
        for name, path in GOLD_FIELDS[resource].items()
    )

    gold = current.select(*columns)
    (gold.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(gold_name))

    spark.sql(
        f"CREATE OR REPLACE VIEW {table(f'vw_gold_{resource.lower()}')} "
        f"AS SELECT * FROM {gold_name}"
    )
    return gold.count()

def process_resource(resource: str) -> None:
    events: List[Dict[str, Any]] = []
    start_at = window_start(resource)

    try:
        records = fetch_resource(resource, start_at, events)
        bronze = write_bronze(resource, records)
        silver_source = build_silver_source(bronze)
        upsert_scd2(resource, silver_source)
        gold_count = materialize_gold(resource)
        update_watermark(resource)

        events.append(audit_event(
            resource, "pipeline", "SUCCESS", record_count=len(records)
        ))
        events.append(audit_event(
            resource, "gold", "SUCCESS", record_count=gold_count
        ))
        append_audit(events)
        print(f"{resource}: {len(records)} ingested; {gold_count} current Gold rows.")
    except Exception as exc:
        events.append(audit_event(
            resource, "pipeline", "FAILED", error_message=str(exc)
        ))
        append_audit(events)
        raise


def run_pipeline() -> None:
    ensure_control_tables()
    for resource in RESOURCES:
        process_resource(resource)


run_pipeline()