import logging
import os
from urllib import parse
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
    OTLPMetricExporter,
)
from opentelemetry.metrics import (
    CallbackOptions,
    Observation,
    Instrument,
    get_meter_provider,
    set_meter_provider,
)
from opentelemetry.sdk.metrics import MeterProvider, Meter, Counter
from opentelemetry.sdk.metrics.export import (
    PeriodicExportingMetricReader,
    AggregationTemporality,
)
from opentelemetry.sdk.metrics.view import LastValueAggregation
from opentelemetry.sdk.resources import (
    SERVICE_NAME,
    OTEL_SERVICE_NAME,
    OTEL_RESOURCE_ATTRIBUTES,
    Resource,
)
from config import config
_LOGGER = logging.getLogger(__name__)


def _detect_resource_attribute() -> "dict":
    """detect any attribute configuration set in environment variables"""
    env_resources_items = os.environ.get(OTEL_RESOURCE_ATTRIBUTES)
    env_resource_map = {}

    if env_resources_items:
        for item in env_resources_items.split(","):
            try:
                key, value = item.split("=", maxsplit=1)
            except ValueError as exc:
                _LOGGER.warning(
                    "Invalid key value resource attribute pair %s: %s",
                    item,
                    exc,
                )
                continue
            value_url_decoded = parse.unquote(value.strip())
            env_resource_map[key.strip()] = value_url_decoded
    return env_resource_map


# Define all metrics names
METRIC = "atriumdb.tscgenerator."
TSCGENERATOR_ERRORS = METRIC + "errors"
TSCGENERATOR_PROCESSED_WAL_FILE = METRIC + "wal.files.processed"
TSCGENERATOR_WAL_FILE_EMPTY = METRIC + "empty.wal.files"
TSCGENERATOR_DUPLICATE_WAL_FILE = METRIC + "duplicate.wal.files"
TSCGENERATOR_DEVICES_INSERTED = METRIC + "devices.inserted"
TSCGENERATOR_MEASURES_INSERTED = METRIC + "measures.inserted"

# Set global Metrics module values
EXPORT_INTERVAL = os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", 5_000)
temporality_cumulative = {Counter: AggregationTemporality.CUMULATIVE}
temporality_delta = {Counter: AggregationTemporality.DELTA}
aggregation_last_value = {Counter: LastValueAggregation()}
resource_attrib = _detect_resource_attribute()
resource_attrib[SERVICE_NAME] = os.environ.get(OTEL_SERVICE_NAME, "atriumdb-tscgenerator")
# here you can add more custom labels that are useful for metrics timeseries
# resource_attrib["custom key"] = "value"
resource_attrib["atriumdb_instance"] = config.instance_name


# TO DO verify that this is not over exposed
# we want that port 4317 not to be bound to external network traffic
_exporter = OTLPMetricExporter(
    insecure=True,
    # preferred_aggregation=aggregation_last_value,
    preferred_temporality=temporality_cumulative,
)

_reader = PeriodicExportingMetricReader(_exporter, export_interval_millis=EXPORT_INTERVAL)
_resource = Resource(attributes=resource_attrib)
_provider = MeterProvider(metric_readers=[_reader], resource=_resource)
set_meter_provider(_provider)
_meter = get_meter_provider().get_meter("opentelemetry.instrumentation.tsc-generator")

_ADAPTER_METRICS = None


def _init_metrics(meter: Meter):
    global _ADAPTER_METRICS
    if not _ADAPTER_METRICS:
        exception_counter = meter.create_counter(
            TSCGENERATOR_ERRORS,
            description="number of non critical errors"
        )
        processed_wal_file_counter = meter.create_counter(
            TSCGENERATOR_PROCESSED_WAL_FILE,
            description="number of processed wal files",
        )
        empty_wal_file_counter = meter.create_counter(
            TSCGENERATOR_WAL_FILE_EMPTY,
            description="number of empty wal files deleted",
        )
        duplicate_wal_file_counter = meter.create_counter(
            TSCGENERATOR_DUPLICATE_WAL_FILE,
            description="number of duplicate wal files deleted",
        )
        devices_inserted_counter = meter.create_counter(
            TSCGENERATOR_DEVICES_INSERTED,
            description="number of devices inserted (should be 0)",
        )
        measures_inserted_counter = meter.create_counter(
            TSCGENERATOR_MEASURES_INSERTED,
            description="number of measures inserted (should be 0)"
        )

        adapter_metrics = {
            TSCGENERATOR_ERRORS: exception_counter,
            TSCGENERATOR_PROCESSED_WAL_FILE: processed_wal_file_counter,
            TSCGENERATOR_WAL_FILE_EMPTY: empty_wal_file_counter,
            TSCGENERATOR_DUPLICATE_WAL_FILE: duplicate_wal_file_counter,
            TSCGENERATOR_DEVICES_INSERTED: devices_inserted_counter,
            TSCGENERATOR_MEASURES_INSERTED: measures_inserted_counter,
        }

        _ADAPTER_METRICS = adapter_metrics
    return _ADAPTER_METRICS


def get_metric(name: str):
    """Get a metric object instance"""
    if not _ADAPTER_METRICS:
        _init_metrics(_meter)
        _print_metrics_configuration()
    return _ADAPTER_METRICS.get(name, None)


def _print_metrics_configuration():
    _LOGGER.info(f"Metrics Resource Lables: {_resource.attributes}")
    _LOGGER.info(f"Metrics EXPORT_INTERVAL(ms): {EXPORT_INTERVAL}")