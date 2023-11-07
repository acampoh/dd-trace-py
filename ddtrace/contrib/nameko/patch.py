import os
from ddtrace import config
from ddtrace.internal.schema import schematize_service_name
from ddtrace.pin import Pin
from ddtrace.utils.wrappers import unwrap as _u
from ddtrace.vendor.wrapt import wrap_function_wrapper as _w
from ddtrace.tracer import get_call_context, trace

import nameko

config._add(
    "nameko",
    {
        "distributed_tracing": True,
        "service_name": os.getenv("DD_NAMEKO_SERVICE_NAME"),
    },
)


def get_version():
    return nameko.__version__


def traced_run(self, *args, **kwargs):
    # Set service info for Nameko spans
    pin = Pin(service=self.service_name, app="nameko")
    pin.onto(get_call_context())

    # Patch ServiceRunner.run() to trace Nameko services
    original_run = nameko.main.ServiceRunner.run

    # Start a new trace for the Nameko service run
    with trace("nameko.run"):
        return original_run(self, *args, **kwargs)


def patch():
    # Check if already patched
    if getattr(nameko.main.ServiceRunner, "_datadog_patched", False):
        return

    nameko.main.ServiceRunner.run = traced_run
    # Mark as patched
    setattr(nameko.main.ServiceRunner, "_datadog_patched", True)


def unpatch():
    if not getattr(nameko, "_datadog_patch", False):
        return

    setattr(nameko, "_datadog_patch", False)

    _u(nameko.applications.FastAPI, "__init__")
