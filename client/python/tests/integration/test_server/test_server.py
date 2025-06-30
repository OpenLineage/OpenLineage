# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


class RunState(str, Enum):
    """OpenLineage run states."""

    START = "START"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    ABORT = "ABORT"
    FAIL = "FAIL"
    OTHER = "OTHER"


class BaseFacet(BaseModel):
    """Base class for OpenLineage facets."""

    model_config = ConfigDict(extra="allow")

    producer: Optional[str] = Field(alias="_producer", default=None)
    schemaURL: Optional[str] = Field(alias="_schemaURL", default=None)


class RunFacets(BaseModel):
    """Run facets container."""

    model_config = ConfigDict(extra="allow")


class JobFacets(BaseModel):
    """Job facets container."""

    model_config = ConfigDict(extra="allow")


class DatasetFacets(BaseModel):
    """Dataset facets container."""

    model_config = ConfigDict(extra="allow")


class Dataset(BaseModel):
    """OpenLineage Dataset model."""

    namespace: str = Field(..., min_length=1, description="Dataset namespace")
    name: str = Field(..., min_length=1, description="Dataset name")
    facets: Optional[DatasetFacets] = Field(default_factory=dict)

    @field_validator("namespace", "name")
    @classmethod
    def validate_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("cannot be empty or whitespace only")
        return v


class Run(BaseModel):
    """OpenLineage Run model."""

    runId: str = Field(..., min_length=1, description="Unique run identifier")
    facets: Optional[RunFacets] = Field(default_factory=dict)

    @field_validator("runId")
    @classmethod
    def validate_run_id(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("runId cannot be empty or whitespace only")
        return v


class Job(BaseModel):
    """OpenLineage Job model."""

    namespace: str = Field(..., min_length=1, description="Job namespace")
    name: str = Field(..., min_length=1, description="Job name")
    facets: Optional[JobFacets] = Field(default_factory=dict)

    @field_validator("namespace", "name")
    @classmethod
    def validate_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("cannot be empty or whitespace only")
        return v


class RunEvent(BaseModel):
    """OpenLineage RunEvent model."""

    eventTime: datetime = Field(..., description="Event timestamp in ISO 8601 format")
    eventType: RunState = Field(..., description="Type of run event")
    run: Run = Field(..., description="Run information")
    job: Job = Field(..., description="Job information")
    inputs: Optional[List[Dataset]] = Field(default_factory=list, description="Input datasets")
    outputs: Optional[List[Dataset]] = Field(default_factory=list, description="Output datasets")
    producer: str = Field(..., min_length=1, description="Producer identifier")
    schemaURL: Optional[str] = Field(default=None, description="Schema URL")

    @field_validator("producer")
    @classmethod
    def validate_producer(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("producer cannot be empty or whitespace only")
        return v


class DatasetEvent(BaseModel):
    """OpenLineage DatasetEvent model."""

    eventTime: datetime = Field(..., description="Event timestamp in ISO 8601 format")
    dataset: Dataset = Field(..., description="Dataset information")
    producer: str = Field(..., min_length=1, description="Producer identifier")
    schemaURL: Optional[str] = Field(default=None, description="Schema URL")

    @field_validator("producer")
    @classmethod
    def validate_producer(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("producer cannot be empty or whitespace only")
        return v


class JobEvent(BaseModel):
    """OpenLineage JobEvent model."""

    eventTime: datetime = Field(..., description="Event timestamp in ISO 8601 format")
    job: Job = Field(..., description="Job information")
    inputs: Optional[List[Dataset]] = Field(default_factory=list, description="Input datasets")
    outputs: Optional[List[Dataset]] = Field(default_factory=list, description="Output datasets")
    producer: str = Field(..., min_length=1, description="Producer identifier")
    schemaURL: Optional[str] = Field(default=None, description="Schema URL")

    @field_validator("producer")
    @classmethod
    def validate_producer(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("producer cannot be empty or whitespace only")
        return v


# Union type for any OpenLineage event
OpenLineageEvent = Union[RunEvent, DatasetEvent, JobEvent]


class EventRecord(BaseModel):
    """Record of a received OpenLineage event."""

    event_id: str
    run_id: Optional[str]
    event_type: Optional[str]
    job_name: Optional[str]
    job_namespace: Optional[str]
    payload: Dict[str, Any]
    headers: Dict[str, str]
    timestamp: float
    content_encoding: Optional[str] = None
    validation_errors: List[str] = []
    parsed_event: Optional[Dict[str, Any]] = None
    event_class: Optional[str] = None


class ServerStats(BaseModel):
    """Statistics about events received by the test server."""

    total_events: int
    events_by_type: Dict[str, int]
    events_by_run: Dict[str, int]
    successful_requests: int
    failed_requests: int
    validation_errors: int
    unique_runs: int
    start_time: float


class ErrorSimulation(BaseModel):
    """Configuration for error simulation."""

    enabled: bool = False
    delay_ms: int = Field(default=0, ge=0)
    # Deterministic sequence support
    response_sequence: Optional[List[int]] = Field(
        default=None, description="Deterministic sequence of HTTP status codes to return"
    )
    sequence_position: int = Field(default=0, description="Current position in the response sequence")


class OpenLineageTestServer:
    """FastAPI-based test server for OpenLineage HTTP transport testing."""

    def __init__(self):
        self.events: List[EventRecord] = []
        self.stats = ServerStats(
            total_events=0,
            events_by_type={},
            events_by_run={},
            successful_requests=0,
            failed_requests=0,
            validation_errors=0,
            unique_runs=0,
            start_time=time.time(),
        )
        self.error_simulation = ErrorSimulation()
        self._request_counter = 0
        self.log = logging.getLogger("OpenLineageTestServer")

    def reset(self):
        """Reset server state for a new test."""
        self.events.clear()
        self.stats = ServerStats(
            total_events=0,
            events_by_type={},
            events_by_run={},
            successful_requests=0,
            failed_requests=0,
            validation_errors=0,
            unique_runs=0,
            start_time=time.time(),
        )
        self.error_simulation = ErrorSimulation()
        self._request_counter = 0

    def _should_simulate_error(self) -> tuple[bool, Optional[int]]:
        """Determine if we should simulate an error for this request.

        Returns:
            tuple[bool, Optional[int]]: (should_error, status_code)
        """
        if not self.error_simulation.enabled:
            return False, None

        # Use deterministic sequence if available
        if self.error_simulation.response_sequence is not None:
            if self.error_simulation.sequence_position < len(self.error_simulation.response_sequence):
                status_code = self.error_simulation.response_sequence[self.error_simulation.sequence_position]
                self.error_simulation.sequence_position += 1

                # 200 means success, anything else is an error
                if status_code == 200:
                    return False, None
                else:
                    return True, status_code
            else:
                # Sequence exhausted, repeat the last status code
                if len(self.error_simulation.response_sequence) > 0:
                    last_status = self.error_simulation.response_sequence[-1]
                    if last_status == 200:
                        return False, None
                    else:
                        return True, last_status
                else:
                    return False, None

        # No sequence provided, default to success
        return False, None

    async def _apply_delay(self):
        """Apply configured delay."""
        if self.error_simulation.delay_ms > 0:
            await asyncio.sleep(self.error_simulation.delay_ms / 1000.0)

    def _detect_and_validate_event(self, payload: Dict[str, Any]):
        """Detect event type and validate using Pydantic models."""
        validation_errors = []
        parsed_event = None
        event_class = "Unknown"

        try:
            # Try to parse as RunEvent first (most common)
            if "run" in payload and "eventType" in payload:
                parsed_event = RunEvent.model_validate(payload)
                event_class = "RunEvent"
            # Try DatasetEvent
            elif "dataset" in payload and "run" not in payload:
                parsed_event = DatasetEvent.model_validate(payload)
                event_class = "DatasetEvent"
            # Try JobEvent
            elif "job" in payload and "run" not in payload and "dataset" not in payload:
                parsed_event = JobEvent.model_validate(payload)
                event_class = "JobEvent"
            else:
                validation_errors.append("Could not determine event type - missing required fields")

        except ValidationError as e:
            validation_errors.extend(
                [f"{'.'.join(map(str, err['loc']))}: {err['msg']}" for err in e.errors()]
            )
        except Exception as e:
            validation_errors.append(f"Unexpected validation error: {str(e)}")

        return parsed_event, validation_errors, event_class

    def _extract_event_info(self, payload: Dict[str, Any], parsed_event: Optional[OpenLineageEvent]):
        """Extract run_id, event_type, job_name, job_namespace from event payload."""
        run_id = None
        event_type = None
        job_name = None
        job_namespace = None

        if parsed_event:
            # Use parsed event for reliable extraction
            if isinstance(parsed_event, RunEvent):
                run_id = parsed_event.run.runId
                event_type = parsed_event.eventType.value
                job_name = parsed_event.job.name
                job_namespace = parsed_event.job.namespace
            elif isinstance(parsed_event, (DatasetEvent, JobEvent)):
                if hasattr(parsed_event, "job"):
                    job_name = parsed_event.job.name
                    job_namespace = parsed_event.job.namespace
        else:
            # Fallback to manual extraction for invalid events
            if "run" in payload and isinstance(payload["run"], dict):
                run_id = payload["run"].get("runId")

            if "eventType" in payload:
                event_type = payload["eventType"]

            if "job" in payload and isinstance(payload["job"], dict):
                job_name = payload["job"].get("name")
                job_namespace = payload["job"].get("namespace")

        return run_id, event_type, job_name, job_namespace

    def _update_stats(self, run_id: Optional[str], event_type: Optional[str], has_validation_errors: bool):
        """Update server statistics."""
        self.stats.total_events += 1

        if has_validation_errors:
            self.stats.validation_errors += 1

        if event_type:
            self.stats.events_by_type[event_type] = self.stats.events_by_type.get(event_type, 0) + 1

        if run_id:
            self.stats.events_by_run[run_id] = self.stats.events_by_run.get(run_id, 0) + 1

        self.stats.unique_runs = len(self.stats.events_by_run)

    async def receive_lineage_event(self, request: Request) -> Dict[str, Any]:
        """Handle incoming OpenLineage events."""
        self._request_counter += 1

        # Apply delay if configured
        await self._apply_delay()

        # Check if we should simulate an error
        should_error, status_code = self._should_simulate_error()
        if should_error:
            self.stats.failed_requests += 1

            if status_code == 0:  # Special code for timeout
                # Simulate timeout by sleeping longer than typical client timeout
                await asyncio.sleep(10)
                return {"status": "timeout"}
            elif status_code == 503 and "connection_error" in str(status_code):
                # Simulate connection error (in real scenario, this would be network-level)
                raise HTTPException(status_code=503, detail="Connection error simulation")
            else:
                # Simulate HTTP error with specific status code
                raise HTTPException(status_code=status_code, detail=f"Simulated {status_code} error")

        try:
            # Read request body
            body = await request.body()

            # Parse JSON payload
            if request.headers.get("content-encoding") == "gzip":
                import gzip

                body = gzip.decompress(body)

            payload = json.loads(body.decode("utf-8"))

            # Validate the event using Pydantic
            parsed_event, validation_errors, event_class = self._detect_and_validate_event(payload)

            # Extract event information
            run_id, event_type, job_name, job_namespace = self._extract_event_info(payload, parsed_event)

            # Generate event ID (similar to how the client does it)
            event_id = f"server-{self._request_counter}"
            if run_id and event_type:
                event_id = f"{run_id}-{event_type}"

            # Store event record
            event_record = EventRecord(
                event_id=event_id,
                run_id=run_id,
                event_type=event_type,
                job_name=job_name,
                job_namespace=job_namespace,
                payload=payload,
                headers=dict(request.headers),
                timestamp=time.time(),
                content_encoding=request.headers.get("content-encoding"),
                validation_errors=validation_errors,
                parsed_event=parsed_event.model_dump() if parsed_event else None,
                event_class=event_class,
            )

            self.events.append(event_record)
            self._update_stats(run_id, event_type, len(validation_errors) > 0)
            self.stats.successful_requests += 1

            response = {"status": "received", "event_id": event_id, "event_class": event_class}

            # Include validation errors if any
            if validation_errors:
                response["validation_errors"] = validation_errors
                response["status"] = "received_with_validation_errors"

            return response

        except json.JSONDecodeError:
            self.stats.failed_requests += 1
            raise HTTPException(status_code=400, detail="Invalid JSON payload")
        except Exception as e:
            self.stats.failed_requests += 1
            raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")


# Global server instance
test_server = OpenLineageTestServer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    print("OpenLineage HTTP Transport Test Server starting...")
    yield
    # Shutdown
    print("OpenLineage HTTP Transport Test Server shutting down...")


# Create FastAPI app
app = FastAPI(
    title="OpenLineage HTTP Transport Test Server",
    description="Test server for validating OpenLineage "
    "HTTP transport functionality with Pydantic V2 validation",
    version="1.0.0",
    lifespan=lifespan,
)


@app.post("/api/v1/lineage")
async def receive_event(request: Request) -> Dict[str, Any]:
    """Receive OpenLineage events - main endpoint."""
    return await test_server.receive_lineage_event(request)


@app.get("/events")
async def get_events(
    run_id: Optional[str] = None,
    event_type: Optional[str] = None,
    job_name: Optional[str] = None,
    has_validation_errors: Optional[bool] = None,
    event_class: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[EventRecord]:
    """Get received events with optional filtering."""
    events = test_server.events

    # Apply filters
    if run_id:
        events = [e for e in events if e.run_id == run_id]

    if event_type:
        events = [e for e in events if e.event_type == event_type]

    if job_name:
        events = [e for e in events if e.job_name == job_name]

    if event_class:
        events = [e for e in events if e.event_class == event_class]

    if has_validation_errors is not None:
        if has_validation_errors:
            events = [e for e in events if len(e.validation_errors) > 0]
        else:
            events = [e for e in events if len(e.validation_errors) == 0]

    # Apply limit
    if limit:
        events = events[:limit]

    return events


@app.get("/events/{run_id}")
async def get_events_for_run(run_id: str) -> List[EventRecord]:
    """Get all events for a specific run ID."""
    return [e for e in test_server.events if e.run_id == run_id]


@app.get("/events/{run_id}/sequence")
async def get_run_event_sequence(run_id: str) -> Dict[str, Any]:
    """Get event sequence for a run and validate ordering."""
    events = [e for e in test_server.events if e.run_id == run_id]
    events.sort(key=lambda x: x.timestamp)

    sequence = []
    has_start = False
    completion_types = ["COMPLETE", "FAIL", "ABORT"]

    for event in events:
        sequence.append(
            {
                "event_type": event.event_type,
                "timestamp": event.timestamp,
                "event_id": event.event_id,
                "validation_errors": event.validation_errors,
            }
        )

        if event.event_type == "START":
            has_start = True

    # Check if there are completion events before START
    invalid_ordering = []
    for i, event in enumerate(events):
        if event.event_type in completion_types and not has_start:
            # Check if there's a START event later in the sequence
            start_later = any(e.event_type == "START" for e in events[i + 1 :])
            if start_later:
                invalid_ordering.append(f"Completion event {event.event_type} received before START")

    return {
        "run_id": run_id,
        "sequence": sequence,
        "has_start_event": has_start,
        "ordering_issues": invalid_ordering,
        "total_events": len(events),
    }


@app.get("/validation/summary")
async def get_validation_summary() -> Dict[str, Any]:
    """Get summary of validation results."""
    total_events = len(test_server.events)
    events_with_errors = [e for e in test_server.events if len(e.validation_errors) > 0]

    error_types = {}
    for event in events_with_errors:
        for error in event.validation_errors:
            error_types[error] = error_types.get(error, 0) + 1

    return {
        "total_events": total_events,
        "valid_events": total_events - len(events_with_errors),
        "invalid_events": len(events_with_errors),
        "validation_error_rate": len(events_with_errors) / total_events if total_events > 0 else 0,
        "error_types": error_types,
        "events_by_class": {
            event_class: len([e for e in test_server.events if e.event_class == event_class])
            for event_class in set(e.event_class for e in test_server.events)
        },
    }


@app.get("/stats")
async def get_stats() -> ServerStats:
    """Get server statistics."""
    return test_server.stats


class ErrorSequenceConfig(BaseModel):
    """Configuration for error sequence simulation."""

    enabled: bool = True
    response_sequence: Optional[List[int]] = None
    delay_ms: int = Field(default=0, ge=0)


@app.post("/simulate/sequence")
async def configure_error_sequence(config: ErrorSequenceConfig) -> Dict[str, Any]:
    """Configure deterministic error sequence simulation."""
    try:
        test_server.error_simulation = ErrorSimulation(
            enabled=config.enabled,
            response_sequence=config.response_sequence,
            sequence_position=0,  # Reset position
            delay_ms=config.delay_ms,
        )
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid configuration: {e}")

    return {
        "status": "configured",
        "enabled": config.enabled,
        "response_sequence": config.response_sequence,
        "delay_ms": config.delay_ms,
    }


@app.post("/reset")
async def reset_server() -> Dict[str, str]:
    """Reset server state."""
    test_server.reset()
    return {"status": "reset"}


@app.get("/health")
async def health_check() -> Dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy", "uptime": str(time.time() - test_server.stats.start_time)}


@app.get("/")
async def root() -> Dict[str, str]:
    """Root endpoint with basic info."""
    return {
        "service": "OpenLineage HTTP Transport Test Server",
        "version": "1.0.0",
        "status": "running",
        "validation": "Pydantic V2 OpenLineage event validation",
        "endpoints": {
            "lineage": "/api/v1/lineage",
            "events": "/events",
            "validation": "/validation/summary",
            "stats": "/stats",
            "health": "/health",
        },
    }


if __name__ == "__main__":
    uvicorn.run("test_server:app", host="0.0.0.0", port=8080, log_level="info", access_log=True)
