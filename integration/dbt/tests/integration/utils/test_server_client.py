# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import time
from typing import Any, Dict, List, Optional

import requests


class TestServerClient:
    """Client for interacting with the OpenLineage test server."""

    def __init__(self, base_url: str = "http://localhost:8080"):
        self.base_url = base_url
        self.session = requests.Session()

    def health_check(self) -> bool:
        """Check if the test server is healthy."""
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=5)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def wait_for_server(self, timeout: int = 30) -> bool:
        """Wait for the server to be ready."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.health_check():
                return True
            time.sleep(1)
        return False

    def clear_events(self) -> bool:
        """Clear all events from the test server."""
        try:
            response = self.session.post(f"{self.base_url}/clear")
            return response.status_code == 200
        except requests.RequestException:
            return False

    def get_events(self) -> List[Dict[str, Any]]:
        """Get all events from the test server."""
        try:
            response = self.session.get(f"{self.base_url}/events")
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            return []

    def get_events_count(self) -> int:
        """Get the number of events stored on the server."""
        events = self.get_events()
        return len(events)

    def wait_for_events(self, expected_count: int, timeout: int = 30) -> bool:
        """Wait for a specific number of events to be received."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.get_events_count() >= expected_count:
                return True
            time.sleep(1)
        return False

    def get_events_by_job(self, job_name: str) -> List[Dict[str, Any]]:
        """Get events for a specific job."""
        events = self.get_events()
        return [event for event in events if event.get("job", {}).get("name") == job_name]

    def get_latest_event(self) -> Optional[Dict[str, Any]]:
        """Get the most recent event."""
        events = self.get_events()
        if events:
            return events[-1]
        return None
