# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import re
from abc import ABC, abstractmethod
from datetime import datetime


def _valid_date(text: str, fmt: str) -> bool:
    try:
        datetime.strptime(text, fmt)
        return True
    except ValueError:
        return False


class DatasetNameTrimmer(ABC):
    """
    DatasetNameTrimmer interface for dataset name trimmers in OpenLineage events.

    A trimmer receives a dataset name (string) and returns
    the trimmed version of that name.

    Implementations MUST satisfy the following two requirements:

    1. **Non-lengthening**: ``trim(name)`` must never return a string longer
       than ``name``.  If violated, the dataset is left completely untrimmed —
       the original name is kept and no further trimmers are applied.

    2. **Convergence**: repeated application must eventually stabilize into a
       fixed point.  A trimmer may require multiple passes to fully trim a name
       (e.g. stripping one ``key=value`` segment at a time is fine), but the
       process must not loop indefinitely.  For example, a trimmer that toggles
       a name between ``/data/table/a`` and ``/data/table/b`` on successive
       calls would never converge.  If convergence is not reached, the reducer
       falls back to the original name.
    """

    SEPARATOR = "/"

    @abstractmethod
    def trim(self, name: str) -> str: ...

    def _get_last_part(self, name: str) -> str:
        if not name or self.SEPARATOR not in name:
            return name

        name_no_trailing = name.rstrip(self.SEPARATOR)

        return name_no_trailing.rsplit(self.SEPARATOR, 1)[-1]

    def _has_multiple_directories(self, name: str) -> bool:
        if not name or self.SEPARATOR not in name:
            return False

        parts = [p for p in name.split(self.SEPARATOR) if p]

        return len(parts) > 1

    def _remove_last_part(self, name: str, parts_to_remove: int = 1) -> str:
        stripped = name.rstrip(self.SEPARATOR)

        if self.SEPARATOR not in stripped:
            return name

        result = stripped

        for _ in range(parts_to_remove):
            idx = result.rfind(self.SEPARATOR)

            if idx == -1:
                break

            result = result[:idx]

        return result


class KeyValueTrimmer(DatasetNameTrimmer):
    """
    Removes the last segment of a path if it is a `key=value` pair.

    Example:
        "/data/table/day=2024-01-01" -> "/data/table"
    """

    EQUALITY_SIGN = "="

    def trim(self, name: str) -> str:
        if not self._has_multiple_directories(name):
            return name

        last = self._get_last_part(name)

        if last.count(self.EQUALITY_SIGN) != 1:
            return name

        return self._remove_last_part(name)


class DateTrimmer(DatasetNameTrimmer):
    """
    Removes the last segment of a path representing a date-like value. If the last
    segment is recognized as a valid date (possibly with 'T'/'Z' timestamp fragments
    or surrounding noise), it is removed; otherwise the original path is returned.

    Heuristics:
        - Segment has to match one of the supported date formats:
          yyyy-MM-dd, dd.MM.yyyy, yyyyMMdd.
        - Removes the candidate date from the segment, then strips characters like
          'T', 'Z', whitespace, ':', '.', '-' and all digits. If nothing meaningful
          remains, the segment is considered date-like.

    Examples:
        "/tmp/20250721"       -> "/tmp"
        "/tmp/20250722T0901Z" -> "/tmp"
        "/tmp/2025-07-22"     -> "/tmp"
        "/20250721"           -> "/20250721"
    """

    def trim(self, name: str) -> str:
        if not self._has_multiple_directories(name):
            return name

        segment = self._get_last_part(name)

        if self._looks_like_date(segment):
            return self._remove_last_part(name)
        return name

    @staticmethod
    def _looks_like_date(segment: str) -> bool:
        """Check whether *segment* is essentially a date, possibly with timestamp noise.

        Strategy: try every known date format against the segment.  When a
        valid date is found, remove it and then strip characters that commonly
        appear around embedded dates in path segments (e.g. ``T``, ``Z``,
        digits for time components, ``:``, ``.``, ``-``, whitespace).  If
        nothing meaningful remains the whole segment is considered date-like.

        This lets us recognize segments such as ``20250722T0901Z``,
        ``2025-07-22T09:01:00.000Z``, or bare ``2025-07-22``.
        """

        date_patterns = [
            ("%Y-%m-%d", re.compile(r"\d{4}-\d{2}-\d{2}")),
            ("%d.%m.%Y", re.compile(r"\d{2}\.\d{2}\.\d{4}")),
            ("%Y%m%d", re.compile(r"\d{8}")),
        ]

        # Characters that are expected "noise" surrounding an embedded date:
        # time-separator 'T', UTC marker 'Z', digits (hours/minutes/seconds),
        # and the punctuation ':', '.', '-' used inside timestamps.
        noise_regex = re.compile(r"[TZ\s:.\-\d]*")

        for fmt, pattern in date_patterns:
            for match in pattern.findall(segment):
                if _valid_date(match, fmt):
                    leftover = segment.replace(match, "", 1)
                    leftover = noise_regex.sub("", leftover)

                    if leftover == "":
                        return True
        return False


class MultiDirDateTrimmer(DatasetNameTrimmer):
    """
    Removes trailing date directories in yyyy/MM or yyyy/MM/dd formats.
    """

    def trim(self, name: str) -> str:
        parts = name.strip(self.SEPARATOR).split(self.SEPARATOR)

        if len(parts) == 3:
            y, m = parts[-2:]
            if _valid_date(f"{y}/{m}", "%Y/%m"):
                return self._remove_last_part(name, parts_to_remove=2)

        if len(parts) >= 4:
            y, m, d = parts[-3:]
            if _valid_date(f"{y}/{m}/{d}", "%Y/%m/%d"):
                return self._remove_last_part(name, parts_to_remove=3)

            y, m = parts[-2:]
            if _valid_date(f"{y}/{m}", "%Y/%m"):
                return self._remove_last_part(name, parts_to_remove=2)

        return name


class YearMonthTrimmer(DatasetNameTrimmer):
    """
    Removes the last segment of a path if it contains a year‑month
    pattern (e.g., yyyyMM or yyyy-MM).
    """

    DATE_PATTERNS = [
        (re.compile(r"^\d{4}\d{2}$"), "%Y%m"),
        (re.compile(r"^\d{4}-\d{2}$"), "%Y-%m"),
    ]

    def trim(self, name: str) -> str:
        if not self._has_multiple_directories(name):
            return name

        last = self._get_last_part(name)
        for pattern, fmt in self.DATE_PATTERNS:
            if pattern.match(last) and _valid_date(last, fmt):
                return self._remove_last_part(name)

        return name
