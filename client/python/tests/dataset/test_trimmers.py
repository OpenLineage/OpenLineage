# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from openlineage.client.dataset.trimmers import (
    DateTrimmer,
    KeyValueTrimmer,
    MultiDirDateTrimmer,
    YearMonthTrimmer,
)


class TestKeyValueTrimmer:
    trimmer = KeyValueTrimmer()

    def test_trim(self):
        assert self.trimmer.trim("/tmp/cat") == "/tmp/cat"

        assert self.trimmer.trim("/tmp/key=value") == "/tmp"
        assert self.trimmer.trim("tmp/key=value") == "tmp"

        assert self.trimmer.trim("/tmp/a=b=c") == "/tmp/a=b=c"

    def test_does_not_trim_whole_name(self):
        assert self.trimmer.trim("key=value") == "key=value"
        assert self.trimmer.trim("/key=value") == "/key=value"


class TestDateTrimmer:
    trimmer = DateTrimmer()

    def test_trim_basic_formats(self):
        assert self.trimmer.trim("/tmp/20250721") == "/tmp"
        assert self.trimmer.trim("tmp/20250721") == "tmp"
        assert self.trimmer.trim("/tmp/2025-07-22") == "/tmp"
        assert self.trimmer.trim("/tmp/22.07.2025") == "/tmp"

    def test_trim_with_noise(self):
        assert self.trimmer.trim("/tmp/20250722T0901Z") == "/tmp"
        assert self.trimmer.trim("/tmp/20250722T09:01Z") == "/tmp"
        assert self.trimmer.trim("/tmp/20250722T09.01Z") == "/tmp"
        assert self.trimmer.trim("/tmp/20250722T09-01Z") == "/tmp"

    def test_invalid_dates_not_trimmed(self):
        assert self.trimmer.trim("/tmp/20122321") == "/tmp/20122321"
        assert self.trimmer.trim("/tmp/2025-99-99") == "/tmp/2025-99-99"
        assert self.trimmer.trim("/tmp/01.13.2025") == "/tmp/01.13.2025"
        assert self.trimmer.trim("/tmp/00000000") == "/tmp/00000000"

    def test_dates_embedded_in_words_not_trimmed(self):
        assert self.trimmer.trim("/tmp/dataset20250721") == "/tmp/dataset20250721"
        assert self.trimmer.trim("/tmp/20250721_backup") == "/tmp/20250721_backup"
        assert self.trimmer.trim("/tmp/abc20250721def") == "/tmp/abc20250721def"

    def test_does_not_trim_whole_name(self):
        assert self.trimmer.trim("20250721") == "20250721"
        assert self.trimmer.trim("/20250721") == "/20250721"


class TestMultiDirDateTrimmer:
    trimmer = MultiDirDateTrimmer()

    def test_trim(self):
        assert self.trimmer.trim("/tmp/2025/07") == "/tmp"
        assert self.trimmer.trim("tmp/2025/07") == "tmp"
        assert self.trimmer.trim("/tmp/2025/07/21") == "/tmp"

    def test_invalid_dates_not_trimmed(self):
        assert self.trimmer.trim("/tmp/2025/14") == "/tmp/2025/14"
        assert self.trimmer.trim("/tmp/2025/07/50") == "/tmp/2025/07/50"

    def test_does_not_trim_whole_name(self):
        assert self.trimmer.trim("2025/07") == "2025/07"
        assert self.trimmer.trim("2025/07/21") == "2025/07/21"

        assert self.trimmer.trim("/2025/07") == "/2025/07"
        assert self.trimmer.trim("/2025/07/21") == "/2025/07/21"


class TestYearMonthTrimmer:
    trimmer = YearMonthTrimmer()

    def test_trim(self):
        assert self.trimmer.trim("/tmp/202507") == "/tmp"
        assert self.trimmer.trim("/tmp/2025-07") == "/tmp"
        assert self.trimmer.trim("tmp/2025-07") == "tmp"

    def test_invalid_dates_not_trimmed(self):
        assert self.trimmer.trim("/tmp/202540") == "/tmp/202540"
        assert self.trimmer.trim("/tmp/2025-33") == "/tmp/2025-33"

    def test_does_not_trim_whole_name(self):
        assert self.trimmer.trim("202507") == "202507"
        assert self.trimmer.trim("/202507") == "/202507"
        assert self.trimmer.trim("/2025-07") == "/2025-07"
