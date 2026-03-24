import pytest
from services.data.db_utils import (
    translate_run_key,
    translate_task_key,
    IdOverflowError,
    PG_INT4_MAX,
    PG_INT8_MAX,
)


class TestTranslateRunKey:

    def test_numeric_run_number_within_range(self):
        col, val = translate_run_key("12345")
        assert col == "run_number"
        assert val == "12345"

    def test_max_valid_run_number(self):
        col, val = translate_run_key(str(PG_INT4_MAX))
        assert col == "run_number"

    def test_overflow_run_number_raises(self):
        with pytest.raises(IdOverflowError):
            translate_run_key(str(PG_INT4_MAX + 1))

    def test_epoch_style_run_number_raises(self):
        # this is the actual failure case from issue #324 / #1922
        with pytest.raises(IdOverflowError):
            translate_run_key("1721143286111471")

    def test_string_run_id_passes(self):
        col, val = translate_run_key("abc-123")
        assert col == "run_id"
        assert val == "abc-123"

    def test_string_run_id_with_numbers_passes(self):
        # not purely numeric, so treated as run_id
        col, val = translate_run_key("run-42")
        assert col == "run_id"


class TestTranslateTaskKey:

    def test_numeric_task_id_within_range(self):
        col, val = translate_task_key("999")
        assert col == "task_id"
        assert val == "999"

    def test_max_valid_task_id(self):
        col, val = translate_task_key(str(PG_INT8_MAX))
        assert col == "task_id"

    def test_overflow_task_id_raises(self):
        with pytest.raises(IdOverflowError):
            translate_task_key(str(PG_INT8_MAX + 1))

    def test_string_task_name_passes(self):
        col, val = translate_task_key("my-task")
        assert col == "task_name"
