"""
Tests for utils.py functions.
"""
import pytest
import tempfile
import os
import json
from app.utils import (
    try_decode_base64,
    find_base64_strings,
    sum_values_in_text,
    extract_json_from_text,
    parse_csv_and_sum,
    extract_text_from_pdf
)


def test_try_decode_base64_valid():
    """Test base64 decoding with valid input."""
    test_text = "Hello, World!"
    encoded = __import__("base64").b64encode(test_text.encode()).decode()
    decoded = try_decode_base64(encoded)
    assert decoded == test_text


def test_try_decode_base64_invalid():
    """Test base64 decoding with invalid input."""
    decoded = try_decode_base64("not-valid-base64!!!")
    assert decoded is None


def test_find_base64_strings():
    """Test finding base64 strings in text."""
    text = "Some text with base64: SGVsbG8gV29ybGQ= and another: dGVzdA== and short: abc"
    results = find_base64_strings(text, min_length=10)
    assert len(results) >= 1


def test_sum_values_in_text():
    """Test summing values from text."""
    text = "The values are: 10, 20, 30, and 40"
    result = sum_values_in_text(text)
    assert result == 100.0


def test_sum_values_in_text_with_column():
    """Test summing values with column hint."""
    text = 'value: 10, value: 20, value: 30'
    result = sum_values_in_text(text, column_hint="value")
    assert result == 60.0


def test_extract_json_from_text():
    """Test JSON extraction from text."""
    text = 'Some text {"key": "value", "number": 42} more text'
    result = extract_json_from_text(text)
    assert result is not None
    assert result["key"] == "value"
    assert result["number"] == 42


def test_parse_csv_and_sum():
    """Test CSV parsing and summing."""
    csv_content = "name,value\nitem1,10\nitem2,20\nitem3,30"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(csv_content)
        temp_path = f.name
    
    try:
        result = parse_csv_and_sum(temp_path, column_hint="value")
        assert result == 60.0
    finally:
        os.unlink(temp_path)

