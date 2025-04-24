# tests/test_cleaning.py
import sys
import os
import pytest
from decimal import Decimal

# Ensure the dags/utils directory is in the Python path for testing
# Adjust path as necessary based on your project structure
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')))

# Now import the functions
from utils.cleaning import get_decade, clean_budget, parse_budget_value

# --- Tests for get_decade ---

@pytest.mark.parametrize("input_year, expected_decade", [
    (1997, 1990),
    ("1983", 1980),
    (2000, 2000),
    (2014, 2010),
    (1927, 1920),
    ("1955 (released)", 1950), # String with extra info
    (1970.0, 1970), # Float year
    (None, None),
    ("", None),
    ("abc", None),
    ("123", None), # Not 4 digits
    (1700, None), # Outside plausible range
    (2200, None), # Outside plausible range
])
def test_get_decade(input_year, expected_decade):
    assert get_decade(input_year) == expected_decade

# --- Tests for parse_budget_value ---

@pytest.mark.parametrize("budget_str, expected_decimal", [
    ("10M", Decimal("10000000")),
    ("1.5 million", Decimal("1500000")),
    ("500k", Decimal("500000")),
    ("1,200,000", Decimal("1200000")),
    ("1.2", Decimal("1.2")), # Assume raw value if no suffix
    (" $ 2 M ", Decimal("2000000")), # Extra spaces and symbol ignored here
    ("abc", None),
    ("", None),
    (None, None),
     ("1.2.3", Decimal("1.23")) # Handle multiple decimals (keeps first dot) - adjust if needed
])
def test_parse_budget_value(budget_str, expected_decimal):
    assert parse_budget_value(budget_str) == expected_decimal


# --- Tests for clean_budget ---
# Note: These tests rely on the fixed EXCHANGE_RATES in cleaning.py

@pytest.mark.parametrize("budget_input, expected_usd_int", [
    # Basic USD
    ("$1,000,000", 1000000),
    (" $ 2.5 M ", 2500000),
    ("100,000 USD", 100000),
    (1500000, 1500000), # Integer input

    # Other Currencies (using fixed rates in cleaning.py)
    ("£1M", 1250000),       # 1M GBP * 1.25
    ("€ 2 million", 2200000), # 2M EUR * 1.10
    ("CAD 500k", 375000),    # 500k CAD * 0.75
    ("1000000 FRF", 170000), # 1M FRF * 0.17

    # Ranges (takes lower bound)
    ("$10M - $12M", 10000000),
    ("£500k-£700k", 625000),    # 500k GBP * 1.25
    ("EUR 1 million to 1.5 million", 1100000), # 1M EUR * 1.10

    # No currency specified (assume USD)
    ("5,000,000", 5000000),
    ("10 M", 10000000),

    # Null/NaN/Invalid cases
    (None, 0),
    (float('nan'), 0),
    ("", 0),
    ("N/A", 0),
    ("Unknown", 0),
    ("Approx. $5M", 5000000), # Should handle "Approx." if value is parsable
    ("$1-2M", 1000000), # Range variation

    # Edge cases
    ("$1", 1),
    ("€1", 1), # 1 EUR * 1.10 = 1.1 -> rounded to 1 (or adjust rounding)
])
def test_clean_budget(budget_input, expected_usd_int):
    assert clean_budget(budget_input) == expected_usd_int