# dags/utils/cleaning.py
import re
import math
import logging
from decimal import Decimal, InvalidOperation

# --- Constants ---
# Placeholder exchange rates - Replace with actual historical rates or API if needed
# These are highly simplified assumptions.
EXCHANGE_RATES = {
    'USD': 1.0,
    '$': 1.0,
    'CAD': 0.75,  # Example: 1 CAD = 0.75 USD
    'GBP': 1.25,  # Example: 1 GBP = 1.25 USD
    '£': 1.25,
    'EUR': 1.10,  # Example: 1 EUR = 1.10 USD
    '€': 1.10,
    'FRF': 0.17,  # French Franc example (approx pre-Euro)
    'DEM': 0.56,  # German Mark example (approx pre-Euro)
    # Add other currencies encountered in the data
}

# --- Year Cleaning ---

def get_decade(year_input: any) -> int | None:
    """
    Cleans the year input and returns the starting year of its decade.
    Example: 1997 -> 1990, "1983" -> 1980.
    Returns None if year cannot be reliably determined.
    """
    if year_input is None or math.isnan(year_input):
        return None
    try:
        # Handle cases like "1999 (details)" or float years like 1999.0
        #year_str = str(year_input)
        #match = re.search(r'\b(\d{4})\b', year_str) # Find the first 4-digit number
        #words = year_str.split() # Split the string by whitespace
        #year = None
        #for word in words:
            # Check if the word consists only of digits AND has length 4
         #   if word.isdigit() and len(word) == 4:
          #      year = word 

        if year_input != None:
            #year = int(match.group(1))
            if 1800 < year_input < 2100: # Basic sanity check for movie years
                return (year_input // 10) * 10
            else:
                logging.warning(f"Year {year_input} seems out of plausible range, treating as invalid.")
                return None
        else:
             logging.warning(f"Could not parse a 4-digit year from input: {year_input}")
             return None
    except (ValueError, TypeError) as e:
        logging.warning(f"Error converting year input '{year_input}' to decade: {e}")
        return None

# --- Budget Cleaning (Bonus) ---

def parse_budget_value(budget_str: str) -> Decimal | None:
    """
    Parses a budget string component (like '10M', '500,000', '1.5') into a Decimal.
    Handles 'million', 'M', 'K'. Returns None if parsing fails.
    """
    if not isinstance(budget_str, str):
        return None

    budget_str = budget_str.strip().lower()
    multiplier = Decimal(1)

    if 'million' in budget_str or budget_str.endswith('m'):
        multiplier = Decimal(1_000_000)
        budget_str = budget_str.replace('million', '').replace('m', '')
    elif budget_str.endswith('k'):
        multiplier = Decimal(1_000)
        budget_str = budget_str.replace('k', '')

    # Remove non-numeric characters except decimal point
    budget_str = re.sub(r'[^\d.]', '', budget_str)

    # Handle cases with multiple decimal points by keeping only the first part
    if budget_str.count('.') > 1:
        parts = budget_str.split('.')
        budget_str = parts[0] + '.' + ''.join(parts[1:])
        # Or be more conservative: budget_str = parts[0] if len(parts) > 1 else budget_str

    if not budget_str:
        return None

    try:
        value = Decimal(budget_str)
        return value * multiplier
    except InvalidOperation:
        logging.warning(f"Could not convert budget string part '{budget_str}' to Decimal.")
        return None

def clean_budget(budget_input: any) -> int:
    """
    Cleans the budget string, converts to USD integer, handling ranges and NaNs.
    - Converts NaN/None/empty to 0.
    - Handles ranges (e.g., "$10M - $12M") by taking the lower value.
    - Identifies currency symbols or codes.
    - Converts known non-USD currencies to USD using EXCHANGE_RATES.
    - Returns the final budget as an integer. Defaults to 0 if parsing fails.

    Assumptions:
    - EXCHANGE_RATES provides reasonable *placeholder* conversions.
    - Currency symbol/code appears at the beginning or end of the string.
    - If no currency is found, assumes USD.
    """
    if budget_input is None or (isinstance(budget_input, float) and math.isnan(budget_input)):
        return 0
    if isinstance(budget_input, (int, float)): # Already numeric
        return int(budget_input) # Assume it's USD if just a number? Risky, but maybe necessary. Let's default to parsing as string.

    budget_str = str(budget_input).strip()
    if not budget_str or budget_str.lower() == 'n/a':
        return 0

    # 1. Handle Range: Use the lowest value
    # More robust range detection: handles separators like '-', 'to', '–'
    range_match = re.search(r'([$€£]?\s*\d[\d,.]*[mk]?)\s*[-–to]+\s*([$€£]?\s*\d[\d,.]*[mk]?)', budget_str, re.IGNORECASE)
    if range_match:
        budget_str = range_match.group(1).strip() # Use the first part (lower bound)
        logging.info(f"Detected range in '{budget_input}', using lower bound: '{budget_str}'")

    # 2. Detect Currency and Extract Value String
    currency = 'USD' # Default
    value_str = budget_str
    rate = Decimal(1.0)

    # Check for known symbols/codes
    found_currency = False
    for code, r in EXCHANGE_RATES.items():
        # Check prefix: "$10M", "EUR 10M"
        if budget_str.startswith(code):
            prefix_len = len(code)
            # Check if followed by space or digit to avoid partial matches (e.g., 'US' in 'US$')
            if len(budget_str) > prefix_len and (budget_str[prefix_len].isspace() or budget_str[prefix_len].isdigit()):
                 value_str = budget_str[prefix_len:].strip()
                 currency = code if code.isalpha() and len(code) == 3 else [c for c, sym in EXCHANGE_RATES.items() if sym == r and len(c)==3][0] # Map symbol back to code if possible
                 rate = Decimal(str(r))
                 found_currency = True
                 break
        # Check suffix: "10M USD", "10M€"
        if budget_str.endswith(code):
             suffix_len = len(code)
             # Check if preceded by space or digit
             if len(budget_str) > suffix_len and (budget_str[-suffix_len-1].isspace() or budget_str[-suffix_len-1].isdigit()):
                 value_str = budget_str[:-suffix_len].strip()
                 currency = code if code.isalpha() and len(code) == 3 else [c for c, sym in EXCHANGE_RATES.items() if sym == r and len(c)==3][0] # Map symbol back to code if possible
                 rate = Decimal(str(r))
                 found_currency = True
                 break

    # Fallback for symbols potentially missed if stuck to numbers (e.g. $1000)
    if not found_currency:
        if value_str.startswith('$'):
            value_str = value_str[1:]
            currency = 'USD'
            rate = Decimal(EXCHANGE_RATES['USD'])
        elif value_str.startswith('£'):
            value_str = value_str[1:]
            currency = 'GBP'
            rate = Decimal(EXCHANGE_RATES['GBP'])
        elif value_str.startswith('€'):
            value_str = value_str[1:]
            currency = 'EUR'
            rate = Decimal(EXCHANGE_RATES['EUR'])
        # Add other common symbols if needed

    # 3. Parse the numeric value
    numeric_value = parse_budget_value(value_str)

    if numeric_value is None:
        logging.warning(f"Could not parse numeric value from budget string: '{budget_input}' (processed as '{value_str}')")
        return 0

    # 4. Convert to USD if necessary
    budget_usd = numeric_value * rate

    # 5. Convert to Integer
    return int(budget_usd.to_integral_value(rounding='ROUND_HALF_UP')) # Or ROUND_FLOOR