from datetime import datetime

def format_expiry_date(date_str: str) -> str:
    """
    Converts date string from DD-MMM-YY format to DDMMMYY format.
    Example: '17-JUL-25' -> '17JUL25'
    """
    try:
        # Parse the date from the input format
        date_obj = datetime.strptime(date_str, '%d-%b-%y')
        # Format the date into the desired output format and make it uppercase
        return date_obj.strftime('%d%b%y').upper()
    except ValueError:
        # Handle cases where the date format might be incorrect
        # For now, we can log this or return the original string
        # depending on desired error handling.
        # Returning original string for now.
        return date_str
