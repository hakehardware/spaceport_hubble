from datetime import datetime, timedelta
import re

class Utils:
    @staticmethod
    def normalize_date(date_str):
        # Truncate the fractional seconds to 6 digits
        truncated_date_str = date_str[:26] + 'Z'
        # Parse the input date string
        dt = datetime.strptime(truncated_date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        # Return the formatted date string
        return dt.strftime('%Y-%m-%d %H:%M:%S')