# src/utilities/__init__.py

"""
Utilities Package

This package contains various utility functions and classes used across the project.

Purpose of __init__.py:
1. Package Marker: Identifies this directory as a Python package.
2. Package Initialization: Executes when the package is imported.
3. Defining Package-Level Attributes: Makes specified modules/objects available at package level.
4. Controlling Package Exports: Uses __all__ to specify what's imported with "from package import *".
5. Simplifying Imports: Provides convenient ways to import commonly used components.

Usage:
    from src.utilities import AsyncDatabaseUtilities, detect_dates, list_price

Benefits:
- Cleaner Imports: Users can import directly from the package.
- Encapsulation: Hides implementation details, exposing only necessary components.
- Flexibility: Allows changes to internal structure without affecting external usage.
- Documentation: Serves as a quick reference for package contents.

Note: While __init__.py is not strictly required in Python 3.3+, it's useful for
      clarity and functionality in package design.
"""

# from .db_utils import AsyncDatabaseUtilities
# from .date_utils import detect_dates, get_sorted_files, time_to_secs, array_tte
# from .other_utils import list_price, append_time, get_eastern_time
#
# __all__ = [
#     'AsyncDatabaseUtilities',
#     'detect_dates',
#     'get_sorted_files',
#     'time_to_secs',
#     'array_tte',
#     'list_price',
#     'append_time',
#     'get_eastern_time'
# ]

# Package-level initialization (if needed)
# print("Initializing utilities package")

# You could also add version information
__version__ = "1.0.0"