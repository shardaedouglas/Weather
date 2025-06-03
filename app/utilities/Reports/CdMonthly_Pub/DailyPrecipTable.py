
import sys
import os

# Get the absolute path to the parent directory
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)

# Now you can import from the parent directory
from HomrDB import ConnectDB, QueryDB, QuerySoM, DailyPrecipQuery


# print(f"{record:<20} {str(float(record) >= .01):<8} {str(float(record) >= .10):<8} {str(float(record) >= 1.00):<8}")

# Testing Multiline queries
# stations = QuerySoM("som")
# print(bool(stations))
# for station in stations[:20]:
#     print(station)

#Testing Daily Precip Query

stations = QueryDB(DailyPrecipQuery)
print(bool(stations))
for station in stations[-20:]:
    print(station)

