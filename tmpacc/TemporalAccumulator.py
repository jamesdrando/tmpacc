import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal, List, Any, Iterator, Generic, TypeVar
import re

# Temporal base units
type Milliseconds = "Milliseconds"
type Seconds = "Seconds"
type Minutes = "Minutes"
type Hours = "Hours"
type Days = "Days"
type Weeks = "Weeks"
type Months = "Months"
type Years = "Years"

# Consolidated temporal base units
type TemporalBaseUnits = Milliseconds | Seconds | Minutes | Hours | Days | Weeks | Months | Years

@dataclass
class Interval:
    unit: TemporalBaseUnits
    scalar: int

    def next(self, from_date: datetime) -> datetime:
        if self.unit == Milliseconds:
            return from_date + timedelta(milliseconds=self.scalar)
        elif self.unit == Seconds:
            return from_date + timedelta(seconds=self.scalar)
        elif self.unit == Minutes:
            return from_date + timedelta(minutes=self.scalar)
        elif self.unit == Hours:
            return from_date + timedelta(hours=self.scalar)
        elif self.unit == Days:
            return from_date + timedelta(days=self.scalar)
        elif self.unit == Weeks:
            return from_date + timedelta(weeks=self.scalar)
        elif self.unit == Months:
            month = from_date.month - 1 + self.scalar
            year = from_date.year + month // 12
            month = month % 12 + 1
            day = min(from_date.day, [31,
                                      29 if year % 4 == 0 and not year % 100 == 0 or year % 400 == 0 else 28,
                                      31, 30, 31, 30,
                                      31, 31, 30, 31,
                                      30, 31][month - 1])
            return from_date.replace(year=year, month=month, day=day)
        elif self.unit == Years:
            try:
                return from_date.replace(year=from_date.year + self.scalar)
            except ValueError:
                # Handle February 29 for leap years
                return from_date.replace(month=2, day=28, year=from_date.year + self.scalar)
        else:
            raise ValueError(f"Unsupported temporal unit: {self.unit}")

# Type affinities
type Categorical = "Categorical"
type Numerical = "Numerical"
type Temporal = "Temporal"

# Aggregations
type CumulativeSum = "CumulativeSum"
type MovingAverage = "MovingAverage"
type LastValue = "LastValue"
type MaxValue = "MaxValue"
type MinValue = "MinValue"
type Identity = "Identity"

# Conolidated types
type Affinity = Categorical | Numerical | Temporal
type Aggregation = CumulativeSum | MovingAverage | LastValue | MaxValue | MinValue

# T = TypeVar('T', Categorical, Numerical, Temporal)

class DataSeries:
    def __init__(self, series: List[Any], affinity: Affinity, aggregation: Aggregation = Identity):
        self.series: List[Any] = series
        self.affinity: Affinity = affinity

    def __getitem__(self, index):
        if isinstance(index, slice):
            return DataSeries(self.series[index], self.affinity)
        return self.series[index]

    def __setitem__(self, index, value):
        self.series[index] = value

    def __delitem__(self, index):
        del self.series[index]

    def __iter__(self) -> Iterator[Any]:
        return iter(self.series)

    def __len__(self) -> int:
        return len(self.series)

    def append(self, value):
        self.series.append(value)

    def extend(self, values):
        self.series.extend(values)

    def insert(self, index, value):
        self.series.insert(index, value)

    def pop(self, index=-1):
        return self.series.pop(index)

    def clear(self):
        self.series.clear()

    def __contains__(self, item):
        return item in self.series

    def __repr__(self):
        return f"DataSeries({self.series!r}, affinity={self.affinity!r})"

class StaticMap:
    def __init__(self, values):
        self.values = values

class Group:
    def __init__(self, key: str, values: List[Any]):
        self.key: str = key
        self.values: List[Any] = values

class Calendar(DataSeries):
    def __init__(self, start: datetime | str, end: datetime | str):
        self.start: datetime = start if isinstance(start, datetime) else self.fromstr(start)
        self.end: datetime = end if isinstance(end, datetime) else self.fromstr(end)
        self.series: List[datetime] = self.generate_dates()
        self.affinity = Temporal
    
    def generate_dates(self) -> List[datetime]:
        dates = []
        current = self.start
        while current <= self.end:
            dates.append(current)
            current += timedelta(days=1)
        return dates
    
    def fromstr(self, date_str: str) -> datetime:
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            date = datetime.fromisoformat(date_str + "T00:00:00")
        elif re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$', date_str):
            date = datetime.fromisoformat(date_str)
        else:
            raise ValueError("Invalid date string format for start date")
        return date

class TemporalAccumulator:
    def __init__(self,
                 temporal_dimension: DataSeries,
                 catagories: List[DataSeries] | DataSeries = None,
                 quantities: List[DataSeries] | DataSeries = None,
                 static_maps: List[StaticMap] = None,
                 calendar: Calendar = None, 
                 interval: Interval = Interval(Days, 1),
                 ):
        self.temporal_dimension: DataSeries = temporal_dimension
        self.catagories: List[DataSeries] = catagories if isinstance(catagories, list) else [catagories] if catagories else []
        self.quantities: List[DataSeries] = quantities if isinstance(quantities, list) else [quantities] if quantities else []
        self.static_maps: List[StaticMap] = static_maps if static_maps else []
        self.calendar: Calendar = calendar if calendar else Calendar(temporal_dimension.series[0], temporal_dimension.series[-1])
        self.interval: Interval = interval
        self.processed_data: List[DataSeries] = self.accumulate()

    def categorize(self):
        # self.catagories is a list of DataSeries
        categorization_map = {}
        if len(self.catagories) == 0:
            categorization_map["__all__"] = self.temporal_dimension.series
        else:
            for i, category_value in enumerate(self.catagories[0]):
                if category_value not in categorization_map:
                    categorization_map[category_value] = {}
                if len(self.catagories) > 1:
                    prev = categorization_map[category_value]
                    for j in range(1, len(self.catagories)):
                        sub_category = self.catagories[j]
                        sub_category_value = sub_category[i]
                        if sub_category_value not in prev:
                          prev[sub_category_value] = {}
                        prev = prev[sub_category_value]
        return categorization_map
                        
                        
                        
    # Recursi
    def accumulate(self) -> List[DataSeries]:
        accumulation_map = {}
        for category in self.catagories:
            category_series = accumulation_map[category]
            for cat_value in category_series:
                accumulation_map[cat_value] = 
            

c = Calendar('2025-01-01', '2025-03-05')

for d in c:
    print(d)