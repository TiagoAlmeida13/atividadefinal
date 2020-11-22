from dataclasses import dataclass
from datetime import datetime as datetime_class
from pytz import UTC

@dataclass
class Point:
    datetime: datetime_class
    value: float

    @property
    def timestamp(self):
        return int(self.datetime.timestamp())

    def as_raw_tuple(self):
        return self.timestamp, self.value

    def as_raw_dict(self):
        return {self.timestamp: self.value}

    def to_utc(self):
        dt_as_utc = self.datetime.astimezone(UTC) if self.datetime.tzinfo else UTC.localize(self.datetime)
        return Point(dt_as_utc, self.value)