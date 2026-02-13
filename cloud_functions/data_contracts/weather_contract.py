from typing import Optional, List
from pydantic import BaseModel, field_validator


class HourlyUnits(BaseModel):
    time: Optional[str] = None
    temperature_2m: Optional[str] = None
    relativehumidity_2m: Optional[str] = None
    dewpoint_2m: Optional[str] = None
    apparent_temperature: Optional[str] = None
    precipitation: Optional[str] = None
    rain: Optional[str] = None
    snowfall: Optional[str] = None
    snow_depth: Optional[str] = None
    weathercode: Optional[str] = None
    pressure_msl: Optional[str] = None
    cloudcover: Optional[str] = None
    visibility: Optional[str] = None
    windspeed_10m: Optional[str] = None
    winddirection_10m: Optional[str] = None
    windgusts_10m: Optional[str] = None


class HourlyData(BaseModel):
    time: List[str]
    temperature_2m: List[Optional[float]]
    relativehumidity_2m: Optional[List[Optional[float]]] = None
    dewpoint_2m: Optional[List[Optional[float]]] = None
    apparent_temperature: Optional[List[Optional[float]]] = None
    precipitation: Optional[List[Optional[float]]] = None
    rain: Optional[List[Optional[float]]] = None
    snowfall: Optional[List[Optional[float]]] = None
    snow_depth: Optional[List[Optional[float]]] = None
    weathercode: Optional[List[Optional[int]]] = None
    pressure_msl: Optional[List[Optional[float]]] = None
    cloudcover: Optional[List[Optional[float]]] = None
    visibility: Optional[List[Optional[float]]] = None
    windspeed_10m: Optional[List[Optional[float]]] = None
    winddirection_10m: Optional[List[Optional[float]]] = None
    windgusts_10m: Optional[List[Optional[float]]] = None

    @field_validator("temperature_2m")
    @classmethod
    def temperature_in_range(cls, v):
        for temp in v:
            if temp is not None and not -60 <= temp <= 60:
                raise ValueError(
                    f"Temperature {temp}°C outside reasonable range (-60 to 60)"
                )
        return v

    @field_validator("precipitation", "rain", "snowfall")
    @classmethod
    def precipitation_non_negative(cls, v):
        if v is not None:
            for val in v:
                if val is not None and val < 0:
                    raise ValueError(f"Precipitation value cannot be negative: {val}")
        return v

    @field_validator("windspeed_10m", "windgusts_10m")
    @classmethod
    def wind_non_negative(cls, v):
        if v is not None:
            for val in v:
                if val is not None and val < 0:
                    raise ValueError(f"Wind speed cannot be negative: {val}")
        return v


class WeatherContract(BaseModel):
    """Contract for raw weather data from Open-Meteo API."""

    latitude: float
    longitude: float
    generationtime_ms: Optional[float] = None
    utc_offset_seconds: Optional[int] = None
    timezone: Optional[str] = None
    timezone_abbreviation: Optional[str] = None
    elevation: Optional[float] = None
    hourly_units: Optional[HourlyUnits] = None
    hourly: HourlyData

    @field_validator("latitude")
    @classmethod
    def latitude_in_range(cls, v):
        if not -90 <= v <= 90:
            raise ValueError(f"Latitude {v} outside valid range (-90 to 90)")
        return v

    @field_validator("longitude")
    @classmethod
    def longitude_in_range(cls, v):
        if not -180 <= v <= 180:
            raise ValueError(f"Longitude {v} outside valid range (-180 to 180)")
        return v
