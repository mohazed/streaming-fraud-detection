"""Static city -> (country_iso2, lat, lon) table. No network, no geocoding API.

The brief's generator emits `location` as a city name (`faker.city()`), but rule 3
asks for transactions across multiple *countries*. A city carries no country by
itself, so Spark broadcast-joins this table to derive one. See PLAN.md §2.1.
"""
from __future__ import annotations

from typing import NamedTuple


class CityInfo(NamedTuple):
    country: str
    lat: float
    lon: float


CITIES: dict[str, CityInfo] = {
    "Paris": CityInfo("FR", 48.8566, 2.3522),
    "Lyon": CityInfo("FR", 45.7640, 4.8357),
    "Marseille": CityInfo("FR", 43.2965, 5.3698),
    "London": CityInfo("GB", 51.5074, -0.1278),
    "Manchester": CityInfo("GB", 53.4808, -2.2426),
    "Berlin": CityInfo("DE", 52.5200, 13.4050),
    "Munich": CityInfo("DE", 48.1351, 11.5820),
    "Hamburg": CityInfo("DE", 53.5511, 9.9937),
    "Madrid": CityInfo("ES", 40.4168, -3.7038),
    "Barcelona": CityInfo("ES", 41.3851, 2.1734),
    "Rome": CityInfo("IT", 41.9028, 12.4964),
    "Milan": CityInfo("IT", 45.4642, 9.1900),
    "Amsterdam": CityInfo("NL", 52.3676, 4.9041),
    "Rotterdam": CityInfo("NL", 51.9244, 4.4777),
    "Brussels": CityInfo("BE", 50.8503, 4.3517),
    "Zurich": CityInfo("CH", 47.3769, 8.5417),
    "Geneva": CityInfo("CH", 46.2044, 6.1432),
    "Vienna": CityInfo("AT", 48.2082, 16.3738),
    "Lisbon": CityInfo("PT", 38.7223, -9.1393),
    "Porto": CityInfo("PT", 41.1579, -8.6291),
    "Dublin": CityInfo("IE", 53.3498, -6.2603),
    "Warsaw": CityInfo("PL", 52.2297, 21.0122),
    "Krakow": CityInfo("PL", 50.0647, 19.9450),
    "Prague": CityInfo("CZ", 50.0755, 14.4378),
    "Budapest": CityInfo("HU", 47.4979, 19.0402),
    "Stockholm": CityInfo("SE", 59.3293, 18.0686),
    "Oslo": CityInfo("NO", 59.9139, 10.7522),
    "Copenhagen": CityInfo("DK", 55.6761, 12.5683),
    "Helsinki": CityInfo("FI", 60.1699, 24.9384),
    "Athens": CityInfo("GR", 37.9838, 23.7275),
    "New York": CityInfo("US", 40.7128, -74.0060),
    "Los Angeles": CityInfo("US", 34.0522, -118.2437),
    "Chicago": CityInfo("US", 41.8781, -87.6298),
    "Toronto": CityInfo("CA", 43.6532, -79.3832),
    "Vancouver": CityInfo("CA", 49.2827, -123.1207),
    "Mexico City": CityInfo("MX", 19.4326, -99.1332),
    "Sao Paulo": CityInfo("BR", -23.5505, -46.6333),
    "Tokyo": CityInfo("JP", 35.6762, 139.6503),
    "Singapore": CityInfo("SG", 1.3521, 103.8198),
    "Sydney": CityInfo("AU", -33.8688, 151.2093),
    "Dubai": CityInfo("AE", 25.2048, 55.2708),
}


class UnknownCityError(KeyError):
    """Raised when a city has no entry in the static table."""


def lookup(city: str) -> CityInfo:
    try:
        return CITIES[city]
    except KeyError:
        raise UnknownCityError(city) from None
