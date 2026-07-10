import pytest

from common.cities import CITIES, UnknownCityError, lookup


def test_every_city_has_country_and_coords():
    for city, info in CITIES.items():
        assert isinstance(info.country, str) and len(info.country) == 2
        assert -90.0 <= info.lat <= 90.0
        assert -180.0 <= info.lon <= 180.0


def test_at_least_two_countries():
    # Otherwise R3 (geo_hop) can never fire.
    assert len({info.country for info in CITIES.values()}) >= 2


def test_lookup_unknown_city_raises():
    with pytest.raises(UnknownCityError):
        lookup("Atlantis")
