"""Pure, seeded transaction simulator. No I/O, no Kafka, no ambient clock.

Deterministic: generate(seed, n, start) always returns the same list of dicts
for the same inputs. See PLAN.md §6.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta

from common.cities import CITIES
from common.contracts import AMOUNT_MAX, AMOUNT_MIN, CURRENCIES, LABEL_FIELD, METHODS

_CITY_NAMES = tuple(CITIES.keys())

_COUNTRY_TO_CITIES: dict[str, tuple[str, ...]] = {}
for _name, _info in CITIES.items():
    _COUNTRY_TO_CITIES.setdefault(_info.country, []).append(_name)
_COUNTRY_TO_CITIES = {k: tuple(v) for k, v in _COUNTRY_TO_CITIES.items()}
_COUNTRIES = tuple(_COUNTRY_TO_CITIES.keys())

_USER_ID_MIN, _USER_ID_MAX = 1000, 9999
_MAX_USERS = _USER_ID_MAX - _USER_ID_MIN + 1
_NEW_USER_PROB = 0.2
_BIG_SPENDER_PROB = 0.08

# Archetype injection probabilities, checked per generated record. See PLAN.md §6.
_P_WHALE, _P_BURST, _P_TRAVELLER = 0.004, 0.0015, 0.003
_WHALE_MIN, _WHALE_MAX = 1200.0, 8000.0
_BURST_LEN_MIN, _BURST_LEN_MAX = 5, 8
_BURST_GAP_MIN, _BURST_GAP_MAX = 1.0, 6.5          # keeps an 8-tx burst under 60s
_TRAVELLER_LEN = 3
_TRAVELLER_GAP_MIN, _TRAVELLER_GAP_MAX = 20.0, 75.0  # keeps 3 hops under 3 minutes
_NORMAL_GAP_MIN, _NORMAL_GAP_MAX = 0.02, 2.5


class _User:
    __slots__ = ("user_id", "home_city", "mu", "sigma")

    def __init__(self, user_id: str, home_city: str, mu: float, sigma: float):
        self.user_id = user_id
        self.home_city = home_city
        self.mu = mu
        self.sigma = sigma


def _new_user(rng: random.Random, index: int) -> _User:
    user_id = f"u{_USER_ID_MIN + index:04d}"
    home_city = rng.choice(_CITY_NAMES)
    # A minority of users are big spenders: their lognormal median alone can clear
    # HIGH_VALUE_EUR, which is what makes normal (non-fraud) traffic sometimes
    # exceed 1000 on its own. See PLAN.md §6.
    if rng.random() < _BIG_SPENDER_PROB:
        mu = rng.uniform(6.3, 7.2)
    else:
        mu = rng.uniform(3.0, 5.8)
    sigma = rng.uniform(0.35, 0.75)
    return _User(user_id, home_city, mu, sigma)


def _pick_user(rng: random.Random, users: list[_User]) -> _User:
    if not users or (len(users) < _MAX_USERS and rng.random() < _NEW_USER_PROB):
        user = _new_user(rng, len(users))
        users.append(user)
        return user
    return rng.choice(users)


def _pick_location(rng: random.Random, user: _User) -> str:
    if rng.random() < 0.8:
        return user.home_city
    return rng.choice(_CITY_NAMES)


def _draw_amount(rng: random.Random, user: _User) -> float:
    amount = rng.lognormvariate(user.mu, user.sigma)
    return round(min(max(amount, AMOUNT_MIN), AMOUNT_MAX), 2)


def _record(user: _User, amount: float, currency: str, method: str, location: str,
            when: datetime, is_fraud: int) -> dict:
    return {
        "user_id": user.user_id,
        "amount": amount,
        "currency": currency,
        "timestamp": when.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "location": location,
        "method": method,
        LABEL_FIELD: is_fraud,
    }


def _emit_normal(rng, users, clock) -> tuple[dict, datetime]:
    user = _pick_user(rng, users)
    record = _record(user, _draw_amount(rng, user), rng.choice(CURRENCIES),
                      rng.choice(METHODS), _pick_location(rng, user), clock, 0)
    clock += timedelta(seconds=rng.uniform(_NORMAL_GAP_MIN, _NORMAL_GAP_MAX))
    return record, clock


def _emit_whale(rng, users, clock) -> tuple[list[dict], datetime]:
    user = _pick_user(rng, users)
    amount = round(rng.uniform(_WHALE_MIN, _WHALE_MAX), 2)
    record = _record(user, amount, rng.choice(CURRENCIES), rng.choice(METHODS),
                      _pick_location(rng, user), clock, 1)
    clock += timedelta(seconds=rng.uniform(_NORMAL_GAP_MIN, _NORMAL_GAP_MAX))
    return [record], clock


def _emit_burst(rng, users, clock) -> tuple[list[dict], datetime]:
    user = _pick_user(rng, users)
    records = []
    for _ in range(rng.randint(_BURST_LEN_MIN, _BURST_LEN_MAX)):
        records.append(_record(user, _draw_amount(rng, user), rng.choice(CURRENCIES),
                                 rng.choice(METHODS), _pick_location(rng, user), clock, 1))
        clock += timedelta(seconds=rng.uniform(_BURST_GAP_MIN, _BURST_GAP_MAX))
    return records, clock


def _emit_traveller(rng, users, clock) -> tuple[list[dict], datetime]:
    user = _pick_user(rng, users)
    records = []
    for country in rng.sample(_COUNTRIES, _TRAVELLER_LEN):
        location = rng.choice(_COUNTRY_TO_CITIES[country])
        records.append(_record(user, _draw_amount(rng, user), rng.choice(CURRENCIES),
                                 rng.choice(METHODS), location, clock, 1))
        clock += timedelta(seconds=rng.uniform(_TRAVELLER_GAP_MIN, _TRAVELLER_GAP_MAX))
    return records, clock


def generate(seed: int, n: int, start: datetime) -> list[dict]:
    """Deterministic. Same (seed, n, start) -> identical list, always."""
    rng = random.Random(seed)
    users: list[_User] = []
    clock = start
    records: list[dict] = []

    while len(records) < n:
        remaining = n - len(records)
        roll = rng.random()

        if roll < _P_WHALE:
            new_records, clock = _emit_whale(rng, users, clock)
        elif remaining >= _BURST_LEN_MAX and roll < _P_WHALE + _P_BURST:
            new_records, clock = _emit_burst(rng, users, clock)
        elif remaining >= _TRAVELLER_LEN and roll < _P_WHALE + _P_BURST + _P_TRAVELLER:
            new_records, clock = _emit_traveller(rng, users, clock)
        else:
            record, clock = _emit_normal(rng, users, clock)
            new_records = [record]

        records.extend(new_records)

    for i, record in enumerate(records):
        record["transaction_id"] = f"t-{i:07d}"
    return records
