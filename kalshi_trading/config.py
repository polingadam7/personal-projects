WEATHER_SERIES = [
    "KXHIGHNY",    # NYC daily high temperature
    "KXHIGHMIA",   # Miami daily high temperature
    "KXHIGHTDAL",  # Dallas daily high temperature
    "KXHIGHTDC",   # Washington DC daily high temperature
    "KXRAINNYC",   # NYC rain
]

# Minimum edge (forecast prob - market price) required to place a trade
MIN_EDGE = 0.07  # 7 cents

# Only trade markets closing at least this many hours from now
MIN_HOURS_TO_CLOSE = 6

# Max fraction of balance to risk per trade
MAX_POSITION_PCT = 0.05  # 5%

# Min and max contracts per order
MIN_CONTRACTS = 1
MAX_CONTRACTS = 20
