import functions as fc


def test_lat_lon_geohash():
    assert fc.generate_geohash(37.7749, -122.4194) == '9q8yy'

test_lat_lon_geohash()