"""Google Maps Methods

Copyright (c) 2023 Colin Dietrich
MIT License, see LICENSE file for complete text.

API Reference:
https://pypi.org/project/googlemaps/
https://googlemaps.github.io/google-maps-services-python/docs/index.html

For explanation of component kwarg:
https://developers.google.com/maps/documentation/javascript/geocoding?hl=en#ComponentFiltering
"""


def reverse_geocode(gmaps_client, lat, lon):
    """Convert geographic coordinates into a human-readable address.

    Parameters
    ----------
    gmaps_client : authenticated googlemaps.client.Client instance
    lat : float, latitude in XXX projection
    lon : float, longitude in XXX projection

    Returns
    -------
    str, formatted address
    """

    reverse_geocode_result = gmaps_client.reverse_geocode((lat, lon))
    address = reverse_geocode_result[0]["formatted_address"]
    return address


def geocode(gmaps_client, address, iso_country):
    """Convert an address (like "1600 Amphitheatre Parkway, Mountain View, CA") into geographic coordinates (like latitude 37.423021 and longitude -122.083739), which you can use to place markers or position the map.

    Parameters
    ----------
    gmaps_client : authenticated googlemaps.client.Client instance
    address : str, formatted address
    iso_country : str, ISO 3166-1 country codes. For full list see:
        https://en.wikipedia.org/wiki/ISO_3166-1

    Returns
    -------
    lat : float, latitude in XXX projection
    lon : float, longitude in XXX projection
    """

    comp = {"country": iso_country}
    response = gmaps_client.geocode(address=address, components=comp)
    y = response[0]
    lat = y["geometry"]["location"]["lat"]
    lon = y["geometry"]["location"]["lng"]
    return lat, lon
