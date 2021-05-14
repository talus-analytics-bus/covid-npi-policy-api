def get_loc_field_from_geo_res(geo_res: str) -> str:
    """Returns the Place field uniquely identifying a location at the defined
    geographic resolution.

    Args:
        geo_res (str): The geographic resolution.

    Raises:
        ValueError: Unknown geographic resolutions.

    Returns:
        str: The Place field uniquely identifying a location at the defined
    geographic resolution.
    """
    if geo_res == "country":
        return "iso3"
    elif geo_res == "state":
        return "area1"
    elif geo_res == "county":
        return "area2"  # TODO replace this with "ansi_fips"
    else:
        raise ValueError("Unknown geo_res: " + geo_res)


def get_level_from_geo_res(geo_res: str) -> str:
    """Returns the Place field defining the level of place corresponding to the
    defined geographic resolution.

    Args:
        geo_res (str): The geographic resolution.

    Raises:
        ValueError: Unknown geographic resolutions.

    Returns:
        str: the Place field defining the level of place corresponding to the
    defined geographic resolution
    """
    if geo_res == "country":
        return "Country"
    elif geo_res == "state":
        return "State / Province"
    elif geo_res == "county":
        return "Local"
    else:
        raise ValueError("Unknown geo_res: " + geo_res)
