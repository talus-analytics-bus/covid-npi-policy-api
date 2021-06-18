from enum import Enum


# define allowed geo_res values
class GeoRes(str, Enum):
    country = "country"
    state = "state"
    county = "county"
    county_plus_state = "county_plus_state"

    def is_child_of(self, geo_res: "GeoRes") -> bool:
        if self.name == "country" or self.name == geo_res.name:
            return False
        elif self.name == "state":
            return geo_res.name == "country"
        elif self.name == "county":
            return geo_res.name in ("country", "state")
        else:
            raise NotImplementedError(
                "This method is not yet implemented for this "
                "geographic resolution: " + self.name
            )

    def get_loc_field(self) -> str:
        """Returns the field corresponding to the geographic resolution's
        location filter in the COVID AMP data system.

        Raises:
            NotImplementedError: If geographic resolution's location filter has
            not been defined.

        Returns:
            str: The field corresponding to the geographic resolution's
            location filter in the COVID AMP data system.
        """
        name: str = self.name
        if name == "country":
            return "iso3"
        elif name == "state":
            return "area1"
        elif name in ("county", "county_plus_state"):
            return "ansi_fips"
        else:
            raise NotImplementedError(
                "No location field defined for this GeoRes, please update "
                "method `get_loc_field`: " + self
            )

    def get_level(self) -> str:
        """Returns the value corresponding to the geographic resolution's
        level in the COVID AMP data system.

        Raises:
            NotImplementedError: If geographic resolution's level value has
            not been defined.

        Returns:
            str: The value corresponding to the geographic resolution's
            level in the COVID AMP data system.
        """
        name: str = self.name
        if name == "country":
            return "Country"
        elif name == "state":
            return "State / Province"
        elif name == "county":
            return "Local"
        elif name == "county_plus_state":
            return "Local plus state/province"
        else:
            raise NotImplementedError(
                "No level defined for this GeoRes, please update "
                "method `get_level`: " + self
            )

    def get_map_type(self):
        """Returns the map type corresponding to the geographic resolution
        in the COVID AMP data system.

        Raises:
            NotImplementedError: If geographic resolution's map type value has
            not been defined.

        Returns:
            str: The value corresponding to the geographic resolution's
            map type in the COVID AMP data system.
        """
        name: str = self.name
        if name == "country":
            return "global"
        elif name == "state":
            return "us"
        elif name == "county":
            return "us-county"
        elif name == "county_plus_state":
            return "us-county-plus-state"
        else:
            raise NotImplementedError(
                "No level defined for this GeoRes, please update "
                "method `get_map_type`: " + self
            )

    # TESTS # --------------------------------------------------------------- #
    def test_is_child_of():
        assert GeoRes.state.is_child_of(GeoRes.country)
        assert not GeoRes.state.is_child_of(GeoRes.county)
        assert not GeoRes.state.is_child_of(GeoRes.state)
        assert GeoRes.county.is_child_of(GeoRes.state)
