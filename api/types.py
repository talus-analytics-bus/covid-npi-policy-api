from db.models import Plan, Policy
from enum import Enum
from db import db


# ClassName = Enum(
#     value="ClassName",
#     names=[
#         ("Policy", "Policy"),
#         ("Plan", "Plan"),
#         ("Court_Challenge", "Court_Challenge"),
#         ("none", ""),
#     ],
# )


class ClassName(str, Enum):
    Policy = "Policy"
    Plan = "Plan"
    Court_Challenge = "Court_Challenge"
    none = ""

    def get_place_field_name(self) -> str:
        """Returns the field corresponding to this entity ClassName on the
        Place entity.

        Raises:
            ValueError: If no such field exists.

        Returns:
            str: The field corresponding to this entity ClassName on the
            Place entity
        """
        if self.name == "Policy":
            return "policies"
        elif self.name == "Plan":
            return "plans"
        else:
            raise ValueError(
                "No place field name exists for this ClassName: " + self.name
            )

    def get_db_model(self) -> db.Entity:
        """Returns the database model for this entity.

        Raises:
            ValueError: If no model exists.
        Returns:
            db.Entity: The database model.
        """
        if self.name == "Policy":
            return Policy
        elif self.name == "Plan":
            return Plan
        else:
            raise ValueError(
                "No database model exists for this ClassName: " + self.name
            )


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
