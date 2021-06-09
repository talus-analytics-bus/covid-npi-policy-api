from enum import Enum


# define allowed geo_res values
class GeoRes(str, Enum):
    country = "country"
    state = "state"
    county = "county"

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

    def test_is_child_of():
        assert GeoRes.state.is_child_of(GeoRes.country)
        assert not GeoRes.state.is_child_of(GeoRes.county)
        assert not GeoRes.state.is_child_of(GeoRes.state)
        assert GeoRes.county.is_child_of(GeoRes.state)
