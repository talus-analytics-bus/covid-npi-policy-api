def get_map_type_from_level(level: str) -> str:
    if level == "Local":
        return "us-county"
    elif level == "Local plus state/province":
        return "us-county-plus-state"
    elif level == "State / Province":
        return "us"
    elif level == "Country":
        return "global"
    else:
        raise ValueError("Unexpected level: " + str(level))
