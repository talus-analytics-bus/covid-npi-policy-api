def get_body_attr(body, attr_name, default=dict()) -> dict:
    has_attr_val = hasattr(body, attr_name)
    if has_attr_val:
        attr_val = getattr(body, attr_name)
        if attr_val is not None and hasattr(attr_val, "dict"):
            return attr_val.dict()
    return default
