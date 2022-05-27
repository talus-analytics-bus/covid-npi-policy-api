from datetime import date


def get_body_attr(body, attr_name, default=dict()) -> dict:
    has_attr_val = hasattr(body, attr_name)
    if has_attr_val:
        attr_val = getattr(body, attr_name)
        if attr_val is not None and hasattr(attr_val, "dict"):
            return attr_val.dict()
    return default


def get_static_excel_export_filename(is_summary: bool) -> str:
    """Returns the correct static Excel filename to use based on whether the Excel
    is full data or summary, and includes today's date.

    Args:
        is_summary (bool): Excel is summary only?

    Returns:
        str: Filename
    """
    # Examples:
    # COVID AMP - Full Data Export 2022-05-27
    # COVID AMP - Full Data Export (summary) 2022-05-27
    today_date: str = date.today().strftime("%Y-%m-%d")
    return (
        f"COVID AMP - Full Data Export{' (summary)' if is_summary else ''}"
        f" {today_date}.xlsx"
    )
