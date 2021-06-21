import pandas as pd

# debug data for testing purposes
debug_df: pd.DataFrame = pd.DataFrame(
    [
        {
            "primary_ph_measure": "Social distancing",
            "ph_measure_details": "Early prison release",
        },
        {
            "primary_ph_measure": "Contact tracing/Testing",
            "ph_measure_details": "Contact tracing",
        },
    ]
)

debug_df_expected: pd.DataFrame = pd.DataFrame(
    [
        {
            "primary_ph_measure": "Enabling and relief measures",
            "ph_measure_details": "Early prison release",
        },
        {
            "primary_ph_measure": "Contact tracing/Testing",
            "ph_measure_details": "Contact tracing",
        },
    ]
)
