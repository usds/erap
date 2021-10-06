import numpy as np
import pandas as pd


def clean_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Clean common problems found in spreadsheets"""
    frame.columns = [str(col).lower().rstrip(",:") for col in frame.columns]
    frame = frame.applymap(
        lambda cell: cell.upper().strip().rstrip(",") if isinstance(cell, str) else cell
    )
    frame = frame.replace({np.nan: None})
    frame = frame.loc[
        :,
        ~(
            frame.columns.str.startswith("unnamed:")
            | frame.columns.str.startswith("Unnamed:")
        ),
    ]
    return frame
