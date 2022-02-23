from typing import List

import pandas as pd
from dask import dataframe as dd


def convert_returned_points_to_ddf(points: List) -> dd.DataFrame:
    # [self.id, self.head, self.x, self.y, self.z, self.tvu, self.rejected, self.pointtime, self.beam, self.linename]

    # THis is how our algos currently expect data
    # df = pd.DataFrame(
    #     {
    #         "lon": [0, 0, 0, 0],
    #         "lat": [0, 0, 0, 0],
    #         "depth": [0, 1, 100, 99],
    #         "ping_number": [0, 0, 0, 0],
    #         "beam_number": [0, 0, 0, 0],
    #         "beam_flag": [0, 0, 0, 0],
    #         "filename": ["file1", "file1", "file1", "file1"],
    #     }
    # )

    df = pd.DataFrame(
        {
            "id": points[0],
            "head": points[1],
            "x": points[2],
            "y": points[3],
            "z": points[4],
            "tvu": points[5],
            "rejected": points[6],
            "pointtime": points[7],
            "linename": points[8],
        }
    )

    ddf = dd.from_pandas(df, npartitions=1)
    return (ddf)
