# Sentinel Preprocessor Configuration

Configuration is done via a json file, with a layout similar to:

```json
{
  "name": "Cairngorms", 
  "geojson": <geojson paste, such as from geojson.io>, 
  "dates": ["20200501", "20200601"], 
  "size": [256, 256], 
  "overlap": [0, 0],
  "cloudcover": [0, 5],
  "bands_S1": ["Sigma0_VH_S", "Sigma0_VV_S", "collocationFlags"],
  "bands_S2": ["B1_M", "B2_M", "B3_M", "B4_M", "B5_M", "B6_M", "B7_M", "B8_M", "B8A_M",
    "B9_M", "B11_M", "B12_M", "opaque_clouds_10m_M", "cirrus_clouds_10m_M", "scl_cloud_shadow_M",
    "scl_cloud_medium_proba_M", "scl_cloud_high_proba_M","scl_thin_cirrus_M"]
}
```

The output filename will be of the format:

    REGION_sXxY_oAxB_DATE1toDATE2.json

Where 

- `sXxY` is *size* X (width) by Y (height)
- `oAxB` is *overlap* A (width) by B (height)

### Config creation utility

This can be more easily created from the command line, using
`./src/senprep.py create`. This will give the user a prompt for each
required field. The `geojson` prompt requires a filename, unless `-c` is
added as an argument, in which case `geojson` data is assumed to be in
the users clipboard (uses the `pyperclip` library).

It can also be created from within a python repl, or jupyter notebook,
via:

```python3
from src import configutil
configutil.create(paste_geojson=True)
```

If `paste_geojson` is true, the user can paste geojson that they've
copied. Otherwise, a filename for a file containing geojson will be
requested.
