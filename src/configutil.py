#!/usr/bin/env python3
"""Managing SentinelPreprocessor config.

Usage:
    configutil.py create [-c]
    configutil.py clone [FILENAME]

Options:
    -c                               Let user paste geojson, rather than ask for filename
"""
import json
from pathlib import Path
from dataclasses import dataclass
from typing import Tuple, List, Dict
from datetime import date
from omegaconf import OmegaConf


def to_int_list(s: str, separator: str = " ") -> List[int]:
    return list(map(int, s.split(separator)))


def __filename(config_dict):
    size_str = f"s{config_dict['size'][0]}x{config_dict['size'][1]}"
    overlap_str = f"o{config_dict['overlap'][0]}x{config_dict['overlap'][1]}"
    date_str = f"{config_dict['dates'][0]}to{config_dict['dates'][1]}"
    config_filename = f"{config_dict['name']}-{size_str}-{overlap_str}-{date_str}.json"
    return config_filename


def __ask_for_roi():
    name = input("ROI Name (for directory): ")

    if paste_geojson:
        roi = input("Paste geojson data: ")
    else:
        filename = input("ROI filename to import: ")
        roi = open(filename).read()
    return name, roi


def __ask_for_bands():
    bands = OmegaConf.load(open("../data/sentinel_bands.json"))
    bands_selection = input("Use default S1 and S2 bands? [y/n]")[0]
    if bands_selection[0].lower() in "Yy":
        print("Default bands selected")
        bands_S1 = [v['band'] for v in bands['S1'].values() if v['is_default']]
        bands_S2 = [v['band'] for v in bands['S2'].values() if v['is_default']]
    else:
        print("Bands for S1:")
        print({k: v['band'] for k, v in bands['S1'].items()})
        bands_chosen = to_int_list(input("Choose S1 bands (space-separated): "))
        bands_S1 = [bands['S1'][s]['band'] for s in bands_chosen]

        print("Bands for S2:")
        print({k: v['band'] for k, v in bands['S2'].items()})
        bands_chosen = to_int_list(input("Choose S2 bands (space-separated): "))
        bands_S2 = [bands['S2'][s]['band'] for s in bands_chosen]
    return bands_S1, bands_S2


def __ask_for_patch_size():
    return to_int_list(input("Patch Size (`width height`): "))


def __ask_for_patch_overlap():
    return to_int_list(input("Patch Overlap (`width height`): "))


def __ask_for_cloud_cover():
    return to_int_list(input("Cloud cover (`min max`): "))


def __ask_for_finder_callback():
    print("Callback (function) in SentinelProductFinder for finding ids?")
    return input("> ")


def __ask_for_snap_callback():
    print("Callback (function) in Snapper for processing ids?")
    return input("> ")



def __ask_for_dates():
    date_start = input("Start date (yyyymmdd): ")
    date_end = input("End date (yyymmdd): ")
    return date_start, date_end


def __ask_for_cloud_filtering():
    cloud_mask_filtering = input("Choose based on cloud coverage? [Y/n]")
    if cloud_mask_filtering:
        return cloud_mask_filtering[0] in 'Yy'
    return False


def create(paste_geojson=False):
    """Create a SentinelPreprocessor configuration file."""
    name, roi = __ask_for_roi()
    date_start, date_end = __ask_for_dates()
    size = __ask_for_patch_size()
    overlap = __ask_for_patch_overlap()
    cloudcover = __ask_for_cloud_cover()
    finder_callback = __ask_for_finder_callback()
    snap_callback = __ask_for_snap_callback()
    bands_s1, bands_s2 = __ask_for_bands()
    cloud_mask_filtering = __ask_for_cloud_filtering()

    config_dir = Path("configurations")
    config_dir.mkdir(exist_ok=True, parents=True)
    config = {
        "name": name,
        "geojson": roi,
        "dates": [date_start, date_end],
        "size": size,
        "overlap": overlap,
        "cloudcover": cloudcover,
        "cloud_mask_filtering": cloud_mask_filtering,
        "callback_find_products": finder_callback,
        "callback_snap": snap_callback,
        "bands_S1": bands_s1,
        "bands_S2": bands_s2,
    }
    full_path = config_dir / __filename(config)
    json.dump(config, full_path.open("w"), indent=2)
    print(f"Config '{full_path}' created")


def clone(filename=None):
    """Clone an existing SentinelPreprocessor configuration file, and change details."""
    if not filename:
        files = sorted(list(Path("configurations").glob("*.json")))
        for i, fname in enumerate(files):
            print(i, fname)
        invalid_input = True
        response = -1
        while invalid_input:
            response = input("Which configuration to copy? ").strip()
            if response == "":
                print("Exiting without cloning.")
                return
            try:
                response = int(response)
                invalid_input = False
            except:
                print("Invalid answer. Number, or blank to exit without cloning.")
                continue
        filename = files[int(response)]

    config = json.load(open(filename))

    msg = f"Cloning {config['name']}\n"
    msg += "Enter what fields you want to change, separated by space. Options:"
    msg += "\n- ".join(config.keys())
    print(msg)
    to_change = input("> ").split(" ")

    if "name" in to_change or "geojson" in to_change:
        name, roi = __ask_for_roi()
        config['name'] = name
        config['geojson'] = roi
    if "dates" in to_change:
        config['dates'] = __ask_for_dates()
    if "size" in to_change:
        config['size'] = __ask_for_patch_size()
    if "overlap" in to_change:
        config['overlap'] = __ask_for_patch_overlap()
    if "cloudcover" in to_change:
        config['cloudcover'] = __ask_for_cloud_cover()
    if "callback_find_products" in to_change:
        config['callback_find_products'] = __ask_for_finder_callback()
    if "callback_snap" in to_change:
        config['callback_snap'] = __ask_for_snap_callback()
    if "bands_S1" in to_change or "bands_S2" in to_change:
        bands_s1, bands_s2 = __ask_for_bands()
        config['bands_S1'] = bands_s1
        config['bands_S2'] = bands_s2

    full_path = Path(filename).parent / __filename(config)
    json.dump(config, full_path.open("w"), indent=2)
    print(f"Config '{full_path}' created")


def display(filename):
    """Display a SentinelPreprocessor configuration file."""
    config = json.load(open(filename))
    msg = f"Name: {config['name']}"
    msg += f"\n> Dates       | {config['dates'][0]} to {config['dates'][1]}"
    msg += f"\n> Size        | {config['size']}"
    msg += f"\n> Overlap     | {config['overlap']}"
    msg += f"\n> Cloud cover | {config['cloudcover']}"
    print(msg)


def list_available(root=None):
    """List available SentinelPreprocessor configuration files."""
    print("Available configurations")
    print("------------------------")
    print("configurations/")
    if not root:
        root = "configurations"
    for filename in sorted(Path(root).glob("*.json")):
        print("\t", filename.name)


if __name__ == "__main__":
    from docopt import docopt
    args = docopt(__doc__)
    if args["create"]:
        create(args["-c"])
    elif args["clone"]:
        clone(args["FILENAME"])
    else:
        list_available()
