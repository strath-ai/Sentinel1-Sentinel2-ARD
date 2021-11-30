# Naming Conventions for Sentinel-2 product

The naming convention for the Sentinel-2 products are
MMM_MSIXXX_YYYYMMDDHHMMSS_Nxxyy_ROOO_Txxxxx_<Product Discriminator>
where

MMM: is the mission ID(S2A/S2B)
    
MSIXXX: MSIL1C denotes the Level-1C product level/ MSIL2A denotes the Level-2A product level
    
YYYYMMDDHHMMSS: the datatake sensing start time
    
Nxxyy: the PDGS Processing Baseline number (e.g. N0204)
    
ROOO: Relative Orbit number (R001 - R143)
    
Txxxxx: Tile Number field
    
    
For e.g. S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443
    
Identifies a Level-1C product acquired by Sentinel-2A on the 5th of January, 2017 at 1:34:42 AM. It was acquired over Tile 53NMJ(2) during Relative Orbit 031, and processed with PDGS Processing Baseline 02.04

Here the tile no has the area predefined as per the 'Tiles and UTM Tiled Grid' section
