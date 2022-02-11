#!/usr/bin/env bash
command=$1; shift

case $command in
    list|l) 
        USER=metaflow python -m src.list_flow $@
        ;; #---------------------------------------
    download|dl) 
        USER=metaflow python -m src.snap_flow --nosnap true $@
        ;; #---------------------------------------
    snap|snapper|process) 
        USER=metaflow python -m src.snap_flow $@
        ;; #---------------------------------------
    config)
        python -m src.configutil $@ 
        ;; #---------------------------------------
    *)
        echo "Sentinel1-Sentinel2-ARD Method Dispatcher"
        echo
        echo "Multiple scripts are provided:"
        echo 
        echo "- 'list' -- will show the products that would be used."
        echo "- 'download' -- retrieves products from relevant endpoints."
        echo "- 'snap' -- runs the ARD framework with downloading, preprocessing, cropping, and patch generation."
        echo "- 'config' -- run the configuration helper. clone/create configurations"
        echo
        echo "Run 'list help', 'download help', or 'snap help' to get full usage information."
        ;; #---------------------------------------
esac
