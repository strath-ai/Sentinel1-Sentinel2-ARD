#!/usr/bin/env bash
snap --nosplash --nogui --modules --update-all 2>&1 | while read -r line; do
    echo "$line"
    if [[ "$line" = "updates=0" ]]; then
        sleep 2
        pkill -TERM -f "snap/jre/bin/java"
        exit
    fi
done

