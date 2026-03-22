#!/bin/bash

TARGET_ROOT="/d/Projects/Community-Pulse/data/yearly-data"

for sub_dir in "$TARGET_ROOT"/*; do
    [ -d "$sub_dir" ] || continue

    folder_name=$(basename "$sub_dir")

    for file in "$sub_dir"/*.csv; do
        [ -e "$file" ] || continue

        year=$(basename "$file" | grep -o '20[0-9][0-9]')

        if [ -n "$year" ]; then
            new_name="${folder_name}${year}-Data.csv"
            mv "$file" "$sub_dir/$new_name"
            echo "Renamed $(basename "$file") -> $new_name"
        fi
    done
done

echo "Done"