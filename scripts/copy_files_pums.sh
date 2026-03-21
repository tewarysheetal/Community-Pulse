#!/bin/bash

SOURCE_ROOT="/d/Projects/Community-Pulse/data/raw"
TARGET_ROOT="/d/Projects/Community-Pulse/data/pums"

for src_dir in "$SOURCE_ROOT"/*; do
    [ -d "$src_dir" ] || continue

    echo "Processing directory: $src_dir"

    folder_name=$(basename "$src_dir")

    year=$(echo "$folder_name" | grep -oP '(19|20)\d{2}')
    [ -z "$year" ] && echo "No year found in $folder_name, skipping" && continue

    if echo "$folder_name" | grep -q "_hil_"; then
        dest_file="housing.csv"
    elif echo "$folder_name" | grep -q "_pil_"; then
        dest_file="person.csv"
    else
        echo "Unknown folder type: $folder_name, skipping"
        continue
    fi

    target_dir="$TARGET_ROOT/$year"
    mkdir -p "$target_dir"

    src_file=$(ls "$src_dir"/*.csv 2>/dev/null | head -1)
    if [ -z "$src_file" ]; then
        echo "No data file found in $folder_name, skipping"
        continue
    fi

    cp "$src_file" "$target_dir/$dest_file"
    echo "Copied $folder_name -> $year/$dest_file"
done

echo "Done"