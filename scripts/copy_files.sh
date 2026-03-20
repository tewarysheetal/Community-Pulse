#!/bin/bash

SOURCE_ROOT="/d/Projects/Community-Pulse/data/raw"
TARGET_ROOT="/d/Projects/Community-Pulse/data/yearly-data"

for src_dir in "$SOURCE_ROOT"/*; do
    [ -d "$src_dir" ] || continue

    echo "Processing directory: $src_dir"

    folder_name=$(basename "$src_dir")

    clean_name=$(echo "$folder_name" | sed 's/(.*)//' | sed 's/-20[0-9][0-9]-20[0-9][0-9]//' | tr 'A-Z' 'a-z')

    echo "Cleaned folder name: $clean_name"

    target_dir="$TARGET_ROOT/$clean_name"
    mkdir -p "$target_dir"

    cp "$src_dir"/*2019*-Data.csv "$target_dir" 2>/dev/null
    cp "$src_dir"/*202*-Data.csv "$target_dir" 2>/dev/null

    echo "Copied files from $folder_name to $clean_name"
done

echo "Done"