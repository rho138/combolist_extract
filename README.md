# Combolist Extractor

## Prior to execution
Since most combolists come with a glorious amount of free extra data, we are going to strip that. Run the code in the command block below in the base `data` directory of your combolist dump.
```
# Strip out any binary data from file dumps prior to execution
find . -type f -print0 |
while read -r -d '' file; do
  tr -cd '\11\12\15\40-\176' < "$file" > "${file}.tmp" \
    && mv "${file}.tmp" "$file"
done

```

## Requirements
We only have one package not in stdlib to install, `tqdm`, you can use pip to perform the action
```
pip install -r requirements.txt
```

## Outputs
iterates a 'data' directory with combolist text files and parses them into two files.

|       File      |                            description                             |
|-----------------|--------------------------------------------------------------------|
| run_su_good.txt | Sorted, deduped, and unique count of domains based on users found within the dump |
| run_su_bad.txt  | Sorted and deduped lines that were not in proper formatting for ingestion into Elastic Stack via filebeat.                                                                   |

NOTE: This could be more thorough, such as checking for password re-use, but really this is to get the ball rolling on priming the data for filebeat ingest.

## Arguments
|      switch     |                           description                           |
|-----------------|-----------------------------------------------------------------|
| `--path`, `-p`  | Set the path, else use pwd. Target path must have 'data' folder |
| `--cores`, `-c` | Set max cores, else set to `cpu_count()-1`                                                                |

## Future Plans
- [ ] Switch option to skip domain shenanigans and only grab `<emailaddr>:<password>`
- [ ] Switch option to create a file that's able to be parsed by filebeat
- [ ] Include index mapping for filebeat