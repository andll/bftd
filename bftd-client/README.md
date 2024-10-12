# Using bftd client

```bash
export BFTD_URL=http://...
```

Send data to a channel:
```
cargo run -p bftd-client -- submit --channels 1 --data ccc
# Also same data can be atomically sent to multiple channels
cargo run -p bftd-client -- submit --channels 2,3,4 --data ccc
# Can also pipe data (sends as a single message, 64Kb message limit)
echo {1..15} | cargo run -p bftd-client -- submit --channels 5
# And send each line as a separate message
echo \\n{1..15} | cargo run -p bftd-client -- submit --channels 6 --stream
```

Tail data from the beginning of the channel:
```
cargo run -p bftd-client -- tail --from 1
# Can also tail as new line separated json stream
cargo run -p bftd-client -- tail --from 1 --json
# Request metadata for the committed events 
cargo run -p bftd-client -- tail --from 1 --metadata timestamp,commit,block
# Resume stream from last received event(stream excludes the event provided)
cargo run -p bftd-client -- tail --from 00000001-00000000-000003f3-00000000
```
