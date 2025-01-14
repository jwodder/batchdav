#!/usr/local/bin/bash
base=https://webdav.dandiarchive.org
#path=zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr/
path=zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr/0/0/0/0/0/

#cargo run -qr -- run "$base/$path" 20 |& tee log
#cargo run -qr -- run -q "$base/$path" 20
cargo run -r -- run "$base/$path" 20
