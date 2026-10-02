#!/usr/bin/env python3
import argparse, hashlib, json, tarfile, tomllib
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--target',default='x86_64-unknown-linux-gnu');a=p.parse_args()
root=Path(__file__).resolve().parent.parent
binary=root/'target'/a.target/'release/kline-proxy';assert binary.is_file(),binary
version=tomllib.loads((root/'Cargo.toml').read_text())['workspace']['package']['version'];out=root/'dist';out.mkdir(exist_ok=True)
files={'kline-proxy':binary,'LICENSE-NotoSans.txt':root/'crates/kline-market/assets/OFL.txt','config.example.json':root/'examples/full-market.json','kline-proxy-rs.service':root/'deploy/kline-proxy-rs.service','deployment.md':root/'docs/deployment.md','config-migration.md':root/'docs/config-migration.md'}
manifest={name:hashlib.sha256(path.read_bytes()).hexdigest() for name,path in files.items()}
manifest_file=out/'manifest.json';manifest_file.write_text(json.dumps({'version':version,'target':a.target,'files':manifest},indent=2)+'\n')
archive=out/f'kline-proxy-rs-{version}-{a.target}.tar.gz'
with tarfile.open(archive,'w:gz') as tar:
    for name,path in files.items():tar.add(path,arcname=name)
    tar.add(manifest_file,arcname='manifest.json')
print(archive);print(hashlib.sha256(archive.read_bytes()).hexdigest())
