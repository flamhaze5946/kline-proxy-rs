#!/usr/bin/env python3
"""Read-only comparison of CLOSED bars; does not switch Nginx or mirror live traffic.
Client durations include connection/network overhead and are not production p99.
"""
import argparse, concurrent.futures, datetime, json, time, urllib.request
from pathlib import Path

def read(url):
    started=time.perf_counter()
    try:
        with urllib.request.urlopen(url,timeout=15) as response:
            return {'status':response.status,'body':json.load(response),'client_ms':(time.perf_counter()-started)*1000}
    except Exception as error:return {'error':str(error),'client_ms':(time.perf_counter()-started)*1000}

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--java-url',required=True);p.add_argument('--rust-url',required=True)
    p.add_argument('--symbols',default='BTCUSDT,ETHUSDT');p.add_argument('--hours',type=float,default=2)
    p.add_argument('--period',type=float,default=60);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();end=time.monotonic()+a.hours*3600;rounds=0;mismatches=0;errors=0
    a.output.parent.mkdir(parents=True,exist_ok=True)
    with a.output.open('x') as output,concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        while True:
            # Avoid a changing forming candle by using a common explicit end timestamp.
            for interval,span in [('1h',3600000),('1d',86400000)]:
                cutoff=int(time.time()*1000)//span*span-1
                for prefix in ['fapi/v1','api/v3']:
                    for symbol in a.symbols.split(','):
                        query=f'/{prefix}/klines?symbol={symbol}&interval={interval}&endTime={cutoff}&limit=2'
                        left=pool.submit(read,a.java_url.rstrip('/')+query);right=pool.submit(read,a.rust_url.rstrip('/')+query)
                        java,rust=left.result(),right.result();ok=java.get('status')==rust.get('status')==200
                        same=ok and java['body']==rust['body'];errors+=not ok;mismatches+=ok and not same
                        record={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'path':query,'same':same,'java':java,'rust':rust}
                        output.write(json.dumps(record,separators=(',',':'))+'\n');output.flush()
            rounds+=1
            print(json.dumps({'rounds':rounds,'mismatches':mismatches,'errors':errors}),flush=True)
            remaining=end-time.monotonic()
            if remaining<=0:break
            time.sleep(min(a.period,remaining))
    if errors or mismatches:raise SystemExit(1)

if __name__=='__main__':main()
