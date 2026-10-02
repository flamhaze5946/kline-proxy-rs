#!/usr/bin/env python3
"""Convert the Java application's configuration (requires PyYAML).
Only writes the explicitly named Rust JSON file. Never edits Java files or deploys.
"""
import argparse
import json
from pathlib import Path
import yaml

def expand(raw):
    """Merge dotted and nested Spring keys without depending on sibling key order."""
    out={}
    def merge(prefix,value):
        if isinstance(value,dict) and value:
            for key,child in value.items():merge(prefix+str(key).split('.'),child)
            return
        target=out
        for part in prefix[:-1]:target=target.setdefault(part,{})
        target[prefix[-1]]=value
    for key,value in raw.items():merge(str(key).split('.'),value)
    return out

def convert(raw,java_root,listen,data_dir):
    c=expand(raw)
    def get(path,default=None):
        v=c
        for key in path.split('.'):
            if not isinstance(v,dict) or key not in v:return default
            v=v[key]
        return v
    persist=get('kline.persistence',{});enabled=persist.get('enabled',False)
    result={'listen':listen,'final_wait_ms':max(0,min(30000,int(get('kline.bulk.finalWaitMaxMs',8000)))) if get('kline.bulk.finalWaitEnabled',True) else 0,'subscriptions':[],'rest':{},'market_api':{'statistics':{}},'websocket':{}}
    result['number_type']=get('number.type','bigDecimal')
    if result['number_type'] not in ['double','float','string','bigDecimal']:
        raise ValueError('Invalid number.type')
    result['strict_readiness']=False
    result['closed_bar_latency_enabled']=bool(get('kline.diagnostics.closedBarLatencyEnabled',True))
    result['market_api']['cms_url']=get('client.binanceComposite.api.rootUrl','https://www.binance.com')
    result['rest'].update({
        'reconcile_seconds':300,
        'hour_boundary_guard_before_ms':max(0,int(get('kline.rpcSync.hourBoundaryGuardBeforeMs',150000))),
        'hour_boundary_guard_after_ms':max(0,int(get('kline.rpcSync.hourBoundaryGuardAfterMs',30000))),
    })
    retention=[]
    for market in ['future','spot']:
        for interval,sub in get(f'kline.binance.{market}.intervalSyncConfigs',{}).items():
            if not get(f'kline.binance.{market}.enabled',True):
                continue
            patterns=sub.get('listenSymbolPatterns')
            if not isinstance(patterns,list) or any(not isinstance(p,str) for p in patterns):
                raise ValueError(f'{market}.{interval}.listenSymbolPatterns must be an explicit list')
            if not patterns:
                continue
            capacity=int(sub.get('minMaintainCount',365))
            rules=persist.get(market,{}).get('intervalConfigs',{})
            rule=rules.get(interval,{})
            count=rule.get('maxStoreCount')
            disk=max(capacity,int(count) if count is not None else capacity*2)
            overrides={s:max(capacity,int(n)) for s,n in rule.get('symbolMaxStoreCounts',{}).items() if n is not None}
            result['subscriptions'].append({'market':market,'interval':interval,'history_capacity':max(capacity,disk) if enabled else capacity,'symbol_capacities':overrides if enabled else {},'symbol_patterns':patterns,'continuous':bool(sub.get('useContinuousKlineStream',False))})
            if enabled and interval in rules:retention.append({'market':market,'interval':interval,'max_store_count':disk,'symbol_counts':overrides})
        client='binanceFuture' if market=='future' else 'binanceSpot'
        default='https://fapi.binance.com' if market=='future' else 'https://api.binance.com'
        result['rest'][market+'_url']=get(f'client.{client}.api.rootUrl',default)
        result['rest'][market+'_refresh_count']=get(f'kline.binance.{market}.rpcRefreshCount',99)
        default_ws='wss://fstream.binance.com/market/ws' if market=='future' else 'wss://stream.binance.com/ws'
        ws=get(f'ws.client.{client}.url',default_ws)
        if ws.endswith('/ws'):ws=ws[:-3]+'/stream'
        result['websocket'][market+'_url']=ws
    if enabled:
        legacy=Path(persist.get('rootDir','./data/kline-cache'))
        if not legacy.is_absolute():legacy=java_root/legacy
        result['persistence']={'directory':str(data_dir),'legacy_directory':str(legacy),'load_on_startup':persist.get('loadOnStartup',True),'dump_on_shutdown':persist.get('dumpOnShutdown',True),'interval_seconds':max(1,int(persist.get('dumpIntervalSeconds',300))),'boundary_guard_before_ms':max(0,int(persist.get('boundaryGuardBeforeMs',30000))),'boundary_guard_after_ms':max(0,int(persist.get('boundaryGuardAfterMs',30000))),'retention':retention}
        result['persistence']['enabled_intervals']=[{'market':r['market'],'interval':r['interval']} for r in retention]
    result['market_api']['funding']={'publication_grace_ms':max(0,int(get('funding.publicationGraceMs',50)))}
    stats=result['market_api']['statistics']
    stats['altcoin_url']=get('client.blockChainCenter.api.rootUrl','https://www.blockchaincenter.net').rstrip('/')+'/en/altcoin-season-index'
    for rust,java,default in [('atr_symbol','binance.atr.symbol','BTCUSDT'),('atr_period','binance.atr.period',48),('start_date','altCoinIndex.startDate','2021-01-01'),('days','yama01altCoinIndex.statisticDays',30),('volume_days','yamaAltCoinIndex.quoteVolumeStatisticDays',7),('volume_rank','yama02altCoinIndex.quoteVolumeMaxRank',20)]:
        stats[rust]=get('statistic.'+java,default)
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('java_config',type=Path);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--java-root',type=Path);p.add_argument('--listen',default='127.0.0.1:1889')
    p.add_argument('--data-dir',type=Path,default=Path('../data/kline-rust'))
    a=p.parse_args()
    result=convert(yaml.safe_load(a.java_config.read_text()),a.java_root or a.java_config.resolve().parent,a.listen,a.data_dir)
    a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(result,indent=2)+'\n')
    print(f'Wrote {a.output}: {len(result["subscriptions"])} subscriptions; persistence={"persistence" in result}')
