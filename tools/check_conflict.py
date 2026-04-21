import json
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR if (SCRIPT_DIR / "logs").exists() else SCRIPT_DIR.parent
STATE_DIR = ROOT / "logs" / "state"

tokens_of_interest = [
    '96397081496122471303936256377287180706458717603115171106140126963333785784378',
    '21743669032210695168079601505378236205866986767926346409604806906483294819314',
]

for state_name in ['state_d748_2125_0369_2a12.json', 'state_d748_2125_0862_8403.json']:
    state_file = STATE_DIR / state_name
    print(f"\n=== {state_file.name} ===")
    with open(state_file, 'r', encoding='utf-8') as f:
        s = json.load(f)
    
    for tid in tokens_of_interest:
        print(f"\n  token={tid}")
        
        # my_positions
        my_pos = s.get('my_positions', [])
        found_pos = [p for p in my_pos if str(p.get('token_id') or p.get('asset_id') or '') == tid]
        if found_pos:
            p = found_pos[0]
            print(f"    POSITION size={p.get('size')} value={p.get('currentValue')} pnl={p.get('cashPnl')}")
        else:
            print(f"    NO POSITION")
        
        # accumulator
        acc = s.get('buy_notional_accumulator', {}).get(tid)
        if acc:
            print(f"    ACCUMULATOR usd={acc.get('usd')}")
        
        # topic_state
        ts = s.get('topic_state', {}).get(tid)
        if ts:
            print(f"    TOPIC_STATE phase={ts.get('phase')} peak={ts.get('target_peak')}")
        
        # ignored
        ign = s.get('ignored_tokens', {}).get(tid)
        if ign:
            print(f"    IGNORED reason={ign.get('reason')} until={ign.get('expires_at')}")
        
        # freeze
        freeze = s.get('missing_data_freeze', {}).get(tid)
        if freeze:
            print(f"    FREEZE reason={freeze.get('reason')} until={freeze.get('expires_at')}")
        
        # unfilled
        unf = s.get('topic_unfilled_attempts', {}).get(tid)
        if unf:
            print(f"    UNFILLED attempts={unf}")
        
        # seen actions
        seen = s.get('seen_action_ids', [])
        print(f"    seen_action_ids count references ~ {len([x for x in seen if tid in str(x)])}")
