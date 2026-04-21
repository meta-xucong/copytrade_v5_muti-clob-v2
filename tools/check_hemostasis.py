import json
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR if (SCRIPT_DIR / "logs").exists() else SCRIPT_DIR.parent
STATE_DIR = ROOT / "logs" / "state"

# Read state files to find hemostasis candidates from the latest scan
for state_name in ['state_d748_2125_0369_2a12.json', 'state_d748_2125_0862_8403.json']:
    state_file = STATE_DIR / state_name
    print(f"\n=== {state_file.name} ===")
    with open(state_file, 'r', encoding='utf-8') as f:
        s = json.load(f)
    
    my_pos = s.get('my_positions', [])
    my_by_tid = {}
    for p in my_pos:
        tid = str(p.get('token_id') or p.get('asset_id') or '')
        if tid:
            my_by_tid[tid] = {
                'title': p.get('title', ''),
                'size': p.get('size', 0),
            }
    
    # Check accumulator for tokens that may have been missed
    acc = s.get('buy_notional_accumulator', {})
    topic = s.get('topic_state', {})
    
    print(f"Positions count: {len(my_pos)}")
    print(f"Accumulator entries: {len(acc)}")
    print(f"Topic entries: {len(topic)}")
    
    # Find tokens with both position and topic_state (potential missed exits)
    missed = []
    for tid, st in topic.items():
        phase = st.get('phase', '')
        my = my_by_tid.get(tid)
        if my and phase in ('LONG', 'EXITING'):
            missed.append((tid, my['title'], my['size'], phase))
    
    if missed:
        print("Tokens with position AND active topic (potential missed exits):")
        for tid, title, size, phase in missed:
            print(f"  {tid[:25]}... {title} size={size} phase={phase}")
    
    # Tokens with position but no topic
    no_topic = []
    for tid, info in my_by_tid.items():
        if tid not in topic:
            no_topic.append((tid, info['title'], info['size']))
    
    if no_topic:
        print(f"Tokens with position but NO topic: {len(no_topic)}")
        for tid, title, size in no_topic[:10]:
            print(f"  {tid[:25]}... {title} size={size}")
