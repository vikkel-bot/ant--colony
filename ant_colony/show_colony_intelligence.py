import json
from pathlib import Path

OUT = Path(r"C:\Trading\ANT_OUT")

EDGE_STATUS = OUT / "combined_colony_status.json"
EDGE4_SIGNAL = OUT / "edge4_signal_lite.json"
SIGNAL_MIX = OUT / "colony_signal_mix.json"
ALLOC = OUT / "alloc_targets.json"
RISK = OUT / "colony_risk_targets.json"
HEALTH = OUT / "market_health.json"


def load(p):
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def pad(v,w):
    s=str(v)
    return s[:w].ljust(w)


def main():

    edge=load(EDGE_STATUS).get("markets",{})
    e4=load(EDGE4_SIGNAL).get("markets",{})
    mix=load(SIGNAL_MIX).get("markets",{})
    alloc=load(ALLOC).get("markets",{})
    risk=load(RISK).get("markets",{})
    health=load(HEALTH).get("markets",{})

    markets=sorted(set(edge)|set(e4)|set(mix)|set(alloc)|set(risk)|set(health))

    print("")
    print("COLONY INTELLIGENCE PANEL")
    print("")

    headers=[
        ("market",10),
        ("trend",9),
        ("e3",6),
        ("e4",4),
        ("e4_bias",7),
        ("agree",9),
        ("colony",8),
        ("conf",6),
        ("alloc",6),
        ("risk",6),
        ("health",6),
        ("final",6)
    ]

    print(" ".join(pad(h,w) for h,w in headers))
    print(" ".join("-"*w for _,w in headers))

    for m in markets:

        e=edge.get(m,{}).get("edge3",{})
        e4row=e4.get(m,{})
        mixrow=mix.get(m,{})
        a=alloc.get(m,{})
        r=risk.get(m,{})
        h=health.get(m,{})

        trend=e.get("cb20_trend","")
        e3=e.get("gate","")
        e4state=e4row.get("edge4_state","")

        e4bias=e4row.get("signal_bias","")

        agree=mixrow.get("agreement","")
        colony=mixrow.get("colony_bias","")
        conf=mixrow.get("colony_confidence","")

        alloc_mult=a.get("target_size_mult","")
        risk_mult=r.get("target_size_mult","")
        health_mult=h.get("health_size_mult","")

        final=r.get("target_size_mult","")

        row=[
            m,
            trend,
            e3,
            e4state,
            e4bias,
            agree,
            colony,
            conf,
            alloc_mult,
            risk_mult,
            health_mult,
            final
        ]

        print(" ".join(pad(v,w) for v,(_,w) in zip(row,headers)))

    print("")


if __name__=="__main__":
    main()