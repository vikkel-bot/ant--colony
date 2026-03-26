import json
from datetime import datetime, timezone
from pathlib import Path

OUT_DIR = Path(r"C:\Trading\ANT_OUT")

EDGE_STATUS = OUT_DIR / "combined_colony_status.json"
EDGE4_SIGNAL = OUT_DIR / "edge4_signal_lite.json"

OUT_JSON = OUT_DIR / "colony_signal_mix.json"
OUT_TSV = OUT_DIR / "colony_signal_mix.tsv"


def utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load(path):
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def trend_to_bias(trend):
    trend=str(trend).upper()
    if trend=="BULL":
        return "LONG"
    if trend=="BEAR":
        return "SHORT"
    return "NEUTRAL"


def agreement(a,b):
    if a==b and a!="NEUTRAL":
        return "AGREE"
    if a!="NEUTRAL" and b!="NEUTRAL" and a!=b:
        return "DISAGREE"
    return "NONE"


def colony_bias(e3,e4,agree):
    if agree=="AGREE":
        return e3
    if agree=="DISAGREE":
        return "NEUTRAL"
    if e3=="NEUTRAL":
        return e4
    if e4=="NEUTRAL":
        return e3
    return "NEUTRAL"


def confidence(agree,strength):
    if agree=="AGREE":
        return round(0.75 + strength*0.25,6)
    if agree=="DISAGREE":
        return 0.25
    return 0.5


def main():

    edge_doc=load(EDGE_STATUS)
    edge4_doc=load(EDGE4_SIGNAL)

    edge_markets=edge_doc.get("markets",{})
    edge4_markets=edge4_doc.get("markets",{})

    markets=sorted(set(edge_markets)|set(edge4_markets))

    rows=[]
    out={}

    for m in markets:

        e3=edge_markets.get(m,{})
        e4=edge4_markets.get(m,{})

        trend=e3.get("edge3",{}).get("cb20_trend","")
        gate=e3.get("edge3",{}).get("gate","")

        e3_bias=trend_to_bias(trend)

        e4_bias=e4.get("signal_bias","NEUTRAL")
        strength=float(e4.get("signal_strength",0))

        agree=agreement(e3_bias,e4_bias)

        colony=colony_bias(e3_bias,e4_bias,agree)

        conf=confidence(agree,strength)

        row={
            "market":m,
            "edge3_trend":trend,
            "edge3_gate":gate,
            "edge4_bias":e4_bias,
            "edge4_strength":strength,
            "agreement":agree,
            "colony_bias":colony,
            "colony_confidence":conf
        }

        rows.append(row)
        out[m]=row

    doc={
        "ts_utc":utc(),
        "component":"colony_signal_mix",
        "market_count":len(rows),
        "markets":out
    }

    OUT_JSON.write_text(json.dumps(doc,indent=2),encoding="utf-8")

    headers=list(rows[0].keys()) if rows else []
    lines=["\t".join(headers)]

    for r in rows:
        lines.append("\t".join(str(r[h]) for h in headers))

    OUT_TSV.write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("WROTE",OUT_JSON)
    print("WROTE",OUT_TSV)


if __name__=="__main__":
    main()