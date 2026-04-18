"""
Analizador: muestra evolución de votos entre snapshots y genera gráficos.

Uso:
  python analyze.py               # imprime resumen texto
  python analyze.py --plot        # genera PNG con evolución presidencial
  python analyze.py --tipo senadores_nacional --plot
"""
import argparse
from pathlib import Path

from db import get_conn

OUT_DIR = Path(__file__).parent / "data"


def print_summary(tipo: str = "presidencial"):
    conn = get_conn()
    snaps = list(conn.execute(
        """SELECT id, captured_at, actas_contabilizadas_pct, contabilizadas, total_actas
           FROM snapshots WHERE tipo=? ORDER BY captured_at""",
        (tipo,),
    ))
    if not snaps:
        print(f"No hay snapshots para tipo={tipo}")
        return

    print(f"=== {tipo.upper()} — {len(snaps)} snapshots ===\n")
    print(f"{'Fecha UTC':<22} {'Actas %':>8} {'Contab.':>10} {'Total':>10}")
    for sid, ts, pct, contab, total in snaps:
        print(f"{ts:<22} {pct:>8.3f} {contab:>10} {total:>10}")

    first_id = snaps[0][0]
    last_id = snaps[-1][0]

    def top(sid, n=6):
        return list(conn.execute(
            """SELECT nombre_agrupacion, nombre_candidato, total_votos_validos, pct_votos_validos
               FROM candidates WHERE snapshot_id=?
               ORDER BY pct_votos_validos DESC LIMIT ?""",
            (sid, n),
        ))

    first_top = top(first_id)
    last_top = top(last_id)
    first_pcts = {r[0]: r[3] for r in first_top}

    print(f"\n=== TOP — último snapshot vs primero ===")
    print(f"{'Agrupación':<50} {'Votos':>10} {'%':>7} {'Δ % vs inicio':>15}")
    for nombre, cand, votos, pct in last_top:
        prev = first_pcts.get(nombre)
        delta = f"{pct - prev:+.3f}" if prev is not None else "—"
        print(f"{(nombre or '')[:48]:<50} {votos:>10} {pct:>7.3f} {delta:>15}")
    conn.close()


def plot(tipo: str = "presidencial", top_n: int = 6):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("pip install matplotlib")
        return

    conn = get_conn()
    # top N agrupaciones del último snapshot
    last = conn.execute(
        """SELECT MAX(id) FROM snapshots WHERE tipo=?""", (tipo,)
    ).fetchone()[0]
    if not last:
        print(f"Sin datos para {tipo}")
        return
    top_codes = [r[0] for r in conn.execute(
        """SELECT codigo_agrupacion FROM candidates
           WHERE snapshot_id=? ORDER BY pct_votos_validos DESC LIMIT ?""",
        (last, top_n),
    )]
    code_names = dict(conn.execute(
        f"""SELECT codigo_agrupacion, nombre_agrupacion FROM candidates
            WHERE snapshot_id=? AND codigo_agrupacion IN ({','.join('?'*len(top_codes))})""",
        (last, *top_codes),
    ))

    fig, ax = plt.subplots(figsize=(12, 6))
    for code in top_codes:
        rows = list(conn.execute(
            """SELECT s.captured_at, c.pct_votos_validos, s.actas_contabilizadas_pct
               FROM candidates c JOIN snapshots s ON s.id=c.snapshot_id
               WHERE s.tipo=? AND c.codigo_agrupacion=?
               ORDER BY s.captured_at""",
            (tipo, code),
        ))
        if not rows:
            continue
        xs = [r[2] for r in rows]  # eje X: % actas contabilizadas
        ys = [r[1] for r in rows]
        label = (code_names.get(code, str(code)) or "")[:40]
        ax.plot(xs, ys, marker="o", label=label)

    ax.set_xlabel("% de actas contabilizadas")
    ax.set_ylabel("% de votos válidos")
    ax.set_title(f"Evolución — {tipo}")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=8)
    OUT_DIR.mkdir(exist_ok=True)
    out = OUT_DIR / f"evolucion_{tipo}.png"
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    print(f"Guardado {out}")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tipo", default="presidencial",
                    choices=["presidencial", "senadores_nacional", "parlamento_andino"])
    ap.add_argument("--plot", action="store_true")
    ap.add_argument("--top", type=int, default=6)
    args = ap.parse_args()
    print_summary(args.tipo)
    if args.plot:
        plot(args.tipo, args.top)
