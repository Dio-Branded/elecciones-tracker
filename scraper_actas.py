"""Runner que selecciona la mejor strategy disponible y descarga mesas.

Orden de preferencia (priority ascendente):
  onpe_oficial (datosabiertos.gob.pe) ← cuando exista dataset 2026
  mesa_search  (ONPE API directa)     ← activa ahora
  prime_csv    (mirror de PRIME)      ← fallback siempre disponible

Uso:
  python scraper_actas.py                          # auto, strategy disponible
  python scraper_actas.py --strategy mesa_search
  python scraper_actas.py --strategy prime_csv
  python scraper_actas.py --probe-all              # reporta estado de cada strategy
  python scraper_actas.py --list                   # lista strategies registradas
  python scraper_actas.py --strategy mesa_search --from 1 --to 1000 --concurrency 20
"""
from __future__ import annotations

import argparse
import sys

from db import get_conn
from sources._common import ActasStrategy, ID_ELECCION_PRESIDENCIAL
from sources.prime_monitor import PrimeCsvMirrorStrategy
from sources.mesa_search import MesaSearchStrategy
from sources.datosabiertos_monitor import DatosAbiertosStrategy


def registered_strategies() -> list[ActasStrategy]:
    """Orden canonico. El runner elige la de menor priority que este available."""
    return [
        DatosAbiertosStrategy(),
        MesaSearchStrategy(),
        PrimeCsvMirrorStrategy(),
    ]


def cmd_list():
    for s in sorted(registered_strategies(), key=lambda x: x.priority):
        print(f"  priority={s.priority:>3}  name={s.name}")


def cmd_probe_all():
    for s in sorted(registered_strategies(), key=lambda x: x.priority):
        print(f"\n[{s.name}] priority={s.priority}")
        r = s.probe()
        mark = "OK" if r.ok else "--"
        print(f"  [{mark}] {r.message}")
        if r.sample and r.ok:
            import json
            print(f"  sample: {json.dumps(r.sample, ensure_ascii=False)[:300]}")


def cmd_run(strategy_name: str | None, id_eleccion: int, extra_args: dict):
    strategies = registered_strategies()

    if strategy_name:
        matches = [s for s in strategies if s.name == strategy_name]
        if not matches:
            print(f"Strategy desconocida: {strategy_name}")
            print("Disponibles: " + ", ".join(s.name for s in strategies))
            sys.exit(1)
        chosen = matches[0]
    else:
        # auto: primera available por priority ascendente
        for s in sorted(strategies, key=lambda x: x.priority):
            if s.available():
                chosen = s
                print(f"[auto] seleccionada strategy: {s.name} (priority={s.priority})")
                break
        else:
            print("ERROR: ninguna strategy disponible")
            sys.exit(2)

    conn = get_conn()
    try:
        # Pasar kwargs solo si el strategy los acepta (mesa_search tiene from/to/concurrency)
        import inspect
        sig = inspect.signature(chosen.download)
        kwargs = {"id_eleccion": id_eleccion}
        for k, v in extra_args.items():
            if k in sig.parameters:
                kwargs[k] = v
        sid = chosen.download(conn, **kwargs)
        if sid is None:
            print("Strategy no produjo snapshot.")
            sys.exit(3)
        print(f"\nsnapshot_id={sid} source={chosen.name}")
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy", choices=None, default=None,
                    help="Forzar una strategy especifica (ver --list)")
    ap.add_argument("--list", action="store_true",
                    help="Listar strategies registradas")
    ap.add_argument("--probe-all", action="store_true",
                    help="Probar cada strategy y reportar disponibilidad")
    ap.add_argument("--eleccion", type=int, default=ID_ELECCION_PRESIDENCIAL)
    # Args opcionales que algunos strategies aceptan
    ap.add_argument("--from", dest="code_from", type=int, default=1)
    ap.add_argument("--to", dest="code_to", type=int, default=999999)
    ap.add_argument("--concurrency", type=int, default=20)
    ap.add_argument("--incremental", action="store_true",
                    help="Solo re-descargar codigos en estado jee/pendiente + sample contabilizadas")
    args = ap.parse_args()

    if args.list:
        cmd_list(); return
    if args.probe_all:
        cmd_probe_all(); return
    cmd_run(
        args.strategy, args.eleccion,
        {"code_from": args.code_from, "code_to": args.code_to,
         "concurrency": args.concurrency, "incremental": args.incremental},
    )


if __name__ == "__main__":
    main()
