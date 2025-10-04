#!/usr/bin/env python3
"""
Auditoría de títulos del catálogo.

Lee un CSV/XLSX, detecta patrones de referencias y medidas en DESCRIPCION,
y genera:
- Resumen por TIPO con frecuencias de patrones
- Muestra de 100 ejemplos antes/después

Uso:
  python scripts/audit_titles.py web/uploads/catalog-current.csv
"""
from __future__ import annotations

import re
import sys
import datetime as dt
from pathlib import Path
from collections import Counter, defaultdict

import pandas as pd  # type: ignore

sys.path.append(str(Path(__file__).resolve().parents[1]))
from utils.helpers import clean_value, get_base_reference  # type: ignore


REF_HINT_WORDS = r"(?:ref\.?|referencia|modelo|codigo|c[oó]digo|sku)\s*[:#-]?\s*"
PAT_REF_TOKEN = re.compile(rf"\b{REF_HINT_WORDS}?([A-Za-z0-9]+[A-Za-z0-9\-_/]{{2,}})\b")

NUM = r"\d+(?:[.,]\d+)?"
UNIT = r"(?:mm|cm|g|gr|gramos?)"
DIM_SEP = r"[x×X]"
PAT_DIM = re.compile(rf"\b{NUM}\s*{DIM_SEP}\s*{NUM}(?:\s*{UNIT})?\b", re.IGNORECASE)
PAT_NUM_UNIT = re.compile(rf"\b{NUM}\s*{UNIT}\b", re.IGNORECASE)
PAT_DIAM = re.compile(rf"(?:Ø|diam(?:etro|[ée]tro)?)\s*{NUM}\s*(?:mm|cm)?", re.IGNORECASE)
PAT_LENGTH = re.compile(rf"(?:longitud|largo|l\.)\s*{NUM}\s*(?:cm|mm)?\b", re.IGNORECASE)
PAT_SIZE = re.compile(rf"(?:talla|t\.?|n[º°]|num\.?|nro\.?|numero)\s*{NUM}\b", re.IGNORECASE)
PAT_PURITY = re.compile(r"\b(?:(?:18|9)\s?k|750|375|kt\.?|quilates?)\b", re.IGNORECASE)


def load_df(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(path)
    raise RuntimeError(f"Formato no soportado: {path.suffix}")


def propose_clean(title: str, ref: str) -> tuple[str, list[str]]:
    removed: list[str] = []
    t = clean_value(title)
    if not t:
        return t, removed
    # quitar pureza
    def _rm(pat: re.Pattern):
        nonlocal t
        for m in list(pat.finditer(t)):
            removed.append(m.group(0))
        t = pat.sub(" ", t)

    _rm(PAT_PURITY)
    # quitar referencias explícitas y tokens parecidos a la base
    base = clean_value(get_base_reference(clean_value(ref)))
    if base:
        # variantes comunes con separadores
        variants = {base, base.replace("-", ""), base.replace(" ", ""), base.replace("/", ""), base.upper(), base.lower()}
        for v in variants:
            if not v:
                continue
            if v in t:
                removed.append(v)
                t = t.replace(v, " ")
    # pistas de referencia con hint words
    for m in list(PAT_REF_TOKEN.finditer(t)):
        tok = m.group(1)
        # Evitar eliminar palabras cortas no alfanuméricas
        if len(tok) >= 4 and re.search(r"\d", tok):
            removed.append(m.group(0))
            t = t.replace(m.group(0), " ")

    # medidas
    for pat in (PAT_DIM, PAT_DIAM, PAT_LENGTH, PAT_NUM_UNIT, PAT_SIZE):
        for m in list(pat.finditer(t)):
            removed.append(m.group(0))
        t = pat.sub(" ", t)

    # normalizar espacios
    t = re.sub(r"\s+", " ", t).strip()
    # capitalización ligera (primera en mayúscula, resto como están en minúsculas genéricas)
    if t:
        t = t[0].upper() + t[1:]
    return t, removed


def main():
    if len(sys.argv) < 2:
        print("Uso: python scripts/audit_titles.py <ruta_csv>")
        sys.exit(1)
    path = Path(sys.argv[1])
    df = load_df(path)
    df.columns = [str(c).strip().upper() for c in df.columns]
    if "DESCRIPCION" not in df.columns or "REFERENCIA" not in df.columns:
        print("El CSV debe incluir columnas DESCRIPCION y REFERENCIA")
        sys.exit(2)

    # límites de muestra por tipo
    tipo_col = "TIPO" if "TIPO" in df.columns else None

    ref_hits = Counter()
    measure_hits = Counter()
    purity_hits = Counter()
    by_tipo = defaultdict(lambda: {"rows": 0, "ref": 0, "measure": 0})
    examples: list[dict] = []

    for _, row in df.iterrows():
        desc = clean_value(row.get("DESCRIPCION", ""))
        ref = clean_value(row.get("REFERENCIA", ""))
        if not desc:
            continue

        # contadores
        if PAT_PURITY.search(desc):
            purity_hits.update([PAT_PURITY.search(desc).group(0).lower()])
        if PAT_REF_TOKEN.search(desc):
            ref_hits.update(["hint_ref"])
        if get_base_reference(ref) in desc:
            ref_hits.update(["base_in_title"])
        if any(p.search(desc) for p in (PAT_DIM, PAT_DIAM, PAT_LENGTH, PAT_NUM_UNIT, PAT_SIZE)):
            measure_hits.update(["measure"])

        new_title, removed = propose_clean(desc, ref)
        if removed:
            examples.append({
                "REFERENCIA": ref,
                "TIPO": clean_value(row.get("TIPO", "")),
                "ORIGINAL": desc,
                "LIMPIO": new_title,
                "ELIMINADO": "; ".join(removed),
            })
        tp = clean_value(row.get("TIPO", "")) if tipo_col else "(sin tipo)"
        by_tipo[tp]["rows"] += 1
        by_tipo[tp]["ref"] += int("base_in_title" in ref_hits or "hint_ref" in ref_hits)
        by_tipo[tp]["measure"] += int("measure" in measure_hits)

    # guardar ejemplos
    outdir = Path("data/audit")
    outdir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_csv = outdir / f"titles_examples-{ts}.csv"
    pd.DataFrame(examples[:100]).to_csv(out_csv, index=False)

    # imprimir resumen
    total = len(df)
    print("=== Auditoría de títulos ===")
    print(f"Archivo: {path}")
    print(f"Filas: {total}")
    print("")
    print("-- Indicadores globales --")
    print(f"Con pistas de referencia: {ref_hits['hint_ref'] + ref_hits['base_in_title']}")
    print(f"Con medidas: {measure_hits['measure']}")
    print(f"Menciones de pureza (ej.): {sum(purity_hits.values())}")
    if purity_hits:
        common = ", ".join([f"{k}:{v}" for k, v in purity_hits.most_common(8)])
        print(f"Top purezas: {common}")
    print("")
    print("-- Por TIPO (muestra) --")
    for tp, stats in list(by_tipo.items())[:20]:
        print(f"{tp or '(sin tipo)'} -> filas:{stats['rows']}")
    print("")
    print(f"Ejemplos guardados: {out_csv}")


if __name__ == "__main__":
    main()

