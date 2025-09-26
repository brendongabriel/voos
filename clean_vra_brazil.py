#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
clean_vra_brazil.py
-------------------
Lê arquivos VRA em ./data (ou pasta indicada), mantém só voos com origem e destino
no Brasil (ICAO inicia com SB/SD/SN/SS/SW) e salva em CSV/JSON.

Exemplos:
    python clean_vra_brazil.py --data-dir ./data --year 2022 --out ./out --format both
    python clean_vra_brazil.py --data-dir ./data --all --out ./out --format json --ndjson --gzip

Requisitos:
    pip install pandas
"""

import argparse
import json
import re
from io import StringIO
from pathlib import Path
from typing import List, Optional
import pandas as pd

# Prefixos ICAO do Brasil
BR_PREFIX = ("SB", "SD", "SN", "SS", "SW")

# ---------- Leitura robusta ----------
def _read_text_try_encodings(p: Path) -> str:
    encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
    for enc in encodings:
        try:
            return p.read_text(encoding=enc)
        except Exception:
            continue
    return p.read_bytes().decode('utf-8', errors='ignore')

def _pd_read_json_text(txt: str, lines: bool = False) -> pd.DataFrame:
    return pd.read_json(StringIO(txt), orient='records', lines=lines)

def read_vra_file(path: Path) -> pd.DataFrame:
    # 1) tentativa direta (array / lines)
    try:
        return pd.read_json(path, orient='records')
    except Exception:
        pass
    try:
        return pd.read_json(path, lines=True)
    except Exception:
        pass

    # 2) saneamento básico
    raw = _read_text_try_encodings(path).replace('\ufeff', '').strip()
    if not raw:
        return pd.DataFrame()

    # JSON Lines (um objeto por linha)?
    if '\n{' in raw and not raw.lstrip().startswith('['):
        linhas = [ln.strip().rstrip(',') for ln in raw.splitlines() if ln.strip()]
        if all(ln.startswith('{') and ln.endswith('}') for ln in linhas):
            txt = '[' + ','.join(linhas) + ']'
            try:
                data = json.loads(txt)
                return pd.DataFrame(data)
            except Exception:
                pass
        try:
            return _pd_read_json_text(raw, lines=True)
        except Exception:
            pass

    # Objetos colados + colchetes ausentes
    fix = re.sub(r'}\s*{', '},{', raw)
    if not fix.lstrip().startswith('['):
        fix = '[' + fix
    if not fix.rstrip().endswith(']'):
        fix = fix + ']'
    fix = re.sub(r',\s*\]', ']', fix)
    try:
        data = json.loads(fix)
        return pd.DataFrame(data)
    except Exception:
        # Último recurso: extrai objetos simples
        objs = re.findall(r'\{[^{}]*\}', raw)
        if objs:
            txt = '[' + ','.join(objs) + ']'
            try:
                data = json.loads(txt)
                return pd.DataFrame(data)
            except Exception:
                pass

    print(f"[AVISO] Falha ao interpretar {path.name} como JSON")
    return pd.DataFrame()

# ---------- Normalização ----------
def normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    ren = {
        'ICAOEmpresaAérea': 'cia_icao',
        'NúmeroVoo': 'numero_voo',
        'CódigoAutorização': 'codigo_autorizacao',
        'CódigoTipoLinha': 'codigo_tipo_linha',
        'ICAOAeródromoOrigem': 'origem_icao',
        'ICAOAeródromoDestino': 'destino_icao',
        'PartidaPrevista': 'partida_prevista',
        'PartidaReal': 'partida_real',
        'ChegadaPrevista': 'chegada_prevista',
        'ChegadaReal': 'chegada_real',
        'SituaçãoVoo': 'situacao_voo',
        'CódigoJustificativa': 'codigo_justificativa',
    }
    for k, v in ren.items():
        if k in df.columns:
            df = df.rename(columns={k: v})

    # datas → datetime
    for c in ['partida_prevista', 'partida_real', 'chegada_prevista', 'chegada_real']:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors='coerce')

    # campos derivados
    if {'origem_icao','destino_icao'}.issubset(df.columns):
        df['rota'] = df['origem_icao'].astype(str) + '-' + df['destino_icao'].astype(str)
    if 'partida_prevista' in df.columns:
        df['ano'] = df['partida_prevista'].dt.year
        df['mes'] = df['partida_prevista'].dt.month

    return df

# ---------- Seleção de arquivos ----------
def files_for_year(data_dir: Path, year: int) -> List[Path]:
    pats = [f"VRA_{year}*", f"VRA{year}*", f"VRA-{year}*"]
    found = []
    for pat in pats:
        found.extend([p for p in data_dir.glob(pat) if p.is_file()])

    def _is_monthy(name: str) -> bool:
        m = re.search(rf"{year}(\d{{1,2}})", name)
        if not m:
            return False
        mo = int(m.group(1))
        return 1 <= mo <= 12

    found = [p for p in found if _is_monthy(p.name)]
    # ordena e deduplica
    found = sorted(found, key=lambda x: x.name)
    uniq, seen = [], set()
    for p in found:
        if p.name not in seen:
            uniq.append(p); seen.add(p.name)
    return uniq

def files_all(data_dir: Path) -> List[Path]:
    return [p for p in sorted(data_dir.glob("VRA*"), key=lambda x: x.name) if p.is_file()]

# ---------- Filtro Brasil ----------
def filter_brazil(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    ok_origem = df['origem_icao'].astype(str).str.upper().str.startswith(BR_PREFIX) if 'origem_icao' in df else False
    ok_dest   = df['destino_icao'].astype(str).str.upper().str.startswith(BR_PREFIX) if 'destino_icao' in df else False
    mask = ok_origem & ok_dest   # Brasil-Brasil; troque para "|" se quiser "Brasil em pelo menos um lado"
    return df[mask].copy()

# ---------- Salvando saídas ----------
def save_outputs(df: pd.DataFrame, out_dir: Path, basename: str, fmt: str, ndjson: bool, gzip: bool) -> None:
    """
    fmt: 'csv' | 'json' | 'both'
    ndjson: se True e fmt inclui json, salva em linhas (um objeto por linha)
    gzip: se True, adiciona .gz
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    want_csv = fmt in ('csv', 'both')
    want_json = fmt in ('json', 'both')

    if want_csv:
        csv_path = out_dir / f"{basename}.csv"
        if gzip:
            csv_path = csv_path.with_suffix(csv_path.suffix + ".gz")
        df.to_csv(csv_path, index=False, encoding="utf-8")
        print(f"CSV salvo: {csv_path}")

    if want_json:
        if ndjson:
            json_path = out_dir / f"{basename}.ndjson"
            if gzip:
                json_path = json_path.with_suffix(json_path.suffix + ".gz")
            # NDJSON: um objeto por linha
            df.to_json(json_path, orient="records", lines=True, force_ascii=False, date_format="iso", compression="infer")
        else:
            json_path = out_dir / f"{basename}.json"
            if gzip:
                json_path = json_path.with_suffix(json_path.suffix + ".gz")
            # JSON "normal": array de objetos
            df.to_json(json_path, orient="records", force_ascii=False, date_format="iso", indent=2, compression="infer")
        print(f"JSON salvo: {json_path}")

# ---------- CLI principal ----------
def main():
    ap = argparse.ArgumentParser(description="Filtra VRA (Brasil-Brasil: SB/SD/SN/SS/SW) e salva CSV/JSON.")
    ap.add_argument("--data-dir", default="./data", help="pasta com arquivos VRA_*")
    ap.add_argument("--year", type=int, help="ano alvo (ex.: 2022)")
    ap.add_argument("--all", action="store_true", help="varrer todos os arquivos VRA* (ignora --year)")
    ap.add_argument("--out", default="./out", help="pasta de saída")
    ap.add_argument("--format", choices=["csv","json","both"], default="both", help="formato de saída")
    ap.add_argument("--ndjson", action="store_true", help="salvar JSON em NDJSON (um objeto por linha)")
    ap.add_argument("--gzip", action="store_true", help="comprimir arquivos (gera .gz)")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    out_base = Path(args.out)

    if not data_dir.exists():
        print(f"Diretório não encontrado: {data_dir}")
        return

    if args.all:
        arquivos = files_all(data_dir)
        out_dir = out_base / "BR_ALL"
        base = "voos_BR_ALL"
        label = "TODOS"
    elif args.year:
        arquivos = files_for_year(data_dir, args.year)
        out_dir = out_base / f"BR_{args.year}"
        base = f"voos_BR_{args.year}"
        label = str(args.year)
    else:
        print("Informe --year YYYY ou use --all.")
        return

    if not arquivos:
        print(f"Nenhum arquivo VRA encontrado para {label} em {data_dir}")
        return

    print(f"Encontrados {len(arquivos)} arquivo(s) para {label}. Lendo e filtrando Brasil...")
    dfs = []
    for p in arquivos:
        df = read_vra_file(p)
        if df.empty:
            print(f"  - {p.name}: vazio/ilegível")
            continue
        df = normalize(df)
        df_br = filter_brazil(df)
        print(f"  - {p.name}: {len(df)} registros | Brasil: {len(df_br)}")
        if not df_br.empty:
            dfs.append(df_br)

    if not dfs:
        print("Nenhum registro elegível (Brasil-Brasil) encontrado.")
        return

    df_all = pd.concat(dfs, ignore_index=True)

    # salva conforme flags
    save_outputs(df_all, out_dir, base, fmt=args.format, ndjson=args.ndjson, gzip=args.gzip)
    print(f"✅ Pronto! Linhas salvas: {len(df_all)}")

if __name__ == "__main__":
    main()
