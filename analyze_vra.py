import argparse
from pathlib import Path
import pandas as pd
from io import StringIO
import json
import re

# ============ Leitura flexível (CSV/JSON/NDJSON ou diretório) ============

def read_any(path: Path) -> pd.DataFrame:
    """Lê CSV/JSON/NDJSON. Se path for diretório, concatena arquivos suportados."""
    if path.is_dir():
        dfs = []
        for p in sorted(path.glob("*")):
            if p.suffix.lower() in (".csv", ".json", ".ndjson", ".gz"):
                try:
                    dfs.append(read_any_file(p))
                    print(f"[OK] Lido: {p.name}")
                except Exception as e:
                    print(f"[AVISO] Falha ao ler {p.name}: {e}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    else:
        return read_any_file(path)

def read_any_file(p: Path) -> pd.DataFrame:
    name = p.name.lower()
    if name.endswith(".csv") or name.endswith(".csv.gz"):
        return pd.read_csv(p)
    if name.endswith(".ndjson") or name.endswith(".ndjson.gz"):
        return pd.read_json(p, orient="records", lines=True, compression="infer")
    if name.endswith(".json") or name.endswith(".json.gz"):
        # tenta array; cai para lines se precisar
        try:
            return pd.read_json(p, orient="records", compression="infer")
        except Exception:
            return pd.read_json(p, orient="records", lines=True, compression="infer")
    # fallback
    try:
        return pd.read_csv(p)
    except Exception:
        return pd.read_json(p, orient="records", lines=True)

# ============ Normalização & features ============

def normalize(df: pd.DataFrame) -> pd.DataFrame:
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

    # atrasos
    if {'partida_prevista','partida_real'}.issubset(df.columns):
        df['atraso_partida_min'] = (df['partida_real'] - df['partida_prevista']).dt.total_seconds() / 60.0
    if {'chegada_prevista','chegada_real'}.issubset(df.columns):
        df['atraso_chegada_min'] = (df['chegada_real'] - df['chegada_prevista']).dt.total_seconds() / 60.0

    # rota
    if {'origem_icao','destino_icao'}.issubset(df.columns):
        df['rota'] = df['origem_icao'].astype(str).str.upper() + '-' + df['destino_icao'].astype(str).str.upper()

    # ---------- FEATURES TEMPORAIS COM FALLBACK ----------
    # timestamp de referência: partida_prevista -> chegada_prevista -> partida_real -> chegada_real
    ref = None
    for c in ['partida_prevista','chegada_prevista','partida_real','chegada_real']:
        if c in df.columns:
            ref = df[c] if ref is None else ref.fillna(df[c])
    if ref is not None:
        ref = pd.to_datetime(ref, errors='coerce')
        df['ano']  = ref.dt.year
        df['mes']  = ref.dt.month
        df['hora'] = ref.dt.hour
        df['dow']  = ref.dt.dayofweek  # 0=Seg..6=Dom

    return df

def build_delay_flags(df: pd.DataFrame, on_time_min: int) -> pd.DataFrame:
    """Atraso = chegada quando disponível; senão, partida. is_delayed = delay_min > on_time_min."""
    df = df.copy()
    ac = pd.to_numeric(df.get('atraso_chegada_min'), errors='coerce')
    ap = pd.to_numeric(df.get('atraso_partida_min'), errors='coerce')
    df['delay_min'] = ac.where(~ac.isna(), ap)
    df['is_delayed'] = df['delay_min'] > float(on_time_min)
    return df

# ============ Gráficos helpers ============

try:
    import matplotlib.pyplot as plt
except Exception:
    plt = None

DOW_LABELS = {0:'Seg',1:'Ter',2:'Qua',3:'Qui',4:'Sex',5:'Sáb',6:'Dom'}
PERIODO_ORDER = ['madrugada','manhã','tarde','noite']

def reindex_if_possible(s: pd.Series, order):
    idx = [k for k in order if k in s.index]
    return s.reindex(idx) if idx else s

def series_not_empty(s: pd.Series) -> bool:
    return s is not None and not s.dropna().empty

def safe_bar_or_line(s: pd.Series, title: str, xlabel: str, ylabel: str, out_png: Path, kind='bar'):
    if plt is None:
        print(f"[AVISO] matplotlib não disponível: {out_png.name}")
        return
    if s is None or s.dropna().empty:
        print(f"[AVISO] Série vazia, gráfico não gerado: {out_png.name} — {title}")
        return
    fig = plt.figure()
    (s.plot(kind=kind) if kind in ('bar','line') else s.plot())
    plt.title(title); plt.xlabel(xlabel); plt.ylabel(ylabel)
    fig.tight_layout(); fig.savefig(out_png, dpi=120); plt.close(fig)

# ============ Perguntas / Lógicas ============

def airport_with_most_delays(df: pd.DataFrame) -> dict:
    """Contagem de atrasos por DESTINO (chegada) e ORIGEM (partida)."""
    out = {}
    if {'destino_icao','is_delayed'}.issubset(df.columns):
        out['destino_top'] = df.groupby('destino_icao')['is_delayed'].sum().sort_values(ascending=False).to_dict()
    if {'origem_icao','is_delayed'}.issubset(df.columns):
        out['origem_top'] = df.groupby('origem_icao')['is_delayed'].sum().sort_values(ascending=False).to_dict()
    return out

def airport_increase_decrease(df: pd.DataFrame) -> dict:
    """Δ atrasos (último ano − primeiro ano) por destino e por origem."""
    out = {}
    if {'destino_icao','ano','is_delayed'}.issubset(df.columns):
        g = df.groupby(['destino_icao','ano'])['is_delayed'].sum().unstack('ano').fillna(0)
        if not g.empty:
            diff = g.iloc[:, -1] - g.iloc[:, 0]
            out['destino_delta'] = diff.to_dict()
    if {'origem_icao','ano','is_delayed'}.issubset(df.columns):
        g2 = df.groupby(['origem_icao','ano'])['is_delayed'].sum().unstack('ano').fillna(0)
        if not g2.empty:
            diff2 = g2.iloc[:, -1] - g2.iloc[:, 0]
            out['origem_delta'] = diff2.to_dict()
    return out

def delays_trend(df: pd.DataFrame) -> pd.DataFrame:
    """Taxa de atraso mensal ao longo do tempo (chegada/fallback partida)."""
    if 'partida_prevista' not in df.columns:
        return pd.DataFrame()
    d = df.copy()
    d['ano_mes'] = d['partida_prevista'].dt.to_period('M').dt.to_timestamp()
    return d.groupby('ano_mes')['is_delayed'].mean().to_frame('taxa_atraso')

def weekday_blocks(df: pd.DataFrame):
    """Retorna taxa e contagem por ano/dow (para fallback)."""
    if not {'ano','dow','is_delayed'}.issubset(df.columns):
        return None, None
    taxa = df.groupby(['ano','dow'])['is_delayed'].mean().unstack('ano')   # index=dow
    cont = df.groupby(['ano','dow'])['is_delayed'].sum().unstack('ano')
    return taxa, cont

def period_blocks(df: pd.DataFrame):
    """Retorna taxa e contagem por ano/período do dia (para fallback)."""
    if not {'ano','hora','is_delayed'}.issubset(df.columns):
        return None, None
    d = df.copy()
    def _periodo(h):
        if pd.isna(h): return None
        h = int(h)
        if 0 <= h <= 5:   return 'madrugada'
        if 6 <= h <= 11:  return 'manhã'
        if 12 <= h <= 17: return 'tarde'
        return 'noite'
    d['periodo'] = d['hora'].apply(_periodo)
    taxa = d.groupby(['ano','periodo'])['is_delayed'].mean().unstack('ano')  # index=periodo
    cont = d.groupby(['ano','periodo'])['is_delayed'].sum().unstack('ano')
    return taxa, cont

def airline_by_year_tables(df: pd.DataFrame, min_count_airline: int):
    """Gera tabelas por ano/cia com TAXA (com mínimo) e permite fallback para CONTAGEM."""
    if not {'ano','cia_icao','is_delayed'}.issubset(df.columns):
        return None
    g = df.groupby(['ano','cia_icao'])['is_delayed'].agg(['mean','count'])
    g_taxa = g[g['count'] >= min_count_airline].copy()
    if not g_taxa.empty:
        g_taxa['rank'] = g_taxa.groupby(level=0)['mean'].rank(ascending=False, method='dense')
    return g_taxa

# ============ Relatório (gera tabelas + gráficos + markdown) ============

def make_report(df: pd.DataFrame, out_dir: Path, on_time_min: int, min_count_airline: int):
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir/'charts').mkdir(exist_ok=True)
    (out_dir/'tables').mkdir(exist_ok=True)

    # 1) Aeroportos com mais atrasos (contagem)
    ap = airport_with_most_delays(df)
    if ap.get('destino_top'):
        s = pd.Series(ap['destino_top'])
        s.to_csv(out_dir/'tables'/'airport_destino_mais_atrasos.csv', header=['atrasos'])
        safe_bar_or_line(s.head(20), "Aeroportos (DESTINO) com mais atrasos (contagem)",
                         "Aeroporto destino (ICAO)", "atrasos", out_dir/'charts'/'aeroportos_destino_mais_atrasos.png', kind='bar')
    if ap.get('origem_top'):
        s2 = pd.Series(ap['origem_top'])
        s2.to_csv(out_dir/'tables'/'airport_origem_mais_atrasos.csv', header=['atrasos'])
        safe_bar_or_line(s2.head(20), "Aeroportos (ORIGEM) com mais atrasos (contagem)",
                         "Aeroporto origem (ICAO)", "atrasos", out_dir/'charts'/'aeroportos_origem_mais_atrasos.png', kind='bar')

    # 2) Aeroporto que aumentou/diminuiu (Δ = último ano − primeiro ano)
    anos_disponiveis = sorted(set(df['ano'].dropna().astype(int))) if 'ano' in df.columns else []
    inc = airport_increase_decrease(df)

    def plot_variacao(dic, titulo, png_name, top=20):
        if not dic: 
            return
        s = pd.Series(dic)
        if s.dropna().empty:
            return
        s_pos = s.sort_values(ascending=False).head(top)
        s_neg = s.sort_values(ascending=True).head(top)
        safe_bar_or_line(s_pos, f"{titulo} — maiores aumentos (Δ)", "Aeroporto (ICAO)", "Δ atrasos (últ-prim)",
                        out_dir/'charts'/f'{png_name}_aumentos.png', kind='bar')
        safe_bar_or_line(s_neg, f"{titulo} — maiores reduções (Δ)", "Aeroporto (ICAO)", "Δ atrasos (últ-prim)",
                        out_dir/'charts'/f'{png_name}_reducoes.png', kind='bar')

    if len(anos_disponiveis) >= 2:
        if inc.get('destino_delta'):
            pd.Series(inc['destino_delta']).to_csv(out_dir/'tables'/'aeroporto_destino_variacao_atrasos.csv', header=['delta_atrasos'])
            plot_variacao(inc['destino_delta'], "Destino: variação de atrasos", "variacao_destino")
        if inc.get('origem_delta'):
            pd.Series(inc['origem_delta']).to_csv(out_dir/'tables'/'aeroporto_origem_variacao_atrasos.csv', header=['delta_atrasos'])
            plot_variacao(inc['origem_delta'], "Origem: variação de atrasos", "variacao_origem")
    else:
        # Fallback: só 1 ano → ranking simples por contagem, destino e origem
        print("[INFO] Seção (2): apenas 1 ano encontrado — gerando ranking por contagem (sem Δ).")
        if {'destino_icao','is_delayed','ano'}.issubset(df.columns):
            ano = int(anos_disponiveis[0]) if anos_disponiveis else None
            s = df[df['ano']==ano].groupby('destino_icao')['is_delayed'].sum().sort_values(ascending=False).head(20)
            safe_bar_or_line(s, f"Destino: aeroportos com mais atrasos — {ano}", "Aeroporto (ICAO)", "atrasos",
                            out_dir/'charts'/f'variacao_destino_sem_delta_{ano}.png', kind='bar')
        if {'origem_icao','is_delayed','ano'}.issubset(df.columns):
            ano = int(anos_disponiveis[0]) if anos_disponiveis else None
            s2 = df[df['ano']==ano].groupby('origem_icao')['is_delayed'].sum().sort_values(ascending=False).head(20)
            safe_bar_or_line(s2, f"Origem: aeroportos com mais atrasos — {ano}", "Aeroporto (ICAO)", "atrasos",
                            out_dir/'charts'/f'variacao_origem_sem_delta_{ano}.png', kind='bar')

    # 3) Tendência geral (taxa mensal)
    trend = delays_trend(df)
    if not trend.empty:
        trend.to_csv(out_dir/'tables'/'tendencia_mensal_taxa_atraso.csv')
        safe_bar_or_line(trend['taxa_atraso'], "Tendência mensal da taxa de atraso", "tempo", "taxa de atraso",
                         out_dir/'charts'/'tendencia_mensal_taxa.png', kind='line')
    else:
        print("[AVISO] Tendência mensal: sem dados suficientes.")

    # 4) Dias da semana com mais atrasos (por ano) — TAXA; fallback CONTAGEM, ordem Seg..Dom
    taxa_dow, cont_dow = weekday_blocks(df)
    if taxa_dow is not None or cont_dow is not None:
        if taxa_dow is not None and not taxa_dow.dropna(how='all').empty:
            taxa_dow.rename(index=DOW_LABELS).to_csv(out_dir/'tables'/'dias_semana_taxa_atraso_por_ano.csv')
        if cont_dow is not None and not cont_dow.dropna(how='all').empty:
            cont_dow.rename(index=DOW_LABELS).to_csv(out_dir/'tables'/'dias_semana_contagem_atraso_por_ano.csv')

        anos = sorted(set(df['ano'].dropna().astype(int))) if 'ano' in df.columns else []
        for ano in anos:
            s_taxa = taxa_dow[ano] if (taxa_dow is not None and ano in taxa_dow.columns) else None
            s_cont = cont_dow[ano] if (cont_dow is not None and ano in cont_dow.columns) else None

            s_plot = None; rotulo = ""
            if s_taxa is not None and not s_taxa.dropna().empty:
                s_plot = s_taxa.copy()
                s_plot.index = [DOW_LABELS.get(int(i), str(i)) for i in s_plot.index]
                s_plot = reindex_if_possible(s_plot, ['Seg','Ter','Qua','Qui','Sex','Sáb','Dom'])
                rotulo = "taxa de atraso"
            elif s_cont is not None and not s_cont.dropna().empty:
                s_plot = s_cont.copy()
                s_plot.index = [DOW_LABELS.get(int(i), str(i)) for i in s_plot.index]
                s_plot = reindex_if_possible(s_plot, ['Seg','Ter','Qua','Qui','Sex','Sáb','Dom'])
                rotulo = "contagem de atrasos"

            if s_plot is not None and not s_plot.dropna().empty:
                safe_bar_or_line(s_plot, f"Dias da semana com mais atrasos - {ano} ({rotulo})",
                                "dia da semana", rotulo, out_dir/'charts'/f'dow_{ano}.png', kind='bar')
            else:
                print(f"[AVISO] (4) Sem dados para dias da semana em {ano}.")
    else:
        print("[AVISO] (4) Colunas ausentes: preciso de {'ano','dow','is_delayed'}.")

    # 5) Período do dia com mais atrasos (por ano) — TAXA; fallback CONTAGEM, ordem fixa
    taxa_per, cont_per = period_blocks(df)
    if taxa_per is not None or cont_per is not None:
        if taxa_per is not None and not taxa_per.dropna(how='all').empty:
            taxa_per.to_csv(out_dir/'tables'/'periodo_dia_taxa_atraso_por_ano.csv')
        if cont_per is not None and not cont_per.dropna(how='all').empty:
            cont_per.to_csv(out_dir/'tables'/'periodo_dia_contagem_atraso_por_ano.csv')

        anos = sorted(set(df['ano'].dropna().astype(int))) if 'ano' in df.columns else []
        for ano in anos:
            s_taxa = taxa_per[ano] if (taxa_per is not None and ano in taxa_per.columns) else None
            s_cont = cont_per[ano] if (cont_per is not None and ano in cont_per.columns) else None

            s_plot = None; rotulo = ""
            if s_taxa is not None and not s_taxa.dropna().empty:
                s_plot = reindex_if_possible(s_taxa, PERIODO_ORDER)
                rotulo = "taxa de atraso"
            elif s_cont is not None and not s_cont.dropna().empty:
                s_plot = reindex_if_possible(s_cont, PERIODO_ORDER)
                rotulo = "contagem de atrasos"

            if s_plot is not None and not s_plot.dropna().empty:
                safe_bar_or_line(s_plot, f"Período do dia com mais atrasos - {ano} ({rotulo})",
                                "período", rotulo, out_dir/'charts'/f'periodo_{ano}.png', kind='bar')
            else:
                print(f"[AVISO] (5) Sem dados para período do dia em {ano}.")
    else:
        print("[AVISO] (5) Colunas ausentes: preciso de {'ano','hora','is_delayed'}.")

    # 6) Companhia que mais atrasa (por ano) — TAXA (n≥mínimo); fallback CONTAGEM
    if {'ano','cia_icao','is_delayed'}.issubset(df.columns):
        g_taxa = airline_by_year_tables(df, min_count_airline=min_count_airline)
        if g_taxa is not None and not g_taxa.empty:
            g_taxa.round(4).to_csv(out_dir/'tables'/'companhias_taxa_atraso_por_ano.csv')
        else:
            print(f"[AVISO] (6) Nenhuma cia atingiu n≥{min_count_airline} para ranking por TAXA; tentando CONTAGEM.")

        anos = sorted(set(df['ano'].dropna().astype(int))) if 'ano' in df.columns else []
        for ano in anos:
            s_plot = None; rotulo = ""
            if g_taxa is not None and not g_taxa.empty and ano in g_taxa.index.get_level_values(0):
                top_taxa = g_taxa.xs(ano).sort_values('mean', ascending=False).head(10)['mean']
                if not top_taxa.dropna().empty:
                    s_plot = top_taxa; rotulo = f"taxa de atraso (n≥{min_count_airline})"

            if s_plot is None:
                g_cont = df[df['ano']==ano].groupby('cia_icao')['is_delayed'].sum().sort_values(ascending=False).head(10)
                if not g_cont.dropna().empty:
                    s_plot = g_cont; rotulo = "contagem de atrasos (fallback)"

            if s_plot is not None:
                safe_bar_or_line(s_plot, f"Companhias com mais atrasos - {ano} ({rotulo})",
                                "CIA", rotulo, out_dir/'charts'/f'cias_{ano}.png', kind='bar')
            else:
                print(f"[AVISO] (6) Sem dados suficientes para cias em {ano}.")
    else:
        print("[AVISO] (6) Colunas ausentes: preciso de {'ano','cia_icao','is_delayed'}.")

    # ========== Escreve report.md ==========
    lines = []
    lines.append("# Relatório de Atrasos (dados tratados)\n")
    lines.append(f"- Regra de pontualidade: atraso > **{on_time_min} min** (chegada preferencial; partida como fallback).")
    lines.append("")
    # Q1
    lines.append("## 1) Qual o aeroporto que tem mais atrasos no geral?")
    if (out_dir/'charts'/'aeroportos_destino_mais_atrasos.png').exists():
        lines.append("### Por destino (chegada)")
        lines.append("![Aeroportos destino com mais atrasos](charts/aeroportos_destino_mais_atrasos.png)\n")
    if (out_dir/'charts'/'aeroportos_origem_mais_atrasos.png').exists():
        lines.append("### Por origem (partida)")
        lines.append("![Aeroportos origem com mais atrasos](charts/aeroportos_origem_mais_atrasos.png)\n")
    # Q2
    lines.append("## 2) Qual aeroporto aumentou e qual diminuiu o número de atrasos?")
    lines.append("Gráficos: `charts/variacao_destino_aumentos.png`, `charts/variacao_destino_reducoes.png`, `charts/variacao_origem_aumentos.png`, `charts/variacao_origem_reducoes.png`.")
    lines.append("Tabelas: `tables/aeroporto_destino_variacao_atrasos.csv`, `tables/aeroporto_origem_variacao_atrasos.csv` (Δ = último ano − primeiro ano).\n")
    # Q3
    lines.append("## 3) Os atrasos aumentaram ou diminuíram no período?")
    if (out_dir/'charts'/'tendencia_mensal_taxa.png').exists():
        lines.append("![Tendência da taxa de atraso](charts/tendencia_mensal_taxa.png)\n")
    # Q4
    lines.append("## 4) Dias da semana com mais atrasos (a cada ano)")
    lines.append("Arquivos: `charts/dow_<ANO>.png` (taxa de atraso; se indisponível, contagem). 0=Seg … 6=Dom.\n")
    # Q5
    lines.append("## 5) Período do dia com mais atrasos (a cada ano)")
    lines.append("Arquivos: `charts/periodo_<ANO>.png` (madrugada, manhã, tarde, noite; taxa de atraso ou contagem).\n")
    # Q6
    lines.append("## 6) Companhia que mais atrasa (a cada ano)")
    lines.append("Arquivos: `charts/cias_<ANO>.png` — por taxa (n≥mínimo) ou por contagem (fallback). Tabela detalhada: `tables/companhias_taxa_atraso_por_ano.csv`.\n")

    (out_dir/'report.md').write_text("\n".join(lines), encoding='utf-8')

# ============ CLI / Main ============

def main():
    ap = argparse.ArgumentParser(description="Relatório de atrasos (gráficos) a partir de dados VRA TRATADOS.")
    ap.add_argument("--input", required=True, help="arquivo CSV/JSON/NDJSON OU diretório com arquivos tratados")
    ap.add_argument("--out", default="./relatorio", help="pasta de saída")
    ap.add_argument("--on-time-min", type=int, default=15, help="limiar de pontualidade (min). atraso = valor > on_time_min")
    ap.add_argument("--min-count-airline", type=int, default=20, help="mínimo de voos/ano por cia para ranking por TAXA (fallback para contagem se ninguém atingir)")
    args = ap.parse_args()

    if plt is None:
        print("[AVISO] matplotlib não disponível. (pip install matplotlib) — os gráficos serão pulados.")

    in_path = Path(args.input)
    out_dir = Path(args.out)

    df = read_any(in_path)
    if df.empty:
        print("Nenhum dado lido da entrada fornecida.")
        return

    df = normalize(df)
    df = build_delay_flags(df, args.on_time_min)
    df = df[df['delay_min'].notna()].copy()
    if df.empty:
        print("Todos os registros ficaram sem métrica de atraso após saneamento.")
        return

    make_report(df, out_dir, on_time_min=args.on_time_min, min_count_airline=args.min_count_airline)
    print(f"✅ Relatório gerado em: {out_dir/'report.md'}")
    print(f"   Tabelas: {out_dir/'tables'}")
    print(f"   Gráficos: {out_dir/'charts'}")

if __name__ == "__main__":
    main()
