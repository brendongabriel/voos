# Análise de Atrasos em Voos (VRA – Brasil)

Pipeline para **limpar dados VRA** (mantendo apenas voos **Brasil–Brasil**) e **gerar relatório com gráficos** de atrasos por aeroporto, dia/horário e companhia.

## 📦 Visão geral

1. **Limpeza** (`clean_vra_brazil.py`): lê arquivos `VRA_*` e salva dados **apenas** com origem e destino no Brasil (prefixos ICAO `SB/SD/SN/SS/SW`) em CSV/JSON/NDJSON.
2. **Análise** (`analyze_vra.py`): consome os dados tratados e produz **tabelas**, **gráficos (PNG)** e um **report.md** com respostas e visualizações.

## 🧰 Requisitos

- Python 3.10+  
- Bibliotecas:
  ```bash
  pip install pandas matplotlib
