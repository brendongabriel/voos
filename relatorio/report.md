# Relatório de Atrasos (dados tratados)

- Regra de pontualidade: atraso > **15 min** (chegada preferencial; partida como fallback).

## 1) Aeroporto com mais atrasos (geral)
**Destino (chegada):** _TOP_DESTINO_  
**Origem (partida):** _TOP_ORIGEM_

<!-- Mostre os gráficos se existirem -->
![Aeroportos destino com mais atrasos](charts/aeroportos_destino_mais_atrasos.png)
![Aeroportos origem com mais atrasos](charts/aeroportos_origem_mais_atrasos.png)

---

## 2) Aeroporto que mais **aumentou** e que mais **diminuiu** atrasos
**Destino – maior aumento:** _DEST_AUM_ | **maior redução:** _DEST_RED_  
**Origem – maior aumento:** _ORI_AUM_  | **maior redução:** _ORI_RED_

![Variação destino — aumentos](charts/variacao_destino_aumentos.png)
![Variação destino — reduções](charts/variacao_destino_reducoes.png)
![Variação origem — aumentos](charts/variacao_origem_aumentos.png)
![Variação origem — reduções](charts/variacao_origem_reducoes.png)

---

## 3) Tendência no período
A taxa de atraso **_SUBIU_OU_DESCEU_** no período analisado.

![Tendência mensal da taxa de atraso](charts/tendencia_mensal_taxa.png)

---

## 4) Dias da semana com mais atrasos (por ano)
Abaixo, um PNG por ano (usa **taxa** de atraso; se faltarem dados, cai para **contagem**):

[![Dias da semana com mais atrasos em 2022](charts/dow_2022.png)](charts/dow_2022.png) [![Dias da semana com mais atrasos em 2023](charts/dow_2023.png)](charts/dow_2023.png) [![Dias da semana com mais atrasos em 2024](charts/dow_2024.png)](charts/dow_2024.png)
---

## 5) Período do dia com mais atrasos (por ano)
(madrugada, manhã, tarde, noite — taxa; se faltar, contagem)

[![Período do dia com mais atrasos em 2022](charts/periodo_2022.png)](charts/periodo_2022.png) [![Período do dia com mais atrasos em 2023](charts/periodo_2023.png)](charts/periodo_2023.png) [![Período do dia com mais atrasos em 2024](charts/periodo_2024.png)](charts/periodo_2024.png)

---

## 6) Companhia que mais atrasa (por ano)
Mostra por **taxa** com `n ≥ mínimo`; se ninguém bater, mostra por **contagem**.

[![Companhia que mais atrasa em 2022](charts/cias_2022.png)](charts/cias_2022.png) [![Companhia que mais atrasa em 2023](charts/cias_2023.png)](charts/cias_2023.png) [![Companhia que mais atrasa em 2024](charts/cias_2024.png)](charts/cias_2024.png)
