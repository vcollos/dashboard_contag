# Projeto Completo RN 518 – Odontologia (Documentação Técnica)

> Documentação consolidada de todo o projeto de implementação dos indicadores econômico-financeiros da **RN 518/2022** aplicados às **operadoras odontológicas** (Cooperativas Uniodonto e Odontologia de Grupo), conforme normativos da **ANS** e plano de contas da **RN 472**. O projeto foi desenvolvido em ambiente **Google BigQuery (região US)**, com estrutura padronizada em datasets `rn518_silver` e `rn518_gold`.

---

## 1) Estrutura Geral

**Projeto:** `bigdata-467917`

**Datasets criados:**

- `datalake_ans` → origem (dados brutos ANS)
- `rn518_silver` → camada intermediária de normalização (views temporais e de beneficiários)
- `rn518_gold` → camada final de indicadores e métricas (componentes e fórmulas RN 518)

**Camadas:**

1. **Silver:** padronização de tipos e chaves (contábeis e beneficiários).
2. **Gold:** consolidação contábil (RN 472) e cálculo de indicadores (RN 518).

---

## 2) Dataset `rn518_silver`

### 2.1 View `vw_contabeis_saldos_trimestre`

Consolida os saldos contábeis trimestrais do DIOPS.

```sql
CREATE OR REPLACE VIEW `bigdata-467917.rn518_silver.vw_contabeis_saldos_trimestre` AS
SELECT
  SAFE_CAST(REG_ANS AS STRING) AS reg_ans,
  EXTRACT(YEAR FROM DATA) AS ano,
  EXTRACT(QUARTER FROM DATA) AS trimestre,
  CD_CONTA_CONTABIL AS cd_conta,
  ANY_VALUE(DESCRICAO) AS descricao,
  SUM(VL_SALDO_FINAL) AS saldo_final
FROM `bigdata-467917.datalake_ans.demonstracoes_contabeis_raw`
GROUP BY 1,2,3,4;
```

**Campos:**

| Campo        | Tipo    | Descrição                      |
| ------------ | ------- | ------------------------------ |
| reg\_ans     | STRING  | Código ANS da operadora        |
| ano          | INT64   | Ano de referência              |
| trimestre    | INT64   | Trimestre (1–4)                |
| cd\_conta    | STRING  | Código RN 472 (conta contábil) |
| descricao    | STRING  | Descrição da conta             |
| saldo\_final | NUMERIC | Valor final do saldo contábil  |

---

### 2.2 View `vw_beneficiarios_trimestre`

Obtém o número de beneficiários do **último mês do trimestre**.

```sql
CREATE OR REPLACE VIEW `bigdata-467917.rn518_silver.vw_beneficiarios_trimestre` AS
WITH base AS (
  SELECT
    SAFE_CAST(reg_ans AS STRING) AS reg_ans,
    EXTRACT(YEAR FROM Periodo) AS ano,
    EXTRACT(QUARTER FROM Periodo) AS trimestre,
    MAX(Periodo) AS periodo_ref,
    ANY_VALUE(Uniodonto) AS flag_uniodonto,
    ANY_VALUE(ATIVA) AS flag_ativa,
    UPPER(ANY_VALUE(modalidade)) AS modalidade_up
  FROM `bigdata-467917.datalake_ans.operadoras_beneficiarios_modalidade`
  GROUP BY reg_ans, ano, trimestre
)
SELECT
  b.reg_ans,
  b.ano,
  b.trimestre,
  d.Beneficiarios AS beneficiarios_ultimo_mes,
  b.flag_uniodonto,
  b.flag_ativa,
  b.modalidade_up
FROM base b
LEFT JOIN `bigdata-467917.datalake_ans.operadoras_beneficiarios_modalidade` d
  ON SAFE_CAST(d.reg_ans AS STRING) = b.reg_ans
 AND d.Periodo = b.periodo_ref;
```

**Campos:**

| Campo                      | Tipo   | Descrição                                                    |
| -------------------------- | ------ | ------------------------------------------------------------ |
| reg\_ans                   | STRING | Código ANS                                                   |
| ano                        | INT64  | Ano                                                          |
| trimestre                  | INT64  | Trimestre                                                    |
| beneficiarios\_ultimo\_mes | INT64  | Beneficiários do último mês do trimestre                     |
| flag\_uniodonto            | STRING | Indica se pertence ao Sistema Uniodonto                      |
| flag\_ativa                | STRING | Situação ativa/inativa                                       |
| modalidade\_up             | STRING | Modalidade (COOPERATIVA ODONTOLÓGICA / ODONTOLOGIA DE GRUPO) |

---

## 3) Dataset `rn518_gold`

### 3.1 View `vw_rn518_componentes`

Consolida os componentes contábeis segundo a **RN 472**.

```sql
CREATE OR REPLACE VIEW `bigdata-467917.rn518_gold.vw_rn518_componentes` AS
WITH base AS (
  SELECT
    SAFE_CAST(reg_ans AS STRING) AS reg_ans,
    ano,
    trimestre,
    LPAD(REGEXP_REPLACE(CAST(cd_conta AS STRING), r'[^0-9]', ''), 4, '0') AS cd_norm,
    saldo_final
  FROM `bigdata-467917.rn518_silver.vw_contabeis_saldos_trimestre`
)
SELECT
  reg_ans, ano, trimestre,
  SUM(IF(cd_norm = '3111', saldo_final, 0)) AS contraprestacoes,
  SUM(IF(cd_norm = '3117', ABS(saldo_final), 0)) AS cct_abs,
  SUM(IF(STARTS_WITH(cd_norm,'41'), saldo_final, 0)) AS eventos_liquidos,
  SUM(IF(STARTS_WITH(cd_norm,'43'), saldo_final, 0)) AS desp_comerciais,
  SUM(IF(STARTS_WITH(cd_norm,'46'), saldo_final, 0)) AS desp_administrativas,
  SUM(IF(STARTS_WITH(cd_norm,'44'), saldo_final, 0)) AS outras_desp_oper,
  SUM(IF(STARTS_WITH(cd_norm,'35'), saldo_final, 0)) AS receitas_fin,
  SUM(IF(STARTS_WITH(cd_norm,'45'), saldo_final, 0)) AS despesas_fin,
  SUM(IF(STARTS_WITH(cd_norm,'12'), saldo_final, 0)) AS ativo_circulante,
  SUM(IF(STARTS_WITH(cd_norm,'21'), saldo_final, 0)) AS passivo_circulante,
  SUM(IF(STARTS_WITH(cd_norm,'23'), saldo_final, 0)) AS passivo_nao_circulante,
  SUM(IF(STARTS_WITH(cd_norm,'25'), saldo_final, 0)) AS patrimonio_liquido,
  SUM(IF(STARTS_WITH(cd_norm,'1231'), saldo_final, 0)) AS contraprestacao_a_receber,
  SUM(IF(STARTS_WITH(cd_norm,'2111'), saldo_final, 0)) AS eventos_a_liquidar
FROM base
GROUP BY 1,2,3;
```

---

### 3.2 View `vw_rn518_componentes_odonto`

Junção dos componentes com beneficiários e filtro de modalidade.

```sql
CREATE OR REPLACE VIEW `bigdata-467917.rn518_gold.vw_rn518_componentes_odonto` AS
SELECT
  c.*,
  b.beneficiarios_ultimo_mes AS beneficiarios_trimestre,
  b.flag_uniodonto,
  b.flag_ativa
FROM `bigdata-467917.rn518_gold.vw_rn518_componentes` c
LEFT JOIN `bigdata-467917.rn518_silver.vw_beneficiarios_trimestre` b
  ON c.reg_ans = b.reg_ans
 AND c.ano = b.ano
 AND c.trimestre = b.trimestre
WHERE COALESCE(b.flag_ativa, 'SIM') = 'SIM'
  AND UPPER(b.modalidade_up) IN ('COOPERATIVA ODONTOLÓGICA','ODONTOLOGIA DE GRUPO');
```

---

### 3.3 Views de Indicadores RN 518

#### 3.3.1 Indicadores principais

(`vw_rn518_indicadores_odonto`)

- Sinistralidade, DA, DC, DOP, IRF, LC, CT/CP, PMCR, PMPE.

#### 3.3.2 Variação de Custos per capita (VC)

(`vw_rn518_vc_odonto`)

- Cálculo rolling 12m.

#### 3.3.3 MLL e ROE

(`vw_rn518_mll_roe_odonto`)

- Margem de Lucro Líquida e Retorno sobre PL.

*(Os scripts completos estão no documento “RN 518 – Indicadores (Camada Gold) – Odonto”.)*

---

## 4) Dicionário geral de campos

Inclui todas as variáveis disponíveis nas views Gold, com tipagem padrão BigQuery.

| Campo                    | Tipo    | Origem        | Descrição                     |
| ------------------------ | ------- | ------------- | ----------------------------- |
| reg\_ans                 | STRING  | ANS           | Código da operadora           |
| ano                      | INT64   | derivado      | Ano da competência            |
| trimestre                | INT64   | derivado      | Trimestre                     |
| contraprestacoes         | NUMERIC | 3111          | Receita de contraprestações   |
| cct\_abs                 | NUMERIC | 3117          | Recuperações de CCT           |
| eventos\_liquidos        | NUMERIC | 41\*\*        | Eventos indenizáveis líquidos |
| desp\_comerciais         | NUMERIC | 43\*\*        | Despesas comerciais           |
| desp\_administrativas    | NUMERIC | 46\*\*        | Despesas administrativas      |
| outras\_desp\_oper       | NUMERIC | 44\*\*        | Outras despesas operacionais  |
| receitas\_fin            | NUMERIC | 35\*\*        | Receitas financeiras          |
| despesas\_fin            | NUMERIC | 45\*\*        | Despesas financeiras          |
| ativo\_circulante        | NUMERIC | 12\*\*        | Ativo circulante              |
| passivo\_circulante      | NUMERIC | 21\*\*        | Passivo circulante            |
| passivo\_nao\_circulante | NUMERIC | 23\*\*        | Passivo não circulante        |
| patrimonio\_liquido      | NUMERIC | 25\*\*        | Patrimônio líquido            |
| beneficiarios\_trimestre | INT64   | beneficiários | Último mês do trimestre       |

---

## 5) Fórmulas dos Indicadores RN 518

| Indicador      | Fórmula (RN 518/2022)                                                                                                          | Campos Utilizados                                        |                         |                                                   |   |                                               |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------- | ----------------------- | ------------------------------------------------- | - | --------------------------------------------- |
| Sinistralidade | (eventos\_liquidos +                                                                                                           | cct\_abs                                                 | ) / (contraprestacoes + | cct\_abs                                          | ) | eventos\_liquidos, cct\_abs, contraprestacoes |
| DA             | desp\_administrativas / (contraprestacoes +                                                                                    | cct\_abs                                                 | )                       | desp\_administrativas, contraprestacoes, cct\_abs |   |                                               |
| DC             | desp\_comerciais / (contraprestacoes +                                                                                         | cct\_abs                                                 | )                       | desp\_comerciais, contraprestacoes, cct\_abs      |   |                                               |
| DOP            | (eventos\_liquidos + cct\_abs + desp\_comerciais + desp\_administrativas + outras\_desp\_oper) / (contraprestacoes + cct\_abs) | idem                                                     |                         |                                                   |   |                                               |
| IRF            | (receitas\_fin - despesas\_fin) / (contraprestacoes + cct\_abs)                                                                | receitas\_fin, despesas\_fin, contraprestacoes, cct\_abs |                         |                                                   |   |                                               |
| LC             | ativo\_circulante / passivo\_circulante                                                                                        | ativo\_circulante, passivo\_circulante                   |                         |                                                   |   |                                               |
| CT/CP          | (passivo\_circulante + passivo\_nao\_circulante) / patrimonio\_liquido                                                         | idem                                                     |                         |                                                   |   |                                               |
| PMCR           | (contraprestacao\_a\_receber \* 360) / contraprestacoes                                                                        | contraprestacao\_a\_receber, contraprestacoes            |                         |                                                   |   |                                               |
| PMPE           | (eventos\_a\_liquidar \* 360) / eventos\_liquidos                                                                              | eventos\_a\_liquidar, eventos\_liquidos                  |                         |                                                   |   |                                               |
| VC             | (eventos\_pc\_t / eventos\_pc\_t-1) - 1                                                                                        | eventos\_liquidos, beneficiarios                         |                         |                                                   |   |                                               |
| MLL            | resultado\_liquido / contraprestacoes                                                                                          | receitas (3\*\*), despesas (4\*\*), IR/CS (61\*\*), 3111 |                         |                                                   |   |                                               |
| ROE            | resultado\_liquido / pl\_medio                                                                                                 | resultado\_liquido, patrimonio\_liquido                  |                         |                                                   |   |                                               |

---

## 6) Governança e Atualização

- **Frequência:** trimestral (pós-publicação DIOPS).
- **Processo:** job automático BigQuery/Cloud Composer.
- **Auditoria:** checagens PPA/DIOPS e reconciliações automáticas.
- **Validação:** Comitê de Dados Uniodonto – conferência de saldos e fórmulas.

---

## 7) Referências normativas

- **RN 518/2022 – ANS:** indicadores econômico-financeiros obrigatórios.
- **RN 472/2021 – ANS:** plano de contas padronizado.
- **Manual de Governança e PPA/DIOPS:** parâmetros de consistência e reconciliação.
- **Notas metodológicas ANS (2023):** critérios de pós-estabelecido, ponderação e comparabilidade.

---

## 8) Próximos passos

- Integrar as views Gold ao app **Streamlit – Painel RN 518 Odonto**.
- Publicar documentação técnica (dbt docs) no repositório `uniodonto-rn518-dashboard`.
- Configurar agendamentos trimestrais automáticos no BigQuery.

