# RN 518 – Componentes Detalhados (para auditoria dos indicadores)

> Objetivo: entregar **valores em R\$ (saldos)** exatamente como constam nas demonstrações contábeis e que **alimentam os indicadores**, permitindo comparação, conferência de saldos e reconciliação com balancetes.

---

## 1) View – **Linhas contábeis classificadas** (com valores em R\$)

``\
Classifica **cada linha** contábil trimestral em um **componente RN 518** (ex.: faturamento 3111, eventos 41\*\*, DA 46\*\*, DC 43\*\*, etc.), preservando `cd_conta`, `descricao` e `saldo_final (R$)`.

```sql
CREATE OR REPLACE VIEW `bigdata-467917.rn518_gold.vw_rn518_linhas_classificadas_odonto` AS
WITH cont AS (
  SELECT 
    SAFE_CAST(reg_ans AS STRING) AS reg_ans,
    ano,
    trimestre,
    -- normaliza código de conta (somente dígitos, com padding)
    LPAD(REGEXP_REPLACE(CAST(cd_conta AS STRING), r'[^0-9]', ''), 4, '0') AS cd_norm,
    cd_conta AS cd_conta_original,
    descricao,
    saldo_final
  FROM `bigdata-467917.rn518_silver.vw_contabeis_saldos_trimestre`
)
, ben AS (
  SELECT reg_ans, ano, trimestre, modalidade_up
  FROM `bigdata-467917.rn518_silver.vw_beneficiarios_trimestre`
)
SELECT 
  c.reg_ans,
  c.ano,
  c.trimestre,
  c.cd_conta_original,
  c.cd_norm,
  c.descricao,
  c.saldo_final AS valor_rs,
  -- mapeamento string-safe
  CASE 
    WHEN c.cd_norm = '3111' THEN 'CONTRAPRESTACOES'
    WHEN c.cd_norm = '3117' THEN 'CCT_RECUPERACAO'
    WHEN STARTS_WITH(c.cd_norm,'41') THEN 'EVENTOS_LIQUIDOS'
    WHEN STARTS_WITH(c.cd_norm,'43') THEN 'DESPESAS_COMERCIAIS'
    WHEN STARTS_WITH(c.cd_norm,'46') THEN 'DESPESAS_ADMINISTRATIVAS'
    WHEN STARTS_WITH(c.cd_norm,'44') THEN 'OUTRAS_DESPESAS_OPER'
    WHEN STARTS_WITH(c.cd_norm,'35') THEN 'RECEITAS_FIN'
    WHEN STARTS_WITH(c.cd_norm,'45') THEN 'DESPESAS_FIN'
    WHEN STARTS_WITH(c.cd_norm,'12') THEN 'ATIVO_CIRCULANTE'
    WHEN STARTS_WITH(c.cd_norm,'21') THEN 'PASSIVO_CIRCULANTE'
    WHEN STARTS_WITH(c.cd_norm,'23') THEN 'PASSIVO_NAO_CIRCULANTE'
    WHEN STARTS_WITH(c.cd_norm,'25') THEN 'PATRIMONIO_LIQUIDO'
    WHEN STARTS_WITH(c.cd_norm,'1231') THEN 'CONTRAPRESTACAO_A_RECEBER'
    WHEN STARTS_WITH(c.cd_norm,'2111') THEN 'EVENTOS_A_LIQUIDAR'
    ELSE 'OUTROS'
  END AS componente
FROM cont c
JOIN ben b USING (reg_ans, ano, trimestre)
WHERE UPPER(b.modalidade_up) IN ('COOPERATIVA ODONTOLÓGICA','ODONTOLOGIA DE GRUPO');
```

**O que essa view entrega (por linha):** `reg_ans, ano, trimestre, cd_conta_original, cd_norm, descricao, valor_rs, componente`.

---

## 2) View – **Resumo monetário por componente** (R\$ usados nos indicadores)

``\
Soma os valores **em R\$** por `reg_ans/ano/trimestre` para cada **componente** utilizado nas fórmulas dos indicadores.

```sql
CREATE OR REPLACE VIEW `bigdata-467917.rn518_gold.vw_rn518_componentes_odonto_resumo_monetario` AS
WITH base AS (
  SELECT * FROM `bigdata-467917.rn518_gold.vw_rn518_linhas_classificadas_odonto`
)
SELECT
  reg_ans,
  ano,
  trimestre,
  -- valores em R$ (iguais aos usados nos indicadores)
  SUM(IF(componente='CONTRAPRESTACOES',           valor_rs, 0))                             AS vr_contraprestacoes,
  SUM(IF(componente='CCT_RECUPERACAO',            ABS(valor_rs), 0))                         AS vr_cct_abs,
  SUM(IF(componente='EVENTOS_LIQUIDOS',           valor_rs, 0))                               AS vr_eventos_liquidos,
  SUM(IF(componente='DESPESAS_COMERCIAIS',        valor_rs, 0))                               AS vr_desp_comerciais,
  SUM(IF(componente='DESPESAS_ADMINISTRATIVAS',   valor_rs, 0))                               AS vr_desp_administrativas,
  SUM(IF(componente='OUTRAS_DESPESAS_OPER',       valor_rs, 0))                               AS vr_outras_desp_oper,
  SUM(IF(componente='RECEITAS_FIN',               valor_rs, 0))                               AS vr_receitas_fin,
  SUM(IF(componente='DESPESAS_FIN',               valor_rs, 0))                               AS vr_despesas_fin,
  SUM(IF(componente='ATIVO_CIRCULANTE',           valor_rs, 0))                               AS vr_ativo_circulante,
  SUM(IF(componente='PASSIVO_CIRCULANTE',         valor_rs, 0))                               AS vr_passivo_circulante,
  SUM(IF(componente='PASSIVO_NAO_CIRCULANTE',     valor_rs, 0))                               AS vr_passivo_nao_circulante,
  SUM(IF(componente='PATRIMONIO_LIQUIDO',         valor_rs, 0))                               AS vr_patrimonio_liquido,
  SUM(IF(componente='CONTRAPRESTACAO_A_RECEBER',  valor_rs, 0))                               AS vr_contraprestacao_a_receber,
  SUM(IF(componente='EVENTOS_A_LIQUIDAR',         valor_rs, 0))                               AS vr_eventos_a_liquidar
FROM base
GROUP BY 1,2,3;
```

**Entregáveis (por período/operadora):** todos os **numeradores/denominadores em R\$** que entram nas fórmulas (ex.: `vr_eventos_liquidos`, `vr_contraprestacoes`, `vr_cct_abs`, etc.).

---

## 3) View – **Indicadores com Numeradores/Denominadores expostos**

``\
Traz os **indicadores** e, junto, os **valores em R\$** que os compõem, para comparação 1:1.

```sql
CREATE OR REPLACE VIEW `bigdata-467917.rn518_gold.vw_rn518_indicadores_odonto_explicado` AS
WITH ind AS (
  SELECT * FROM `bigdata-467917.rn518_gold.vw_rn518_indicadores_odonto`
)
SELECT 
  i.reg_ans,
  i.ano,
  i.trimestre,
  i.beneficiarios_trimestre,
  i.flag_uniodonto,
  -- indicadores
  i.sinistralidade,
  i.pct_despesas_administrativas,
  i.pct_despesas_comerciais,
  i.pct_despesas_operacionais,
  i.indice_resultado_financeiro,
  i.liquidez_corrente,
  i.ct_cp,
  i.pm_contraprestacoes,
  i.pm_eventos,
  -- componentes monetários (R$) usados nas fórmulas
  r.vr_contraprestacoes,
  r.vr_cct_abs,
  r.vr_eventos_liquidos,
  r.vr_desp_comerciais,
  r.vr_desp_administrativas,
  r.vr_outras_desp_oper,
  r.vr_receitas_fin,
  r.vr_despesas_fin,
  r.vr_ativo_circulante,
  r.vr_passivo_circulante,
  r.vr_passivo_nao_circulante,
  r.vr_patrimonio_liquido,
  r.vr_contraprestacao_a_receber,
  r.vr_eventos_a_liquidar
FROM ind i
LEFT JOIN `bigdata-467917.rn518_gold.vw_rn518_componentes_odonto_resumo_monetario` r
  USING (reg_ans, ano, trimestre);
```

---

## 4) Consultas de reconciliação (R\$ vs. indicadores)

### 4.1 Sinistralidade: comparar valor calculado vs. indicador

```sql
SELECT 
  reg_ans, ano, trimestre,
  sinistralidade,
  SAFE_DIVIDE(vr_eventos_liquidos + vr_cct_abs, NULLIF(vr_contraprestacoes + vr_cct_abs,0)) AS sinistralidade_recalc,
  ABS( sinistralidade - SAFE_DIVIDE(vr_eventos_liquidos + vr_cct_abs, NULLIF(vr_contraprestacoes + vr_cct_abs,0)) ) AS diff
FROM `bigdata-467917.rn518_gold.vw_rn518_indicadores_odonto_explicado`
WHERE ABS( sinistralidade - SAFE_DIVIDE(vr_eventos_liquidos + vr_cct_abs, NULLIF(vr_contraprestacoes + vr_cct_abs,0)) ) > 1e-6;
```

### 4.2 DOP, DA, DC, IRF (mesma lógica)

```sql
SELECT 
  reg_ans, ano, trimestre,
  pct_despesas_operacionais,
  SAFE_DIVIDE(vr_eventos_liquidos + vr_cct_abs + vr_desp_comerciais + vr_desp_administrativas + vr_outras_desp_oper,
              NULLIF(vr_contraprestacoes + vr_cct_abs,0)) AS dop_recalc
FROM `bigdata-467917.rn518_gold.vw_rn518_indicadores_odonto_explicado`;
```

### 4.3 LC e CT/CP

```sql
SELECT 
  reg_ans, ano, trimestre,
  liquidez_corrente,
  SAFE_DIVIDE(vr_ativo_circulante, NULLIF(vr_passivo_circulante,0)) AS lc_recalc,
  ct_cp,
  SAFE_DIVIDE(vr_passivo_circulante + vr_passivo_nao_circulante, NULLIF(vr_patrimonio_liquido,0)) AS ctcp_recalc
FROM `bigdata-467917.rn518_gold.vw_rn518_indicadores_odonto_explicado`;
```

### 4.4 Prazos médios

```sql
SELECT
  reg_ans, ano, trimestre,
  pm_contraprestacoes,
  SAFE_MULTIPLY(SAFE_DIVIDE(GREATEST(vr_contraprestacao_a_receber,0), NULLIF(vr_contraprestacoes,0)), 360) AS pmcr_recalc,
  pm_eventos,
  SAFE_MULTIPLY(SAFE_DIVIDE(GREATEST(vr_eventos_a_liquidar,0), NULLIF(vr_eventos_liquidos,0)), 360) AS pmpe_recalc
FROM `bigdata-467917.rn518_gold.vw_rn518_indicadores_odonto_explicado`;
```

---

## 5) Observações normativas e de negócio

- **Valores em R\$** são os mesmos que alimentam os indicadores, extraídos por **prefixo/código RN 472** (string-safe).
- **CCT (3117)** é tratado em **valor absoluto** nas bases de cálculo, conforme RN 518.
- **Pós‑estabelecido**: recuperação do custo reflete em **41**; `vr_cct_abs` tende a zero e as fórmulas seguem válidas.
- **Prazos médios**: quando houver **PPSC** segregada, ajustar PMCR para `(vr_contraprestacao_a_receber - vr_ppsc)`.
- Para **auditoria fina**, use `vw_rn518_linhas_classificadas_odonto` para navegar nas **linhas originais** (conta, descrição, valor R\$) que compõem cada componente.

