# RN 518 – Componentes Detalhados (para auditoria dos indicadores)

> Objetivo: fornecer uma view separada com os **valores brutos** que compõem os indicadores, permitindo confronto com as métricas calculadas e verificação detalhada dos componentes.

---

## 1) View – Componentes detalhados
**`vw_rn518_componentes_odonto_detalhe`**

```sql
CREATE OR REPLACE VIEW `bigdata-467917.rn518_gold.vw_rn518_componentes_odonto_detalhe` AS
SELECT
  reg_ans,
  ano,
  trimestre,
  flag_uniodonto,
  beneficiarios_trimestre,
  contraprestacoes                           AS faturamento_contraprestacoes,
  cct_abs                                    AS recuperacao_cct_absoluta,
  eventos_liquidos                           AS eventos_indenizaveis_liquidos,
  desp_comerciais                            AS despesa_comercial,
  desp_administrativas                       AS despesa_administrativa,
  outras_desp_oper                           AS outras_despesas_operacionais,
  receitas_fin                               AS receitas_financeiras,
  despesas_fin                               AS despesas_financeiras,
  ativo_circulante,
  passivo_circulante,
  passivo_nao_circulante,
  patrimonio_liquido,
  contraprestacao_a_receber,
  eventos_a_liquidar,
  (contraprestacoes + cct_abs)               AS base_receita_contraprestacoes_cct,
  (eventos_liquidos + cct_abs)               AS base_eventos_cct,
  (desp_comerciais + desp_administrativas + outras_desp_oper) AS total_despesas_operacionais
FROM `bigdata-467917.rn518_gold.vw_rn518_componentes_odonto`;
```

---

## 2) View – Indicadores + Componentes (comparativo)
**`vw_rn518_indicadores_com_componentes_odonto`**

```sql
CREATE OR REPLACE VIEW `bigdata-467917.rn518_gold.vw_rn518_indicadores_com_componentes_odonto` AS
WITH ind AS (
  SELECT * FROM `bigdata-467917.rn518_gold.vw_rn518_indicadores_odonto`
)
SELECT
  i.reg_ans,
  i.ano,
  i.trimestre,
  i.beneficiarios_trimestre,
  i.flag_uniodonto,
  i.sinistralidade,
  i.pct_despesas_administrativas,
  i.pct_despesas_comerciais,
  i.pct_despesas_operacionais,
  i.indice_resultado_financeiro,
  i.liquidez_corrente,
  i.ct_cp,
  i.pm_contraprestacoes,
  i.pm_eventos,
  d.faturamento_contraprestacoes,
  d.recuperacao_cct_absoluta,
  d.eventos_indenizaveis_liquidos,
  d.despesa_comercial,
  d.despesa_administrativa,
  d.outras_despesas_operacionais,
  d.receitas_financeiras,
  d.despesas_financeiras,
  d.ativo_circulante,
  d.passivo_circulante,
  d.passivo_nao_circulante,
  d.patrimonio_liquido,
  d.contraprestacao_a_receber,
  d.eventos_a_liquidar
FROM ind i
LEFT JOIN `bigdata-467917.rn518_gold.vw_rn518_componentes_odonto_detalhe` d
  USING (reg_ans, ano, trimestre);
```

---

## 3) Checks de conciliação corrigidos (sem alias incorreto)

### ✅ Opção 1 – expressão direta no WHERE
```sql
SELECT 
  i.reg_ans, 
  i.ano, 
  i.trimestre,
  ABS(i.sinistralidade - SAFE_DIVIDE(
      d.eventos_indenizaveis_liquidos + d.recuperacao_cct_absoluta,
      NULLIF(d.faturamento_contraprestacoes + d.recuperacao_cct_absoluta,0)
  )) AS diff_sinistralidade
FROM `bigdata-467917.rn518_gold.vw_rn518_indicadores_com_componentes_odonto` i
JOIN `bigdata-467917.rn518_gold.vw_rn518_componentes_odonto_detalhe` d 
  USING (reg_ans, ano, trimestre)
WHERE ABS(i.sinistralidade - SAFE_DIVIDE(
      d.eventos_indenizaveis_liquidos + d.recuperacao_cct_absoluta,
      NULLIF(d.faturamento_contraprestacoes + d.recuperacao_cct_absoluta,0)
  )) > 1e-6;
```

### ✅ Opção 2 – usando subquery para reutilizar o alias
```sql
WITH diffs AS (
  SELECT 
    i.reg_ans, 
    i.ano, 
    i.trimestre,
    ABS(i.sinistralidade - SAFE_DIVIDE(
        d.eventos_indenizaveis_liquidos + d.recuperacao_cct_absoluta,
        NULLIF(d.faturamento_contraprestacoes + d.recuperacao_cct_absoluta,0)
    )) AS diff_sinistralidade
  FROM `bigdata-467917.rn518_gold.vw_rn518_indicadores_com_componentes_odonto` i
  JOIN `bigdata-467917.rn518_gold.vw_rn518_componentes_odonto_detalhe` d 
    USING (reg_ans, ano, trimestre)
)
SELECT *
FROM diffs
WHERE diff_sinistralidade > 1e-6;
```

---

## 4) Observações normativas (odonto)
- **Pós‑estabelecido (desde 2022):** a recuperação do custo migra para redução de **41**, mantendo `cct_abs` ≈ 0 e a fórmula válida.
- **Prazos médios (PMCR/PMPE):** considerar versão líquida de PPSC quando disponível.
- **Corresponsabilidade:** `cct_abs` deve ser usado em valor absoluto nas fórmulas (RN 518 – Anexo IV).

