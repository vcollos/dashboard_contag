"""
Streamlit dashboard for ANS RN 518 odontological indicators.

The app reads the consolidated view `datalake_ans.vw_indicadores_odonto`
on BigQuery and allows filtering by operator, modality, porte, year and quarter.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Iterable

import pandas as pd
import streamlit as st
import altair as alt
from google.cloud import bigquery

PROJECT_ID_ENV_VAR = "GCP_PROJECT_ID"
DEFAULT_PROJECT = "bigdata-467917"
DATASET = "datalake_ans"
INDICATOR_VIEW = "rn518_gold.vw_rn518_12_indicadores"
COMPONENT_VIEW = "rn518_gold.vw_rn518_resultados_contabeis_trimestre"
DEMONSTRATION_VIEW = "rn518_gold.vw_rn518_resultados_contabeis_trimestre"
META_VIEW = "datalake_ans.vw_indicadores_odonto"
OPERADORAS_TABLE = "datalake_ans.operadoras"
UNIODONTO_CSV = Path(__file__).resolve().parent / "documentos" / "Operadoras.csv"
SERVICE_ACCOUNT_RELATIVE_PATH = (
    "serviceaccount/bigdata-467917-252d585a99b8.json"
)


@dataclass(frozen=True)
class Indicator:
    name: str
    column: str
    kind: str  # pct | ratio | days


@dataclass(frozen=True)
class Component:
    name: str
    column: str
    tooltip: str | None = None


INDICATORS: tuple[Indicator, ...] = (
    Indicator("Sinistralidade", "sinistralidade", "pct"),
    Indicator("Despesas Administrativas", "pct_despesas_administrativas", "pct"),
    Indicator("Despesas Comerciais", "pct_despesas_comerciais", "pct"),
    Indicator("Despesas Operacionais", "pct_despesas_operacionais", "pct"),
    Indicator("Índice Resultado Financeiro", "indice_resultado_financeiro", "pct"),
    Indicator("Liquidez Corrente", "liquidez_corrente", "ratio"),
    Indicator("CT / CP", "ct_cp", "ratio"),
    Indicator("Prazo Médio (Contraprestações)", "pm_contraprestacoes", "days"),
    Indicator("Prazo Médio (Eventos)", "pm_eventos", "days"),
    Indicator("Margem Financeira Líquida", "margem_financeira_liquida", "pct"),
    Indicator("Margem Operacional", "margem_operacional", "pct"),
    Indicator("Margem Lucro Líquida", "margem_lucro_liquida", "pct"),
)

COMPONENTS: tuple[Component, ...] = (
    Component("Contraprestações", "vr_contraprestacoes"),
    Component("Recuperação CCT (ABS)", "vr_cct_abs"),
    Component("Eventos Indenizáveis Líquidos", "vr_eventos_liquidos"),
    Component("Despesa Comercial", "vr_desp_comerciais"),
    Component("Despesa Administrativa", "vr_desp_administrativas"),
    Component("Outras Despesas Operacionais", "vr_outras_desp_oper"),
    Component("Outras Receitas Operacionais", "vr_outras_receitas_operacionais"),
    Component("Receitas Financeiras", "vr_receitas_fin"),
    Component("Despesas Financeiras", "vr_despesas_fin"),
    Component("Ativo Circulante", "vr_ativo_circulante"),
    Component("Passivo Circulante", "vr_passivo_circulante"),
    Component("Passivo Não Circulante", "vr_passivo_nao_circulante"),
    Component("Patrimônio Líquido", "vr_patrimonio_liquido"),
    Component("Créditos Operações Saúde", "vr_creditos_operacoes_saude"),
    Component("Eventos a Liquidar", "vr_eventos_a_liquidar"),
)

COMPONENT_LABELS = {component.column: component.name for component in COMPONENTS}


def chunked(iterable: Iterable[Indicator], size: int) -> Iterable[tuple[Indicator, ...]]:
    iterator = iter(iterable)
    while chunk := tuple(islice(iterator, size)):
        yield chunk


def ensure_credentials() -> Path | None:
    """Guarantee that Google credentials are configured before creating client."""
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        return Path(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    credentials_path = Path(__file__).resolve().parent / SERVICE_ACCOUNT_RELATIVE_PATH
    if credentials_path.exists():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path)
        return credentials_path
    return None


@st.cache_resource(show_spinner=False)
def get_bigquery_client() -> bigquery.Client:
    credentials_path = ensure_credentials()
    if credentials_path is None:
        raise FileNotFoundError(
            "Credencial Google não encontrada. Ajuste a variável "
            f"GOOGLE_APPLICATION_CREDENTIALS ou coloque o arquivo em {SERVICE_ACCOUNT_RELATIVE_PATH}."
        )
    project_id = os.getenv(PROJECT_ID_ENV_VAR, DEFAULT_PROJECT)
    return bigquery.Client(project=project_id)


def run_query(client: bigquery.Client, query: str, parameters: list | None = None) -> pd.DataFrame:
    job_config = bigquery.QueryJobConfig(query_parameters=parameters or [])
    rows = client.query(query, job_config=job_config).result()
    records = [dict(row.items()) for row in rows]
    return pd.DataFrame(records)


@st.cache_data(show_spinner=False)
def load_uniodonto_catalog() -> pd.DataFrame:
    if not UNIODONTO_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(UNIODONTO_CSV, dtype=str)
    df["REG_ANS"] = df["REG_ANS"].str.strip()
    df["CNPJ"] = df["CNPJ"].str.strip()
    return df


@st.cache_data(ttl=3600)
def load_filter_options() -> dict[str, list]:
    client = get_bigquery_client()
    indicator_ref = f"`{client.project}.{INDICATOR_VIEW}`"
    meta_ref = f"`{client.project}.{META_VIEW}`"
    operadoras_ref = f"`{client.project}.{OPERADORAS_TABLE}`"
    meta_query = f"""
        WITH base AS (
          SELECT
            i.ano,
            i.trimestre,
            COALESCE(NULLIF(m.modalidade, ''), NULLIF(UPPER(op.modalidade), '')) AS modalidade,
            NULLIF(m.porte, '') AS porte
          FROM {indicator_ref} i
          LEFT JOIN {meta_ref} m
            ON i.reg_ans = m.reg_ans
           AND i.ano = m.ano
           AND i.trimestre = m.trimestre
          LEFT JOIN {operadoras_ref} op
            ON i.reg_ans = SAFE_CAST(op.REG_ANS AS STRING)
        )
        SELECT
          ARRAY_AGG(DISTINCT ano ORDER BY ano DESC) AS anos,
          ARRAY_AGG(DISTINCT trimestre ORDER BY trimestre) AS trimestres,
          ARRAY_AGG(DISTINCT modalidade IGNORE NULLS ORDER BY modalidade) AS modalidades,
          ARRAY_AGG(DISTINCT porte IGNORE NULLS ORDER BY porte) AS portes
        FROM base
    """
    filters_df = run_query(client, meta_query)
    if filters_df.empty:
        return {"anos": [], "trimestres": [], "modalidades": [], "portes": [], "operadoras": []}
    options = filters_df.iloc[0].to_dict()
    for key in ("anos", "trimestres", "modalidades", "portes"):
        if options.get(key) is None:
            options[key] = []

    operadoras_query = f"""
        SELECT DISTINCT
          i.reg_ans,
          COALESCE(
            NULLIF(m.nome_fantasia, ''),
            NULLIF(m.razao_social, ''),
            NULLIF(op.NOME_FANTASIA, ''),
            NULLIF(op.RAZAO_SOCIAL, ''),
            i.reg_ans
          ) AS nome,
          COALESCE(NULLIF(m.modalidade, ''), NULLIF(UPPER(op.MODALIDADE), '')) AS modalidade,
          NULLIF(m.porte, '') AS porte
        FROM {indicator_ref} i
        LEFT JOIN {meta_ref} m
          ON i.reg_ans = m.reg_ans
         AND i.ano = m.ano
         AND i.trimestre = m.trimestre
        LEFT JOIN {operadoras_ref} op
          ON i.reg_ans = SAFE_CAST(op.REG_ANS AS STRING)
        ORDER BY nome
    """
    operadoras_df = run_query(client, operadoras_query)
    uniodonto_df = load_uniodonto_catalog()
    uniodonto_reg_ans = set()
    if not uniodonto_df.empty:
        uniodonto_reg_ans = set(uniodonto_df["REG_ANS"].dropna().astype(str))
        operadoras_df["is_uniodonto"] = operadoras_df["reg_ans"].isin(uniodonto_reg_ans)
    else:
        operadoras_df["is_uniodonto"] = False
    operadoras_df = (
        operadoras_df.sort_values(["nome", "reg_ans"], na_position="last")
        .drop_duplicates(subset=["reg_ans"], keep="first")
        .reset_index(drop=True)
    )
    options["operadoras"] = operadoras_df
    options["uniodonto_reg_ans"] = sorted(uniodonto_reg_ans)
    return options


def build_filter_conditions(
    filters: dict,
    alias: str = "b",
    ignore_period_filters: bool = False,
) -> tuple[str, list[bigquery.QueryParameter]]:
    clauses = ["1 = 1"]
    params: list[bigquery.QueryParameter] = []

    if not ignore_period_filters and filters.get("anos"):
        clauses.append(f"{alias}.ano IN UNNEST(@anos)")
        params.append(bigquery.ArrayQueryParameter("anos", "INT64", filters["anos"]))
    if not ignore_period_filters and filters.get("trimestres"):
        clauses.append(f"{alias}.trimestre IN UNNEST(@trimestres)")
        params.append(bigquery.ArrayQueryParameter("trimestres", "INT64", filters["trimestres"]))
    if filters.get("modalidades"):
        clauses.append(f"{alias}.modalidade IN UNNEST(@modalidades)")
        params.append(bigquery.ArrayQueryParameter("modalidades", "STRING", filters["modalidades"]))
    if filters.get("portes"):
        clauses.append(f"{alias}.porte IN UNNEST(@portes)")
        params.append(bigquery.ArrayQueryParameter("portes", "STRING", filters["portes"]))
    if filters.get("operadoras"):
        clauses.append(f"{alias}.reg_ans IN UNNEST(@operadoras)")
        params.append(bigquery.ArrayQueryParameter("operadoras", "STRING", filters["operadoras"]))

    if filters.get("somente_uniodonto"):
        uniodonto_reg_ans = filters.get("uniodonto_reg_ans") or []
        if uniodonto_reg_ans:
            clauses.append(f"{alias}.reg_ans IN UNNEST(@uniodonto)")
            params.append(bigquery.ArrayQueryParameter("uniodonto", "STRING", uniodonto_reg_ans))

    return " AND ".join(clauses), params


@st.cache_data(ttl=900, show_spinner=False)
def load_indicators(filters: dict, ignore_period_filters: bool = False) -> pd.DataFrame:
    client = get_bigquery_client()
    indicator_ref = f"`{client.project}.{INDICATOR_VIEW}`"
    meta_ref = f"`{client.project}.{META_VIEW}`"
    operadoras_ref = f"`{client.project}.{OPERADORAS_TABLE}`"
    component_ref = f"`{client.project}.{COMPONENT_VIEW}`"
    filters = dict(filters)
    where_clause, parameters = build_filter_conditions(
        filters,
        alias="b",
        ignore_period_filters=ignore_period_filters,
    )
    query = f"""
        WITH base AS (
          SELECT
            i.reg_ans,
            i.ano,
            i.trimestre,
            COALESCE(
              NULLIF(m.nome_fantasia, ''),
              NULLIF(m.razao_social, ''),
              NULLIF(op.NOME_FANTASIA, ''),
              NULLIF(op.RAZAO_SOCIAL, ''),
              i.reg_ans
            ) AS nome_fantasia,
            COALESCE(
              NULLIF(m.razao_social, ''),
              NULLIF(op.RAZAO_SOCIAL, '')
            ) AS razao_social,
            COALESCE(NULLIF(m.modalidade, ''), NULLIF(UPPER(op.MODALIDADE), '')) AS modalidade,
            NULLIF(m.porte, '') AS porte,
            m.total_beneficiarios AS total_beneficiarios,
            CAST(NULL AS BOOL) AS flag_uniodonto,
            i.sinistralidade,
            i.pct_despesas_administrativas,
            i.pct_despesas_comerciais,
            i.pct_despesas_operacionais,
            i.indice_resultado_financeiro,
            i.liquidez_corrente,
            i.ct_cp,
            i.pm_contraprestacoes,
            i.pm_eventos,
            i.margem_financeira_liquida,
            i.margem_operacional,
            i.margem_lucro_liquida
          FROM {indicator_ref} i
          LEFT JOIN {meta_ref} m
            ON i.reg_ans = m.reg_ans
           AND i.ano = m.ano
           AND i.trimestre = m.trimestre
          LEFT JOIN {operadoras_ref} op
            ON i.reg_ans = SAFE_CAST(op.REG_ANS AS STRING)
        ),
        componentes AS (
          SELECT
            reg_ans,
            ano,
            trimestre,
            contraprestacoes AS vr_contraprestacoes,
            cct_abs AS vr_cct_abs,
            eventos_liquidos AS vr_eventos_liquidos,
            desp_comerciais AS vr_desp_comerciais,
            desp_administrativas AS vr_desp_administrativas,
            outras_desp_oper AS vr_outras_desp_oper,
            vr_outras_receitas_operacionais,
            receitas_fin AS vr_receitas_fin,
            despesas_fin AS vr_despesas_fin,
            ativo_circulante AS vr_ativo_circulante,
            passivo_circulante AS vr_passivo_circulante,
            passivo_nao_circulante AS vr_passivo_nao_circulante,
            patrimonio_liquido AS vr_patrimonio_liquido,
            vr_creditos_operacoes_saude,
            vr_eventos_a_liquidar,
            beneficiarios_trimestre
          FROM {component_ref}
        )
        SELECT
          b.*,
          c.vr_contraprestacoes,
          c.vr_cct_abs,
          c.vr_eventos_liquidos,
          c.vr_desp_comerciais,
          c.vr_desp_administrativas,
          c.vr_outras_desp_oper,
          c.vr_outras_receitas_operacionais,
          c.vr_receitas_fin,
          c.vr_despesas_fin,
          c.vr_ativo_circulante,
          c.vr_passivo_circulante,
          c.vr_passivo_nao_circulante,
          c.vr_patrimonio_liquido,
          c.vr_creditos_operacoes_saude,
          c.vr_eventos_a_liquidar,
          COALESCE(b.total_beneficiarios, c.beneficiarios_trimestre) AS total_beneficiarios
        FROM base b
        LEFT JOIN componentes c
          ON b.reg_ans = c.reg_ans
         AND b.ano = c.ano
         AND b.trimestre = c.trimestre
        WHERE {where_clause}
        ORDER BY b.ano DESC, b.trimestre DESC, b.reg_ans
    """
    return run_query(client, query, parameters)


@st.cache_data(ttl=900, show_spinner=False)
def load_financial_overview(filters: dict) -> pd.DataFrame:
    client = get_bigquery_client()
    indicator_ref = f"`{client.project}.{INDICATOR_VIEW}`"
    meta_ref = f"`{client.project}.{META_VIEW}`"
    operadoras_ref = f"`{client.project}.{OPERADORAS_TABLE}`"
    demo_ref = f"`{client.project}.{DEMONSTRATION_VIEW}`"
    filters = dict(filters)
    where_clause, parameters = build_filter_conditions(filters, alias="b", ignore_period_filters=False)
    query = f"""
        WITH base AS (
          SELECT
            i.reg_ans,
            i.ano,
            i.trimestre,
            COALESCE(
              NULLIF(m.nome_fantasia, ''),
              NULLIF(m.razao_social, ''),
              NULLIF(op.NOME_FANTASIA, ''),
              NULLIF(op.RAZAO_SOCIAL, ''),
              i.reg_ans
            ) AS nome_fantasia,
            COALESCE(
              NULLIF(m.razao_social, ''),
              NULLIF(op.RAZAO_SOCIAL, '')
            ) AS razao_social,
            COALESCE(NULLIF(m.modalidade, ''), NULLIF(UPPER(op.MODALIDADE), '')) AS modalidade,
            NULLIF(m.porte, '') AS porte,
            m.total_beneficiarios AS total_beneficiarios,
            CAST(NULL AS BOOL) AS flag_uniodonto
          FROM {indicator_ref} i
          LEFT JOIN {meta_ref} m
            ON i.reg_ans = m.reg_ans
           AND i.ano = m.ano
           AND i.trimestre = m.trimestre
          LEFT JOIN {operadoras_ref} op
            ON i.reg_ans = SAFE_CAST(op.REG_ANS AS STRING)
        )
        SELECT
          b.*,
          d.vr_contraprestacoes,
          d.vr_eventos_liquidos,
          d.vr_desp_comerciais,
          d.vr_desp_administrativas,
          d.vr_outras_desp_oper,
          d.vr_outras_receitas_operacionais,
          d.vr_receitas_fin,
          d.vr_despesas_fin,
          d.vr_ativo_circulante,
          d.vr_passivo_circulante,
          d.vr_passivo_nao_circulante,
          d.vr_patrimonio_liquido,
          d.vr_creditos_operacoes_saude,
          d.vr_eventos_a_liquidar
        FROM base b
        LEFT JOIN {demo_ref} d
          ON b.reg_ans = d.reg_ans
         AND b.ano = d.ano
         AND b.trimestre = d.trimestre
        WHERE {where_clause}
        ORDER BY b.ano DESC, b.trimestre DESC, b.reg_ans
    """
    return run_query(client, query, parameters)


def format_metric(value: float | None, kind: str) -> str:
    if value is None or pd.isna(value):
        return "—"
    if kind == "pct":
        return f"{value * 100:,.2f}%"
    if kind == "days":
        return f"{value:,.1f} dias"
    return f"{value:,.2f}"


def format_currency(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"R$ {value:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")


def classify_indicator_value(column: str, value: float | None) -> str:
    if value is None or pd.isna(value):
        return "Sem dado"
    if column == "sinistralidade":
        if value <= 0.75:
            return "Excelente"
        if value <= 0.85:
            return "Adequado"
        return "Crítico"
    if column == "pct_despesas_administrativas":
        if value <= 0.10:
            return "Enxuto"
        if value <= 0.15:
            return "Controle"
        return "Pressão"
    if column == "pct_despesas_comerciais":
        if value <= 0.07:
            return "Competitivo"
        if value <= 0.12:
            return "Atenção"
        return "Elevado"
    if column == "pct_despesas_operacionais":
        if value <= 0.90:
            return "Controlado"
        if value <= 1.00:
            return "Limite"
        return "Desfavorável"
    if column == "indice_resultado_financeiro":
        if value >= 0.02:
            return "Positivo"
        if value >= 0:
            return "Neutro"
        return "Negativo"
    if column == "liquidez_corrente":
        if value >= 1.2:
            return "Sólida"
        if value >= 1.0:
            return "Confortável"
        if value >= 0.8:
            return "Alerta"
        return "Risco"
    if column == "ct_cp":
        if value <= 1.5:
            return "Baixo"
        if value <= 3.0:
            return "Moderado"
        return "Alavancado"
    if column in {"margem_financeira_liquida", "margem_operacional", "margem_lucro_liquida"}:
        if value >= 0.05:
            return "Saudável"
        if value >= 0:
            return "Equilíbrio"
        return "Prejuízo"
    if column in {"pm_contraprestacoes", "pm_eventos"}:
        if value <= 60:
            return "Curto"
        if value <= 90:
            return "Médio"
        return "Longo"
    return ""


def render_filters(options: dict) -> dict:
    st.sidebar.header("Filtros")
    anos = options.get("anos", [])
    trimestres = options.get("trimestres", [])
    modalidades = [m for m in options.get("modalidades", []) if m]
    portes = [p for p in options.get("portes", []) if p]
    operadoras_df: pd.DataFrame = options.get("operadoras", pd.DataFrame()).copy()
    uniodonto_reg_ans: list[str] = options.get("uniodonto_reg_ans", [])

    only_uniodonto = st.sidebar.toggle(
        "Somente Uniodonto",
        value=False,
        help="Mantém apenas operadoras Uniodonto nos dados e listas.",
        disabled=len(uniodonto_reg_ans) == 0,
    )
    if "is_uniodonto" not in operadoras_df.columns:
        operadoras_df["is_uniodonto"] = False
    if only_uniodonto and not operadoras_df.empty:
        operadoras_df = operadoras_df.loc[operadoras_df["is_uniodonto"]].reset_index(drop=True)
        modalidades = sorted({m for m in operadoras_df["modalidade"].dropna().unique() if m})
        portes = sorted({p for p in operadoras_df["porte"].dropna().unique() if p})

    selected_years = st.sidebar.multiselect(
        "Ano",
        options=anos,
        default=anos[:4],
    )
    selected_quarters = st.sidebar.multiselect(
        "Trimestre",
        options=trimestres,
        default=trimestres,
    )
    selected_modalidades = st.sidebar.multiselect(
        "Modalidade",
        options=modalidades,
        default=modalidades,
    )
    selected_portes = st.sidebar.multiselect(
        "Porte",
        options=portes,
        default=portes,
    )

    operadora_options = []
    format_lookup: dict[str, str] = {}
    if not operadoras_df.empty:
        operadora_options = operadoras_df["reg_ans"].tolist()
        for _, row in operadoras_df.iterrows():
            desc = row["nome"]
            modalidade = row.get("modalidade")
            porte = row.get("porte")
            if row.get("is_uniodonto"):
                desc = f"{desc} [Uniodonto]"
            if modalidade:
                desc = f"{desc} – {modalidade}"
            if porte:
                desc = f"{desc} / {porte}"
            label = f"{row['reg_ans']} • {desc}"
            format_lookup[row["reg_ans"]] = label

    selected_operadoras = st.sidebar.multiselect(
        "Operadora",
        options=operadora_options,
        format_func=format_lookup.get,
    )

    return {
        "anos": selected_years,
        "trimestres": selected_quarters,
        "modalidades": selected_modalidades,
        "portes": selected_portes,
        "operadoras": selected_operadoras,
        "somente_uniodonto": only_uniodonto,
        "uniodonto_reg_ans": uniodonto_reg_ans,
    }


def append_period_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    periods = pd.PeriodIndex(year=df["ano"], quarter=df["trimestre"], freq="Q")
    df = df.copy()
    df["periodo"] = periods.to_timestamp(how="end")
    df["periodo_label"] = periods.astype(str)

    def build_label(row: pd.Series) -> str:
        fantasia = row.get("nome_fantasia")
        razao = row.get("razao_social")
        if isinstance(fantasia, str) and fantasia.strip():
            nome = fantasia.strip()
        elif isinstance(razao, str) and razao.strip():
            nome = razao.strip()
        else:
            nome = row.get("reg_ans", "")
        return f"{row.get('reg_ans', '')} • {nome}"

    df["operadora_label"] = df.apply(build_label, axis=1)
    return df


def prepare_series_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    base_df = df.copy()
    base_df["_replicado"] = False
    freq_per_year = base_df.groupby(["reg_ans", "ano"])["trimestre"].transform("nunique")
    annual_mask = freq_per_year <= 1
    base_df["periodicidade_registro"] = "Trimestral"
    base_df.loc[annual_mask, "periodicidade_registro"] = "Anual"

    clones: list[dict] = []
    if annual_mask.any():
        annual_df = base_df.loc[annual_mask]
        for (reg_ans, ano), group in annual_df.groupby(["reg_ans", "ano"], as_index=False):
            existing_quarters = set(group["trimestre"])
            missing_quarters = [q for q in (1, 2, 3, 4) if q not in existing_quarters]
            if not missing_quarters:
                continue
            template = group.iloc[0]
            for trimestre in missing_quarters:
                row_dict = template.to_dict()
                row_dict["trimestre"] = trimestre
                row_dict["_replicado"] = True
                clones.append(row_dict)

    if clones:
        base_df = pd.concat([base_df, pd.DataFrame(clones)], ignore_index=True)

    return append_period_columns(base_df)


def show_metrics(df: pd.DataFrame) -> None:
    st.subheader("Indicadores médios do filtro")
    summary = df[[ind.column for ind in INDICATORS]].mean(numeric_only=True)
    for row in chunked(INDICATORS, 3):
        cols = st.columns(len(row))
        for col, indicator in zip(cols, row):
            value = summary.get(indicator.column)
            col.metric(indicator.name, format_metric(value, indicator.kind))


def show_component_summary(df: pd.DataFrame) -> None:
    available_components = [component for component in COMPONENTS if component.column in df.columns]
    if not available_components:
        return

    st.subheader("Componentes contábeis (soma no filtro)")
    totals = df[[component.column for component in available_components]].sum(numeric_only=True)
    for row in chunked(available_components, 3):
        cols = st.columns(len(row))
        for col, component in zip(cols, row):
            value = totals.get(component.column)
            col.metric(component.name, format_currency(value))


def show_indicator_status(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("Indicadores indisponíveis para o filtro atual.")
        return
    summary = df[[ind.column for ind in INDICATORS]].mean(numeric_only=True)
    records = []
    for indicator in INDICATORS:
        value = summary.get(indicator.column)
        records.append(
            {
                "Indicador": indicator.name,
                "Valor médio": format_metric(value, indicator.kind),
                "Status": classify_indicator_value(indicator.column, value),
            }
        )
    status_df = pd.DataFrame(records)
    st.subheader("Classificação dos indicadores")
    st.dataframe(status_df, hide_index=True, use_container_width=True)


def show_timeseries(df: pd.DataFrame) -> None:
    st.subheader("Série temporal")
    indicator_names = [indicator.name for indicator in INDICATORS]
    indicator_by_name = {indicator.name: indicator for indicator in INDICATORS}
    selected_indicator_name = st.selectbox(
        "Selecione o indicador",
        options=indicator_names,
        index=0,
    )
    indicator = indicator_by_name[selected_indicator_name]

    if df.empty:
        st.info("Sem dados para os filtros aplicados.")
        return

    chart_df = df.sort_values(["periodo", "reg_ans"])
    mode = st.radio(
        "Visualizar por",
        options=("Consolidado", "Operadora"),
        horizontal=True,
    )

    if mode == "Consolidado":
        grouped = (
            chart_df.groupby(["periodo", "periodo_label"], as_index=False)[indicator.column]
            .mean(numeric_only=True)
        )
        grouped.rename(columns={indicator.column: "valor"}, inplace=True)
        st.line_chart(
            grouped.set_index("periodo")["valor"],
            height=320,
        )
    else:
        pivot_df = chart_df.pivot_table(
            index="periodo",
            columns="operadora_label",
            values=indicator.column,
            aggfunc="mean",
        )
        st.line_chart(pivot_df, height=320)

    st.caption(
        "Valores consolidados são médias simples das operadoras selecionadas. "
        "As séries individuais exibem a média trimestral por operadora. "
        "O histórico ignora o filtro de ano/trimestre e inclui operadoras de envio anual "
        "com valores replicados ao longo do ano para fins comparativos."
    )


def show_ranking_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("Sem dados para ranquear.")
        return
    latest_period = df.loc[:, ["ano", "trimestre"]].sort_values(["ano", "trimestre"]).drop_duplicates().iloc[-1]
    latest_df = df[(df["ano"] == latest_period["ano"]) & (df["trimestre"] == latest_period["trimestre"])]
    if latest_df.empty:
        st.info("Não foi possível montar o ranking para o período selecionado.")
        return
    latest_with_period = append_period_columns(latest_df)
    ranking = latest_with_period[
        [
            "reg_ans",
            "periodo_label",
            "nome_fantasia",
            "modalidade",
            "porte",
            "sinistralidade",
            "margem_lucro_liquida",
            "liquidez_corrente",
        ]
    ].copy()
    ranking["ranking_mll"] = ranking["margem_lucro_liquida"].rank(ascending=False, method="min").astype(int)
    ranking["ranking_sinistralidade"] = ranking["sinistralidade"].rank(ascending=True, method="min").astype(int)
    ranking.sort_values(["ranking_mll", "ranking_sinistralidade"], inplace=True)
    ranking.rename(columns={"periodo_label": "Período"}, inplace=True)
    ranking["Sinistralidade"] = ranking["sinistralidade"].apply(lambda x: format_metric(x, "pct"))
    ranking["MLL"] = ranking["margem_lucro_liquida"].apply(lambda x: format_metric(x, "pct"))
    ranking["Liquidez Corrente"] = ranking["liquidez_corrente"].apply(lambda x: format_metric(x, "ratio"))
    ranking.rename(
        columns={
            "ranking_mll": "Ranking MLL",
            "reg_ans": "Registro ANS",
            "nome_fantasia": "Operadora",
            "modalidade": "Modalidade",
            "porte": "Porte",
            "ranking_sinistralidade": "Ranking Sinistralidade",
        },
        inplace=True,
    )
    display_cols = [
        "Período",
        "Ranking MLL",
        "Registro ANS",
        "Operadora",
        "Modalidade",
        "Porte",
        "Sinistralidade",
        "Ranking Sinistralidade",
        "MLL",
        "Liquidez Corrente",
    ]
    st.subheader(
        f"Ranking trimestral (Q{int(latest_period['trimestre'])}/{int(latest_period['ano'])})"
    )
    st.dataframe(
        ranking[display_cols],
        hide_index=True,
        use_container_width=True,
    )


def show_detail_table(df: pd.DataFrame) -> None:
    st.subheader("Detalhamento")
    tab_indicadores, tab_componentes = st.tabs(["Indicadores", "Componentes"])

    indicator_cols = [
        "periodo_label",
        "reg_ans",
        "nome_fantasia",
        "modalidade",
        "porte",
        "total_beneficiarios",
        "flag_uniodonto",
    ] + [indicator.column for indicator in INDICATORS]
    renaming = {
        "periodo_label": "Período",
        "reg_ans": "Registro ANS",
        "nome_fantasia": "Operadora",
        "modalidade": "Modalidade",
        "porte": "Porte",
        "total_beneficiarios": "Beneficiários",
        "flag_uniodonto": "Uniodonto",
    }

    with tab_indicadores:
        indicator_data = df[indicator_cols].rename(columns=renaming)
        st.dataframe(
            indicator_data,
            use_container_width=True,
            hide_index=True,
        )
        st.download_button(
            "Download Indicadores (CSV)",
            indicator_data.to_csv(index=False).encode("utf-8"),
            file_name="indicadores_rn518_odonto.csv",
            mime="text/csv",
        )

    component_columns = [component.column for component in COMPONENTS if component.column in df.columns]
    if component_columns:
        component_renaming = {
            component.column: component.name for component in COMPONENTS if component.column in component_columns
        }
        base_cols = ["periodo_label", "reg_ans", "nome_fantasia", "modalidade", "porte", "flag_uniodonto"]
        available_base = [col for col in base_cols if col in df.columns]
        renamed_cols = component_renaming | renaming
        component_data = df[available_base + component_columns].rename(columns=renamed_cols)
    else:
        component_data = pd.DataFrame()

    with tab_componentes:
        if component_data.empty:
            st.info("Componentes indisponíveis para os filtros aplicados.")
            return
        currency_columns = {
            component_renaming[col]: st.column_config.NumberColumn(
                label=component_renaming[col],
                format="R$ %,.0f",
                help=next((comp.tooltip for comp in COMPONENTS if comp.column == col), "") or "",
            )
            for col in component_columns
        }
        st.dataframe(
            component_data,
            use_container_width=True,
            hide_index=True,
            column_config=currency_columns,
        )
        st.download_button(
            "Download Componentes (CSV)",
            component_data.to_csv(index=False).encode("utf-8"),
            file_name="componentes_rn518_odonto.csv",
            mime="text/csv",
        )


def show_financial_panel(df: pd.DataFrame) -> None:
    st.subheader("Painel financeiro (valores trimestrais)")
    if df.empty:
        st.info("Sem valores financeiros para os filtros aplicados.")
        return

    df = append_period_columns(df)
    value_columns = [col for col in df.columns if col.startswith("vr_")]

    operadoras = df[["reg_ans", "nome_fantasia"]].drop_duplicates().sort_values("nome_fantasia")
    operadora_options = ["Consolidado"] + operadoras["reg_ans"].tolist()
    option_labels = {
        "Consolidado": "Consolidado (soma)",
        **{row["reg_ans"]: f"{row['reg_ans']} • {row['nome_fantasia']}" for _, row in operadoras.iterrows()},
    }
    selected_operadora = st.selectbox(
        "Selecione a operadora",
        options=operadora_options,
        format_func=lambda x: option_labels.get(x, x),
    )

    if selected_operadora == "Consolidado":
        filtered = df.groupby(["periodo_label"], as_index=False)[value_columns].sum(numeric_only=True)
        filtered["Operadora"] = "Consolidado"
    else:
        filtered = df[df["reg_ans"] == selected_operadora].copy()
        filtered.rename(columns={"periodo_label": "periodo_label"}, inplace=True)
        filtered["Operadora"] = option_labels[selected_operadora]

    long_df = filtered.melt(
        id_vars=["Operadora", "periodo_label"],
        value_vars=value_columns,
        var_name="conta",
        value_name="valor",
    ).sort_values(["conta", "periodo_label"])
    long_df["valor_anterior"] = long_df.groupby(["Operadora", "conta"])["valor"].shift(1)
    long_df["variacao_pct"] = long_df.apply(
        lambda row: None
        if row["valor_anterior"] in (0, None) or pd.isna(row["valor_anterior"])
        else (row["valor"] - row["valor_anterior"]) / row["valor_anterior"],
        axis=1,
    )
    long_df["Conta"] = long_df["conta"].map(lambda col: COMPONENT_LABELS.get(col, col))
    long_df["Valor (R$)"] = long_df["valor"].apply(format_currency)
    long_df["Variação %"] = long_df["variacao_pct"].apply(lambda x: format_metric(x, "pct"))

    st.dataframe(
        long_df[["Operadora", "periodo_label", "Conta", "Valor (R$)", "Variação %"]],
        use_container_width=True,
        hide_index=True,
    )

    contas_disponiveis = sorted(long_df["Conta"].unique())
    selected_conta = st.selectbox("Visualizar evolução da conta", contas_disponiveis)
    chart_df = long_df[long_df["Conta"] == selected_conta].copy()
    chart_df["Valor numérico"] = chart_df["valor"]
    chart = (
        alt.Chart(chart_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("periodo_label:N", title="Período"),
            y=alt.Y("Valor numérico:Q", title="Valor (R$)", axis=alt.Axis(format="~s")),
            tooltip=[
                alt.Tooltip("Operadora", title="Operadora"),
                alt.Tooltip("periodo_label", title="Período"),
                alt.Tooltip("Valor (R$)", title="Valor"),
                alt.Tooltip("Variação %", title="Variação"),
            ],
        )
    )
    st.altair_chart(chart, use_container_width=True)


def show_correlation_panel(df: pd.DataFrame) -> None:
    st.subheader("Correlação despesas administrativas x MLL")
    if df.empty:
        st.info("Sem dados para análise de correlação.")
        return
    periods = sorted(df["periodo_label"].unique(), reverse=True)
    selected_period = st.selectbox("Período de análise", periods)
    subset = df[df["periodo_label"] == selected_period].copy()
    if subset.empty:
        st.info("Sem dados para o período selecionado.")
        return
    subset["pct_da"] = subset["pct_despesas_administrativas"]
    subset["mll"] = subset["margem_lucro_liquida"]
    if "vr_desp_administrativas" in subset.columns:
        subset["desp_adm_abs"] = subset["vr_desp_administrativas"]
    else:
        subset["desp_adm_abs"] = pd.Series([0] * len(subset), index=subset.index)
    if "operadora_label" not in subset.columns:
        subset["operadora_label"] = subset["reg_ans"]

    chart = (
        alt.Chart(subset)
        .mark_circle()
        .encode(
            x=alt.X("pct_da:Q", title="% Despesas Administrativas", axis=alt.Axis(format="%", tickCount=6)),
            y=alt.Y("mll:Q", title="Margem de Lucro Líquida", axis=alt.Axis(format="%", tickCount=6)),
            size=alt.Size("desp_adm_abs:Q", title="Despesa Administrativa (R$)", scale=alt.Scale(type="sqrt")),
            color=alt.Color("operadora_label:N", title="Operadora"),
            tooltip=[
                alt.Tooltip("operadora_label:N", title="Operadora"),
                alt.Tooltip("pct_da:Q", title="% DA", format=".2%"),
                alt.Tooltip("mll:Q", title="MLL", format=".2%"),
                alt.Tooltip("desp_adm_abs:Q", title="Despesa Adm (R$)", format=",.0f"),
            ],
        )
    )
    st.altair_chart(chart, use_container_width=True)
    resumo = subset[
        ["operadora_label", "pct_da", "mll", "desp_adm_abs", "vr_contraprestacoes"]
    ].copy()
    resumo.rename(
        columns={
            "operadora_label": "Operadora",
            "pct_da": "% Desp. Adm",
            "mll": "MLL",
            "desp_adm_abs": "Desp. Adm (R$)",
            "vr_contraprestacoes": "Contraprestações (R$)",
        },
        inplace=True,
    )
    resumo["% Desp. Adm"] = resumo["% Desp. Adm"].apply(lambda x: format_metric(x, "pct"))
    resumo["MLL"] = resumo["MLL"].apply(lambda x: format_metric(x, "pct"))
    resumo["Desp. Adm (R$)"] = resumo["Desp. Adm (R$)"].apply(format_currency)
    resumo["Contraprestações (R$)"] = resumo["Contraprestações (R$)"].apply(format_currency)
    st.dataframe(resumo, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(
        page_title="Painel RN 518 – Indicadores Odonto",
        page_icon="📊",
        layout="wide",
    )
    st.title("Painel RN 518 – Indicadores Odontológicos")
    st.caption(
        "Dados do `rn518_gold` (`vw_rn518_12_indicadores`, `vw_rn518_componentes_odonto`, "
        "`vw_rn518_demonstracoes_trimestre`) no projeto BigQuery `bigdata-467917`."
    )

    options = load_filter_options()
    selected_filters = render_filters(options)
    data = load_indicators(selected_filters)

    if data.empty:
        st.warning("Nenhum registro encontrado para os filtros aplicados.")
        return

    data = append_period_columns(data)
    series_data = load_indicators(selected_filters, ignore_period_filters=True)
    series_data = prepare_series_data(series_data)
    financial_df = load_financial_overview(selected_filters)

    tab_sintetico, tab_financeiro, tab_series, tab_correlacao = st.tabs(
        ["Painel sintético", "Painel financeiro", "Séries e ranking", "Correlação"]
    )

    with tab_sintetico:
        show_indicator_status(data)
        show_metrics(data)
        show_component_summary(data)
        show_detail_table(data)

    with tab_financeiro:
        show_financial_panel(financial_df)

    with tab_series:
        show_timeseries(series_data)
        show_ranking_table(series_data)

    with tab_correlacao:
        show_correlation_panel(data)


if __name__ == "__main__":
    main()
