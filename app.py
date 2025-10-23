"""
Streamlit dashboard for ANS RN 518 odontological indicators.

The app reads the consolidated dataset `dados/csv_completao_2trimestre25.parquet`
and allows filtering by operadora, modalidade, porte, ano e trimestre.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Iterable

import altair as alt
import pandas as pd
import streamlit as st

INDICATORS_DATA_PATH = Path(__file__).resolve().parent / "dados" / "csv_completao_2trimestre25.parquet"


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
    Indicator("Despesas com Tributos", "pct_despesas_tributarias", "pct"),
    Indicator("Despesas Operacionais", "pct_despesas_operacionais", "pct"),
    Indicator("Índice Resultado Financeiro", "indice_resultado_financeiro", "pct"),
    Indicator("Margem Financeira Líquida", "margem_financeira_liquida", "pct"),
    Indicator("Liquidez Corrente", "liquidez_corrente", "ratio"),
    Indicator("Liquidez Seca", "liquidez_seca", "ratio"),
    Indicator("Endividamento", "endividamento", "ratio"),
    Indicator("Imobilização do PL", "imobilizacao_pl", "ratio"),
    Indicator("Retorno sobre PL", "retorno_patrimonio_liquido", "pct"),
    Indicator("Cobertura Provisões Técnicas", "cobertura_provisoes", "ratio"),
    Indicator("Margem de Solvência", "margem_solvencia", "ratio"),
)

COMPONENTS: tuple[Component, ...] = tuple()

COMPONENT_LABELS = {component.column: component.name for component in COMPONENTS}


def chunked(iterable: Iterable[Indicator], size: int) -> Iterable[tuple[Indicator, ...]]:
    iterator = iter(iterable)
    while chunk := tuple(islice(iterator, size)):
        yield chunk


@st.cache_data(show_spinner=False)
def load_dataset() -> pd.DataFrame:
    if not INDICATORS_DATA_PATH.exists():
        raise FileNotFoundError(
            f"Arquivo de indicadores não encontrado em {INDICATORS_DATA_PATH}. "
            "Confirme que o arquivo Parquet definitivo está disponível."
        )
    df = pd.read_parquet(INDICATORS_DATA_PATH)
    df = df.copy()
    df.columns = [col.strip() for col in df.columns]
    df["reg_ans"] = df["reg_ans"].astype(str).str.strip()
    df["ano"] = df["ano"].astype(int)
    df["trimestre"] = df["trimestre"].astype(int)
    if "nome_operadora" not in df.columns:
        df["nome_operadora"] = ""
    else:
        df["nome_operadora"] = df["nome_operadora"].fillna("").astype(str).str.strip()
    df["nome_fantasia"] = df["nome_operadora"]
    df["razao_social"] = df["nome_operadora"]
    if "modalidade" not in df.columns:
        df["modalidade"] = ""
    else:
        df["modalidade"] = df["modalidade"].fillna("").astype(str).str.strip()
    if "porte" not in df.columns:
        df["porte"] = ""
    else:
        df["porte"] = df["porte"].fillna("").astype(str).str.strip()
    if "qt_beneficiarios_periodo" in df.columns:
        df["total_beneficiarios"] = df["qt_beneficiarios_periodo"].fillna(0)
    else:
        df["total_beneficiarios"] = 0
    if "uniodonto" in df.columns:
        df["uniodonto"] = df["uniodonto"].fillna("").astype(str).str.strip()
    else:
        df["uniodonto"] = ""
    df["is_uniodonto"] = df["uniodonto"].str.upper() == "SIM"
    for indicator in INDICATORS:
        if indicator.column not in df.columns:
            df[indicator.column] = float("nan")
    return df


@st.cache_data(show_spinner=False)
def load_filter_options() -> dict[str, list]:
    df = load_dataset()
    anos = sorted(df["ano"].dropna().unique().tolist(), reverse=True)
    trimestres = sorted(df["trimestre"].dropna().unique().tolist())
    modalidades = sorted(
        value for value in df["modalidade"].dropna().unique().tolist() if isinstance(value, str) and value
    )
    portes = sorted(
        value for value in df["porte"].dropna().unique().tolist() if isinstance(value, str) and value
    )
    operadoras_df = (
        df[["reg_ans", "nome_fantasia", "modalidade", "porte", "is_uniodonto"]]
        .drop_duplicates(subset=["reg_ans"], keep="first")
        .sort_values(["nome_fantasia", "reg_ans"], na_position="last")
        .rename(columns={"nome_fantasia": "nome"})
        .reset_index(drop=True)
    )
    operadoras_df["nome"] = operadoras_df["nome"].fillna("").astype(str).str.strip()
    operadoras_df.loc[operadoras_df["nome"] == "", "nome"] = operadoras_df.loc[
        operadoras_df["nome"] == "", "reg_ans"
    ]
    uniodonto_reg_ans = sorted(operadoras_df.loc[operadoras_df["is_uniodonto"], "reg_ans"].tolist())
    return {
        "anos": anos,
        "trimestres": trimestres,
        "modalidades": modalidades,
        "portes": portes,
        "operadoras": operadoras_df,
        "uniodonto_reg_ans": uniodonto_reg_ans,
    }


def apply_filters(df: pd.DataFrame, filters: dict, ignore_period_filters: bool = False) -> pd.DataFrame:
    if df.empty or not filters:
        return df.copy()

    mask = pd.Series(True, index=df.index)

    if not ignore_period_filters:
        anos = filters.get("anos") or []
        if anos:
            mask &= df["ano"].isin(anos)
        trimestres = filters.get("trimestres") or []
        if trimestres:
            mask &= df["trimestre"].isin(trimestres)

    modalidades = filters.get("modalidades") or []
    if modalidades:
        mask &= df["modalidade"].isin(modalidades)

    portes = filters.get("portes") or []
    if portes:
        mask &= df["porte"].isin(portes)

    operadoras = filters.get("operadoras") or []
    if operadoras:
        mask &= df["reg_ans"].isin(operadoras)

    if filters.get("somente_uniodonto"):
        mask &= df["is_uniodonto"]

    return df.loc[mask].copy()


@st.cache_data(ttl=900, show_spinner=False)
def load_indicators(filters: dict, ignore_period_filters: bool = False) -> pd.DataFrame:
    df = load_dataset()
    filtered = apply_filters(df, filters, ignore_period_filters=ignore_period_filters)
    if filtered.empty:
        return filtered
    return (
        filtered.sort_values(["ano", "trimestre", "reg_ans"], ascending=[False, False, True])
        .reset_index(drop=True)
    )


@st.cache_data(ttl=900, show_spinner=False)
def load_financial_overview(filters: dict) -> pd.DataFrame:
    df = load_dataset()
    filtered = apply_filters(df, filters, ignore_period_filters=False)
    if filtered.empty:
        return filtered
    return filtered.sort_values(["ano", "trimestre", "reg_ans"], ascending=[False, False, True]).reset_index(drop=True)


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


def format_difference(value: float | None, kind: str) -> str:
    if value is None or pd.isna(value):
        return "—"
    if kind == "pct":
        return f"{value * 100:+.2f}%"
    if kind == "days":
        return f"{value:+.1f} dias"
    return f"{value:+.2f}"


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
    if column == "pct_despesas_tributarias":
        if value <= 0.03:
            return "Controlado"
        if value <= 0.05:
            return "Atenção"
        return "Pressão"
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
    if column == "liquidez_seca":
        if value >= 1.2:
            return "Sólida"
        if value >= 1.0:
            return "Confortável"
        if value >= 0.8:
            return "Alerta"
        return "Risco"
    if column == "endividamento":
        if value <= 1.0:
            return "Baixo"
        if value <= 2.0:
            return "Moderado"
        return "Elevado"
    if column == "imobilizacao_pl":
        if value <= 0.6:
            return "Adequado"
        if value <= 0.8:
            return "Atenção"
        return "Alto"
    if column == "retorno_patrimonio_liquido":
        if value >= 0.08:
            return "Excelente"
        if value >= 0.04:
            return "Adequado"
        if value >= 0:
            return "Atenção"
        return "Negativo"
    if column in {"margem_financeira_liquida"}:
        if value >= 0.05:
            return "Saudável"
        if value >= 0:
            return "Equilíbrio"
        return "Prejuízo"
    if column == "cobertura_provisoes":
        if value >= 1.0:
            return "Coberto"
        if value >= 0.9:
            return "Atenção"
        return "Descoberto"
    if column == "margem_solvencia":
        if value >= 1.0:
            return "Atende"
        if value >= 0.8:
            return "Atenção"
        return "Insuficiente"
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

    selected_years = st.sidebar.multiselect("Ano", options=anos, default=[])
    selected_quarters = st.sidebar.multiselect("Trimestre", options=trimestres, default=[])
    selected_modalidades = st.sidebar.multiselect("Modalidade", options=modalidades, default=[])
    selected_portes = st.sidebar.multiselect("Porte", options=portes, default=[])

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
    st.dataframe(status_df, hide_index=True, use_container_width=True, height=len(status_df) * 48 + 38)


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
    required_cols = {"sinistralidade", "retorno_patrimonio_liquido", "liquidez_corrente"}
    if not required_cols.issubset(latest_with_period.columns):
        st.info("Indicadores necessários para o ranking não estão disponíveis.")
        return
    ranking = latest_with_period[
        [
            "reg_ans",
            "periodo_label",
            "nome_fantasia",
            "modalidade",
            "porte",
            "sinistralidade",
            "retorno_patrimonio_liquido",
            "liquidez_corrente",
        ]
    ].copy()
    ranking["ranking_roe"] = ranking["retorno_patrimonio_liquido"].rank(ascending=False, method="min").astype("Int64")
    ranking["ranking_sinistralidade"] = ranking["sinistralidade"].rank(ascending=True, method="min").astype("Int64")
    ranking.sort_values(["ranking_roe", "ranking_sinistralidade"], inplace=True)
    ranking.rename(columns={"periodo_label": "Período"}, inplace=True)
    ranking["Sinistralidade"] = ranking["sinistralidade"].apply(lambda x: format_metric(x, "pct"))
    ranking["ROE"] = ranking["retorno_patrimonio_liquido"].apply(lambda x: format_metric(x, "pct"))
    ranking["Liquidez Corrente"] = ranking["liquidez_corrente"].apply(lambda x: format_metric(x, "ratio"))
    ranking.rename(
        columns={
            "ranking_roe": "Ranking ROE",
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
        "Ranking ROE",
        "Registro ANS",
        "Operadora",
        "Modalidade",
        "Porte",
        "Sinistralidade",
        "Ranking Sinistralidade",
        "ROE",
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
    ] + [indicator.column for indicator in INDICATORS]
    renaming = {
        "periodo_label": "Período",
        "reg_ans": "Registro ANS",
        "nome_fantasia": "Operadora",
        "modalidade": "Modalidade",
        "porte": "Porte",
        "total_beneficiarios": "Beneficiários",
    }
    indicator_renaming = {indicator.column: indicator.name for indicator in INDICATORS}

    with tab_indicadores:
        indicator_data_raw = df[indicator_cols].copy()
        indicator_display = indicator_data_raw.rename(columns=renaming | indicator_renaming)
        for indicator in INDICATORS:
            col_name = indicator_renaming[indicator.column]
            indicator_display[col_name] = indicator_display[col_name].apply(lambda x: format_metric(x, indicator.kind))
        st.dataframe(
            indicator_display,
            use_container_width=True,
            hide_index=True,
        )
        st.download_button(
            "Download Indicadores (CSV)",
            indicator_data_raw.rename(columns=renaming | indicator_renaming).to_csv(index=False).encode("utf-8"),
            file_name="indicadores_rn518_odonto.csv",
            mime="text/csv",
        )

    with tab_componentes:
        component_columns = [component.column for component in COMPONENTS if component.column in df.columns]
        if not component_columns:
            st.info("Componentes contábeis não foram incluídos nesta base.")
            return
        component_renaming = {
            component.column: component.name for component in COMPONENTS if component.column in component_columns
        }
        base_cols = ["periodo_label", "reg_ans", "nome_fantasia", "modalidade", "porte"]
        available_base = [col for col in base_cols if col in df.columns]
        renamed_cols = component_renaming | renaming
        component_data = df[available_base + component_columns].rename(columns=renamed_cols)
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


def show_segment_comparison(
    filtered_df: pd.DataFrame,
    segment_df: pd.DataFrame,
    selected_filters: dict,
) -> None:
    st.subheader("Comparativo Operadora x Segmento")
    if filtered_df.empty:
        st.info("Sem dados para as operadoras selecionadas.")
        return
    selected_operadoras = selected_filters.get("operadoras") or []
    if not selected_operadoras:
        st.info("Selecione uma operadora na barra lateral para visualizar o comparativo com modalidade e porte.")
        return
    available_ops = (
        filtered_df[["reg_ans", "operadora_label"]]
        .drop_duplicates()
        .set_index("reg_ans")["operadora_label"]
        .to_dict()
    )
    default_operadora = next((reg for reg in selected_operadoras if reg in available_ops), None)
    if default_operadora is None:
        st.info("Operadora selecionada não possui dados nos filtros atuais.")
        return
    operadora_choices = [reg for reg in selected_operadoras if reg in available_ops]
    if len(operadora_choices) > 1:
        selected_id = st.selectbox(
            "Operadora para comparação",
            options=operadora_choices,
            format_func=lambda reg: available_ops.get(reg, reg),
            index=operadora_choices.index(default_operadora),
        )
    else:
        selected_id = default_operadora

    op_df = filtered_df[filtered_df["reg_ans"] == selected_id]
    if op_df.empty:
        st.info("Dados da operadora selecionada não encontrados nos filtros atuais.")
        return

    modality_value = next((val for val in op_df["modalidade"].dropna().astype(str) if val.strip()), "")
    porte_value = next((val for val in op_df["porte"].dropna().astype(str) if val.strip()), "")

    indicator_columns = [indicator.column for indicator in INDICATORS]
    operadora_avg = op_df[indicator_columns].mean(numeric_only=True)

    modalidade_df = (
        segment_df[segment_df["modalidade"] == modality_value] if modality_value else pd.DataFrame()
    )
    porte_df = segment_df[segment_df["porte"] == porte_value] if porte_value else pd.DataFrame()

    modalidade_avg = modalidade_df[indicator_columns].mean(numeric_only=True) if not modalidade_df.empty else pd.Series()
    porte_avg = porte_df[indicator_columns].mean(numeric_only=True) if not porte_df.empty else pd.Series()

    records: list[dict[str, str]] = []
    for indicator in INDICATORS:
        op_value = operadora_avg.get(indicator.column)
        mod_value = modalidade_avg.get(indicator.column) if not modalidade_avg.empty else None
        porte_value_avg = porte_avg.get(indicator.column) if not porte_avg.empty else None

        records.append(
            {
                "Indicador": indicator.name,
                "Operadora": format_metric(op_value, indicator.kind),
                "Modalidade (média)": format_metric(mod_value, indicator.kind) if modality_value else "—",
                "Δ Modalidade": format_difference(
                    op_value - mod_value if mod_value is not None and not pd.isna(mod_value) else None,
                    indicator.kind,
                )
                if modality_value
                else "—",
                "Porte (média)": format_metric(porte_value_avg, indicator.kind) if porte_value else "—",
                "Δ Porte": format_difference(
                    op_value - porte_value_avg
                    if porte_value_avg is not None and not pd.isna(porte_value_avg)
                    else None,
                    indicator.kind,
                )
                if porte_value
                else "—",
            }
        )

    comparison_df = pd.DataFrame(records)
    st.dataframe(
        comparison_df,
        use_container_width=True,
        hide_index=True,
    )
    st.caption(
        "Diferenças (Δ) mostram quanto a operadora se distancia da média do segmento selecionado, "
        "considerando os mesmos filtros de período aplicados."
    )


def show_financial_panel(df: pd.DataFrame) -> None:
    st.subheader("Painel financeiro (valores trimestrais)")
    if df.empty:
        st.info("Sem valores financeiros para os filtros aplicados.")
        return

    df = append_period_columns(df)
    value_columns = [component.column for component in COMPONENTS if component.column in df.columns]
    if not value_columns:
        st.info("Valores contábeis agregados não estão disponíveis na base atual.")
        return

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
    st.subheader("Correlação despesas administrativas x ROE")
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
    subset["roe"] = subset["retorno_patrimonio_liquido"]
    subset["base_tamanho"] = subset.get("qt_beneficiarios_periodo", pd.Series(0, index=subset.index)).fillna(0)
    if "operadora_label" not in subset.columns:
        subset["operadora_label"] = subset["reg_ans"]

    chart = (
        alt.Chart(subset)
        .mark_circle()
        .encode(
            x=alt.X("pct_da:Q", title="% Despesas Administrativas", axis=alt.Axis(format="%", tickCount=6)),
            y=alt.Y("roe:Q", title="Retorno sobre PL (ROE)", axis=alt.Axis(format="%", tickCount=6)),
            size=alt.Size(
                "base_tamanho:Q",
                title="Beneficiários (tamanho relativo)",
                scale=alt.Scale(type="sqrt"),
            ),
            color=alt.Color("operadora_label:N", title="Operadora"),
            tooltip=[
                alt.Tooltip("operadora_label:N", title="Operadora"),
                alt.Tooltip("pct_da:Q", title="% DA", format=".2%"),
                alt.Tooltip("roe:Q", title="ROE", format=".2%"),
                alt.Tooltip("base_tamanho:Q", title="Beneficiários", format=",.0f"),
            ],
        )
    )
    st.altair_chart(chart, use_container_width=True)
    resumo = subset[
        ["operadora_label", "pct_da", "roe", "base_tamanho"]
    ].copy()
    resumo.rename(
        columns={
            "operadora_label": "Operadora",
            "pct_da": "% Desp. Adm",
            "roe": "ROE",
            "base_tamanho": "Beneficiários",
        },
        inplace=True,
    )
    resumo["% Desp. Adm"] = resumo["% Desp. Adm"].apply(lambda x: format_metric(x, "pct"))
    resumo["ROE"] = resumo["ROE"].apply(lambda x: format_metric(x, "pct"))
    resumo["Beneficiários"] = resumo["Beneficiários"].apply(lambda x: f"{x:,.0f}".replace(",", "."))
    st.dataframe(resumo, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(
        page_title="Painel RN 518 – Indicadores Odonto",
        page_icon="📊",
        layout="wide",
    )
    st.title("Painel RN 518 – Indicadores Odontológicos")
    st.caption(
        "Dados carregados do arquivo local `dados/csv_completao_2trimestre25.parquet` (RN 518 completo + indicadores oficiais)."
    )

    options = load_filter_options()
    selected_filters = render_filters(options)

    if not selected_filters["anos"] or not selected_filters["trimestres"]:
        st.info("Selecione pelo menos um ano e um trimestre na barra lateral para visualizar o painel.")
        return

    data = load_indicators(selected_filters)

    if data.empty:
        st.warning("Nenhum registro encontrado para os filtros aplicados.")
        return

    data = append_period_columns(data)
    segment_filters = dict(selected_filters)
    segment_filters["operadoras"] = []
    segment_df = load_indicators(segment_filters)
    segment_df = append_period_columns(segment_df)
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
        show_segment_comparison(data, segment_df, selected_filters)
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
