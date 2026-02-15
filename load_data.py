# ============================================================
# Abies – end-to-end pipeline:
# PostgreSQL -> pandas -> grafy -> profesionálny PDF (ReportLab)
#
# GitHub-ready verzia:
# - bez hesiel v kóde (konfigurácia cez .env)
# - výstupy do report_outputs/ (ignorované v .gitignore)
# - logo v assets/abies_logo.jpg (voliteľné)
# ============================================================

import os
from datetime import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# načítanie .env (lokálne), na GitHub ide len .env.example
from dotenv import load_dotenv
load_dotenv()

# ReportLab
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak
)
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics

# Slovenská diakritika: nájdeme DejaVu Sans cez Matplotlib font manager
import matplotlib.font_manager as fm


# ----------------------------
# KONFIGURÁCIA VÝSTUPOV
# ----------------------------
OUTPUT_DIR = "report_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Logo (odporúčané): assets/abies_logo.jpg
LOGO_PATH = os.getenv("ABIES_LOGO_PATH", "assets/abies_logo.jpg")

PDF_PATH = os.path.join(OUTPUT_DIR, "abies_report_profi.pdf")

# Ak chceš striktne reportovať len 2021–2025, nechaj tak.
# Ak chceš reportovať všetko, nastav FILTER_START napr. na "1900-01-01" a FILTER_END_EXCL na "2100-01-01".
FILTER_START = os.getenv("FILTER_START", "2021-01-01")
FILTER_END_EXCL = os.getenv("FILTER_END_EXCL", "2026-01-01")  # exkluzívny koniec (t.j. do 2025-12-31 vrátane)


# ----------------------------
# DB PRIPOJENIE (cez .env)
# ----------------------------
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE")

# Názvy view/tabuliek v DB (môžeš zmeniť v .env)
VIEW_MONTHLY_KPI = os.getenv("VIEW_MONTHLY_KPI", "rpt_monthly_kpi")
VIEW_BOOKINGS_ALL = os.getenv("VIEW_BOOKINGS_ALL", "abies_bookings_all")


def require_env(var_name: str, value: str | None) -> str:
    if not value:
        raise SystemExit(
            f"❌ Chýba konfigurácia {var_name}. "
            f"Skontroluj .env (lokálne) alebo nastav premennú prostredia."
        )
    return value


def get_engine():
    user = require_env("PG_USER", PG_USER)
    pwd = require_env("PG_PASSWORD", PG_PASSWORD)
    db = require_env("PG_DATABASE", PG_DATABASE)

    conn_str = f"postgresql+psycopg2://{user}:{pwd}@{PG_HOST}:{PG_PORT}/{db}"
    return create_engine(conn_str)


# ----------------------------
# 1) Načítanie dát
# ----------------------------
def load_monthly_kpi(engine) -> pd.DataFrame:
    query = f"""
    select *
    from {VIEW_MONTHLY_KPI}
    order by year, month_num, portal
    """
    df = pd.read_sql(query, engine)
    df["month"] = pd.to_datetime(df["month"])
    df["commission"] = df["commission"].fillna(0)
    df["revenue_net"] = df["revenue_net"].fillna(df["revenue_gross"])

    df = df[(df["month"] >= FILTER_START) & (df["month"] < FILTER_END_EXCL)]
    return df


def load_bookings(engine) -> pd.DataFrame:
    query_bookings = f"""
    select
      prichod::date as prichod,
      odchod::date as odchod,
      pocet_noci::int as pocet_noci,
      cena::numeric as cena,
      coalesce(provizia,0)::numeric as provizia
    from {VIEW_BOOKINGS_ALL}
    where prichod is not null
      and odchod is not null
      and odchod > prichod
      and pocet_noci > 0
    order by prichod;
    """
    df_b = pd.read_sql(query_bookings, engine)
    df_b["prichod"] = pd.to_datetime(df_b["prichod"])
    df_b["odchod"] = pd.to_datetime(df_b["odchod"])

    # filter podľa príchodu
    df_b = df_b[(df_b["prichod"] >= FILTER_START) & (df_b["prichod"] < FILTER_END_EXCL)]
    return df_b


# ----------------------------
# 2) Occupancy + RevPAR v Pythone
# ----------------------------
def compute_monthly_occupancy_revpar(df_b: pd.DataFrame) -> pd.DataFrame:
    if df_b.empty:
        raise SystemExit("❌ df_b je prázdny — view abies_bookings_all nevrátilo žiadne rezervácie (po filtri).")


    start_date = df_b["prichod"].min().normalize()

    # ohraničenie kalendára na FILTER_END_EXCL (aby sa neobjavil napr. 2026 v grafoch)
    end_limit = pd.to_datetime(FILTER_END_EXCL) - pd.Timedelta(days=1)
    end_date = min((df_b["odchod"].max() - pd.Timedelta(days=1)).normalize(), end_limit)

    calendar = pd.DataFrame({"date": pd.date_range(start_date, end_date, freq="D")})
    calendar["month"] = calendar["date"].values.astype("datetime64[M]")

    rows = []
    for r in df_b.itertuples(index=False):
        nights = pd.date_range(r.prichod, r.odchod - pd.Timedelta(days=1), freq="D")
        daily_rev = float(r.cena) / int(r.pocet_noci)
        daily_comm = float(r.provizia) / int(r.pocet_noci)
        for d in nights:
            d = d.normalize()
            if d > end_date:
                continue
            rows.append((d, daily_rev, daily_comm))

    daily = pd.DataFrame(rows, columns=["date", "daily_rev", "daily_comm"])

    daily_agg = daily.groupby("date", as_index=False).agg(
        revenue_gross=("daily_rev", "sum"),
        commission=("daily_comm", "sum"),
        occ_cnt=("daily_rev", "size"),
    )
    daily_agg["occupied_flag"] = np.where(daily_agg["occ_cnt"] > 0, 1, 0)
    daily_agg = daily_agg.drop(columns=["occ_cnt"])

    daily_full = calendar.merge(daily_agg, on="date", how="left")
    daily_full["revenue_gross"] = daily_full["revenue_gross"].fillna(0)
    daily_full["commission"] = daily_full["commission"].fillna(0)
    daily_full["occupied_flag"] = daily_full["occupied_flag"].fillna(0)

    monthly = daily_full.groupby("month", as_index=False).agg(
        available_nights=("date", "count"),
        occupied_nights=("occupied_flag", "sum"),
        revenue_gross=("revenue_gross", "sum"),
        commission=("commission", "sum"),
    )

    monthly["revenue_net"] = monthly["revenue_gross"] - monthly["commission"]
    monthly["occupancy_pct"] = 100.0 * monthly["occupied_nights"] / monthly["available_nights"]
    monthly["revpar_gross"] = monthly["revenue_gross"] / monthly["available_nights"]
    monthly["revpar_net"] = monthly["revenue_net"] / monthly["available_nights"]

    monthly["year"] = monthly["month"].dt.year
    monthly["month_num"] = monthly["month"].dt.month

    return monthly.sort_values("month")


# ----------------------------
# 3) KPI agregácie
# ----------------------------
def compute_yearly_kpi(df: pd.DataFrame) -> pd.DataFrame:
    yearly = (
        df.groupby("year", as_index=False)
          .agg(revenue_gross=("revenue_gross", "sum"),
               revenue_net=("revenue_net", "sum"),
               commission=("commission", "sum"),
               nights_sold=("nights_sold", "sum"),
               bookings=("bookings", "sum"))
    )
    yearly["adr_weighted"] = yearly["revenue_gross"] / yearly["nights_sold"]
    return yearly.sort_values("year")


def compute_portal_kpi(df: pd.DataFrame) -> pd.DataFrame:
    by_portal = (
        df.groupby("portal", as_index=False)
          .agg(revenue_gross=("revenue_gross", "sum"),
               revenue_net=("revenue_net", "sum"),
               commission=("commission", "sum"),
               nights_sold=("nights_sold", "sum"),
               bookings=("bookings", "sum"))
    )
    by_portal["commission_pct"] = 100.0 * by_portal["commission"] / by_portal["revenue_gross"]
    by_portal["adr_weighted"] = by_portal["revenue_gross"] / by_portal["nights_sold"]
    return by_portal.sort_values("revenue_gross", ascending=False)


def compute_monthly_total(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby("month", as_index=False)["revenue_gross"].sum().sort_values("month")


# ----------------------------
# 4) Grafy (PNG) pre vloženie do PDF
# ----------------------------
def save_charts(monthly_total: pd.DataFrame, monthly_occ: pd.DataFrame) -> dict:
    paths = {}

    p1 = os.path.join(OUTPUT_DIR, "chart_monthly_revenue.png")
    plt.figure(figsize=(10, 4))
    plt.plot(monthly_total["month"], monthly_total["revenue_gross"])
    plt.title("Mesačné tržby (gross)")
    plt.xlabel("Mesiac")
    plt.ylabel("Tržby (€)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(p1, dpi=160)
    plt.close()
    paths["revenue"] = p1

    p2 = os.path.join(OUTPUT_DIR, "chart_occupancy.png")
    plt.figure(figsize=(10, 4))
    plt.plot(monthly_occ["month"], monthly_occ["occupancy_pct"])
    plt.title("Obsadenosť % (mesačne)")
    plt.xlabel("Mesiac")
    plt.ylabel("Obsadenosť (%)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(p2, dpi=160)
    plt.close()
    paths["occupancy"] = p2

    p3 = os.path.join(OUTPUT_DIR, "chart_revpar.png")
    plt.figure(figsize=(10, 4))
    plt.plot(monthly_occ["month"], monthly_occ["revpar_gross"])
    plt.title("RevPAR (gross) (mesačne)")
    plt.xlabel("Mesiac")
    plt.ylabel("RevPAR (€)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(p3, dpi=160)
    plt.close()
    paths["revpar"] = p3

    return paths


# ----------------------------
# 5) ReportLab PDF (logo + layout + diakritika)
# ----------------------------
def register_slovak_font() -> str:
    font_path = fm.findfont("DejaVu Sans", fallback_to_default=True)
    font_name = "DejaVuSans"
    try:
        pdfmetrics.registerFont(TTFont(font_name, font_path))
        return font_name
    except Exception:
        return "Helvetica"


def money(x) -> str:
    return f"{float(x):,.2f} €".replace(",", " ").replace(".", ",")


def pct(x) -> str:
    return f"{float(x):,.2f} %".replace(".", ",")


def num(x) -> str:
    return str(int(x))


def build_pdf_report(df: pd.DataFrame,
                     yearly: pd.DataFrame,
                     by_portal: pd.DataFrame,
                     monthly_occ: pd.DataFrame,
                     chart_paths: dict):
    font_name = register_slovak_font()

    doc = SimpleDocTemplate(
        PDF_PATH,
        pagesize=A4,
        leftMargin=2.0*cm, rightMargin=2.0*cm,
        topMargin=1.8*cm, bottomMargin=1.8*cm
    )

    styles = getSampleStyleSheet()
    base = ParagraphStyle("Base", parent=styles["Normal"], fontName=font_name, fontSize=10.5, leading=14)
    h1 = ParagraphStyle("H1", parent=styles["Heading1"], fontName=font_name, fontSize=18, leading=22, spaceAfter=10)
    h2 = ParagraphStyle("H2", parent=styles["Heading2"], fontName=font_name, fontSize=13.5, leading=18, spaceBefore=10, spaceAfter=6)
    small = ParagraphStyle("Small", parent=base, fontName=font_name, fontSize=9.5, leading=12, textColor=colors.grey)

    elements = []

    # Header
    if os.path.exists(LOGO_PATH):
        logo = Image(LOGO_PATH)
        logo.drawHeight = 2.2*cm
        logo.drawWidth = 2.2*cm
    else:
        logo = Paragraph("<b>ABIES</b>", h1)

    title_block = [
        Paragraph("<b>ABIES APARTMÁN – Manažérsky report</b>", h1),
        Paragraph(f"Generované: {datetime.now().strftime('%Y-%m-%d %H:%M')}", base),
        Paragraph(f"Obdobie: {FILTER_START} až {(pd.to_datetime(FILTER_END_EXCL) - pd.Timedelta(days=1)):%Y-%m-%d}", base),
    ]

    header = Table([[logo, title_block]], colWidths=[2.6*cm, 12.4*cm])
    header.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("LEFTPADDING", (0,0), (-1,-1), 0),
        ("RIGHTPADDING", (0,0), (-1,-1), 0),
        ("BOTTOMPADDING", (0,0), (-1,-1), 6),
    ]))
    elements.append(header)
    elements.append(Spacer(1, 6))
    elements.append(Table([[""]], colWidths=[15*cm], rowHeights=[0.8], style=[("LINEABOVE", (0,0), (-1,-1), 1, colors.lightgrey)]))
    elements.append(Spacer(1, 10))

    # Executive Summary
    elements.append(Paragraph("Executive Summary", h2))

    total_gross = df["revenue_gross"].sum()
    total_net = df["revenue_net"].sum()
    total_comm = df["commission"].sum()
    total_nights = df["nights_sold"].sum()
    total_bookings = df["bookings"].sum()
    adr_weighted = (total_gross / total_nights) if total_nights else 0

    avg_occ = monthly_occ["occupancy_pct"].mean()
    avg_revpar = monthly_occ["revpar_gross"].mean()

    top_portal = by_portal.sort_values("revenue_net", ascending=False).head(1)
    top_portal_name = top_portal["portal"].iloc[0] if not top_portal.empty else "-"

    kpi_data = [
        ["Tržby (gross)", money(total_gross), "Tržby (net)", money(total_net)],
        ["Provízie", money(total_comm), "ADR (vážené)", money(adr_weighted)],
        ["Obsadenosť (avg)", pct(avg_occ), "RevPAR (avg)", money(avg_revpar)],
        ["Počet nocí", num(total_nights), "Rezervácie", num(total_bookings)],
        ["Najvýnosnejší portál", top_portal_name, "", ""],
    ]
    kpi_tbl = Table(kpi_data, colWidths=[4.2*cm, 3.3*cm, 4.2*cm, 3.3*cm])
    kpi_tbl.setStyle(TableStyle([
        ("BOX", (0,0), (-1,-1), 0.8, colors.lightgrey),
        ("INNERGRID", (0,0), (-1,-1), 0.4, colors.lightgrey),
        ("FONTNAME", (0,0), (-1,-1), font_name),
        ("FONTSIZE", (0,0), (-1,-1), 10),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("ALIGN", (1,0), (1,-1), "RIGHT"),
        ("ALIGN", (3,0), (3,-1), "RIGHT"),
        ("LEFTPADDING", (0,0), (-1,-1), 8),
        ("RIGHTPADDING", (0,0), (-1,-1), 8),
        ("TOPPADDING", (0,0), (-1,-1), 6),
        ("BOTTOMPADDING", (0,0), (-1,-1), 6),
    ]))
    elements.append(kpi_tbl)
    elements.append(Spacer(1, 10))
    elements.append(Paragraph("Pozn.: Occupancy/RevPAR počítané na báze dostupných nocí (1 apartmán).", small))

    elements.append(PageBreak())

    # Ročné KPI
    elements.append(Paragraph("Ročné KPI", h2))

    yearly_show = yearly.copy()
    yearly_show["revenue_gross"] = yearly_show["revenue_gross"].map(money)
    yearly_show["revenue_net"] = yearly_show["revenue_net"].map(money)
    yearly_show["commission"] = yearly_show["commission"].map(money)
    yearly_show["adr_weighted"] = yearly_show["adr_weighted"].map(money)
    yearly_show["nights_sold"] = yearly_show["nights_sold"].map(num)
    yearly_show["bookings"] = yearly_show["bookings"].map(num)

    yearly_cols = ["year", "revenue_gross", "revenue_net", "commission", "adr_weighted", "nights_sold", "bookings"]
    yearly_header = ["Rok", "Tržby gross", "Tržby net", "Provízie", "ADR", "Noci", "Rezervácie"]
    yearly_table_data = [yearly_header] + yearly_show[yearly_cols].values.tolist()

    t = Table(yearly_table_data, repeatRows=1,
              colWidths=[1.4*cm, 3.0*cm, 3.0*cm, 2.6*cm, 2.2*cm, 1.7*cm, 2.1*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#E8EEF6")),
        ("FONTNAME", (0,0), (-1,-1), font_name),
        ("FONTSIZE", (0,0), (-1,-1), 9.5),
        ("GRID", (0,0), (-1,-1), 0.4, colors.lightgrey),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("ALIGN", (1,1), (-1,-1), "RIGHT"),
        ("LEFTPADDING", (0,0), (-1,-1), 6),
        ("RIGHTPADDING", (0,0), (-1,-1), 6),
        ("TOPPADDING", (0,0), (-1,-1), 5),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 14))

    # KPI podľa portálu
    elements.append(Paragraph("KPI podľa portálu (celé obdobie)", h2))

    portal_show = by_portal.copy()
    portal_show["revenue_gross"] = portal_show["revenue_gross"].map(money)
    portal_show["revenue_net"] = portal_show["revenue_net"].map(money)
    portal_show["commission"] = portal_show["commission"].map(money)
    portal_show["commission_pct"] = portal_show["commission_pct"].map(pct)
    portal_show["adr_weighted"] = portal_show["adr_weighted"].map(money)
    portal_show["nights_sold"] = portal_show["nights_sold"].map(num)
    portal_show["bookings"] = portal_show["bookings"].map(num)

    portal_cols = ["portal", "revenue_gross", "revenue_net", "commission", "commission_pct", "adr_weighted", "nights_sold", "bookings"]
    portal_header = ["Portál", "Gross", "Net", "Provízie", "Provízia %", "ADR", "Noci", "Rezervácie"]
    portal_table_data = [portal_header] + portal_show[portal_cols].values.tolist()

    t2 = Table(portal_table_data, repeatRows=1,
               colWidths=[2.2*cm, 2.5*cm, 2.5*cm, 2.3*cm, 2.2*cm, 2.0*cm, 1.6*cm, 1.9*cm])
    t2.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#E8EEF6")),
        ("FONTNAME", (0,0), (-1,-1), font_name),
        ("FONTSIZE", (0,0), (-1,-1), 9.2),
        ("GRID", (0,0), (-1,-1), 0.4, colors.lightgrey),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("ALIGN", (1,1), (-1,-1), "RIGHT"),
        ("LEFTPADDING", (0,0), (-1,-1), 6),
        ("RIGHTPADDING", (0,0), (-1,-1), 6),
        ("TOPPADDING", (0,0), (-1,-1), 5),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
    ]))
    elements.append(t2)

    elements.append(PageBreak())

    # Grafy
    elements.append(Paragraph("Trendy a sezónnosť", h2))
    elements.append(Paragraph("Nižšie sú kľúčové trendové grafy generované automaticky z dát.", base))
    elements.append(Spacer(1, 10))

    def add_chart(title: str, path: str):
        elements.append(Paragraph(title, ParagraphStyle("ChartTitle", parent=base, fontName=font_name, fontSize=12, spaceAfter=6)))
        if os.path.exists(path):
            img = Image(path)
            img.drawWidth = 16.0*cm
            img.drawHeight = 6.3*cm
            elements.append(img)
            elements.append(Spacer(1, 10))
        else:
            elements.append(Paragraph(f"(Graf nenájdený: {path})", small))
            elements.append(Spacer(1, 10))

    add_chart("Mesačné tržby (gross)", chart_paths["revenue"])
    add_chart("Obsadenosť % (mesačne)", chart_paths["occupancy"])
    add_chart("RevPAR (gross) (mesačne)", chart_paths["revpar"])

    elements.append(PageBreak())

    # Insights
    elements.append(Paragraph("Insights & odporúčania", h2))

    top_revpar = monthly_occ.sort_values("revpar_gross", ascending=False).head(1)
    top_month = top_revpar["month"].iloc[0].strftime("%Y-%m") if not top_revpar.empty else "-"
    top_revpar_val = float(top_revpar["revpar_gross"].iloc[0]) if not top_revpar.empty else 0

    most_exp = by_portal.sort_values("commission_pct", ascending=False).head(1)
    most_exp_portal = most_exp["portal"].iloc[0] if not most_exp.empty else "-"
    most_exp_pct = float(most_exp["commission_pct"].iloc[0]) if not most_exp.empty else 0

    insight_lines = [
        f"• Najvýnosnejší portál podľa netto: <b>{top_portal_name}</b>.",
        f"• Najlepší mesiac podľa RevPAR: <b>{top_month}</b> (RevPAR: <b>{money(top_revpar_val)}</b>).",
        f"• Provízne najdrahší kanál: <b>{most_exp_portal}</b> (≈ {pct(most_exp_pct)}).",
        f"• Priemerná obsadenosť: <b>{pct(avg_occ)}</b>; Priemerný RevPAR: <b>{money(avg_revpar)}</b>.",
        "",
        "<b>Odporúčania:</b>",
        "• V peak mesiacoch testovať vyšší ADR (cena za noc) – cieľ zvýšiť RevPAR.",
        "• Podporiť priame rezervácie (nižšie náklady distribúcie).",
        "• V slabších mesiacoch cieliť dlhšie pobyty / promo balíčky.",
    ]
    elements.append(Paragraph("<br/>".join(insight_lines), base))
    elements.append(Spacer(1, 14))
    elements.append(Paragraph("© Abies Apartmán – interný analytický report", small))

    doc.build(elements)


def main():
    engine = get_engine()
    print("✅ Pripojenie vytvorené")

    df = load_monthly_kpi(engine)
    df_b = load_bookings(engine)

    print("\n📊 Prvé riadky rpt_monthly_kpi:")
    print(df.head())

    print("\n🧾 Rezervácie (sample):")
    print(df_b.head())

    monthly_occ = compute_monthly_occupancy_revpar(df_b)

    yearly = compute_yearly_kpi(df)
    by_portal = compute_portal_kpi(df)
    monthly_total = compute_monthly_total(df)

    # export CSV – (report_outputs je v .gitignore)
    yearly.to_csv(os.path.join(OUTPUT_DIR, "yearly_kpi.csv"), index=False)
    by_portal.to_csv(os.path.join(OUTPUT_DIR, "portal_kpi.csv"), index=False)
    monthly_total.to_csv(os.path.join(OUTPUT_DIR, "monthly_trend.csv"), index=False)
    monthly_occ.to_csv(os.path.join(OUTPUT_DIR, "monthly_occupancy_revpar_py.csv"), index=False)

    chart_paths = save_charts(monthly_total, monthly_occ)
    build_pdf_report(df, yearly, by_portal, monthly_occ, chart_paths)

    print(f"\n📄 PDF uložený: {PDF_PATH}")
    print("🚀 Hotovo.")


if __name__ == "__main__":
    main()
