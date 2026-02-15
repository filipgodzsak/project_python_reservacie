# project_python_reservacie

End‑to‑end analytický projekt nad rezervačnými dátami apartmánu (2021–2025).

## Čo projekt robí
- načíta mesačné KPI z PostgreSQL view `rpt_monthly_kpi`
- načíta booking-level dáta z `abies_bookings_all`
- v Pythone vypočíta hotelové KPI: **Occupancy** a **RevPAR**
- vygeneruje grafy (PNG) a profesionálny PDF report (ReportLab) s logom

## Architektúra
PostgreSQL → SQL views → Python (pandas) → Matplotlib grafy → ReportLab PDF

## Spustenie
1) Nainštaluj knižnice:
```bash
python -m pip install -r requirements.txt
```

2) Vytvor si lokálne `.env` podľa `.env.example` (NEcommituj).

3) Spusti:
```bash
python load_data.py
```

## Výstupy
Generujú sa do `report_outputs/` (tento priečinok je v `.gitignore`).
- `abies_report_profi.pdf`
- `chart_*.png`
- `*.csv`

## Poznámka (GDPR)
Reálne dáta s menami hostí nie sú súčasťou repozitára.
