#!/usr/bin/env python3
"""
FSAE data ingestion — PDF → Hive-partitioned CSV + Parquet.

Scans ./pdf/ for SAE results PDFs, extracts year from filename, and writes
each event table to both:
    ./csv/competition={comp}/year={year}/{event}.csv
    ./parquet/competition={comp}/year={year}/{event}.parquet

Page layout is detected dynamically from column count and header patterns so
the same script handles different year PDFs without hardcoded page ranges.

Usage:
    uv run main.py                        # process all PDFs in ./pdf/
    uv run main.py pdf/fsae_2024.pdf      # process a specific PDF
    uv run main.py --competition lincoln pdf/fsae_lincoln_2025.pdf
"""

import argparse
import re
from pathlib import Path

import pandas as pd
import pdfplumber

# ---------------------------------------------------------------------------
# Column schemas, keyed by (event, ncols) to handle year-to-year variations.
# ---------------------------------------------------------------------------
EVENT_COLS: dict[tuple[str, int], list[str]] = {
    # ── Modern era (2020+) ──────────────────────────────────────────────────
    ("overall", 13): [
        "Place", "CarNum", "Team", "Penalty",
        "CostScore", "PresentationScore", "DesignScore", "AccelerationScore",
        "SkidPadScore", "AutocrossScore", "EnduranceScore", "EfficiencyScore", "TotalScore",
    ],
    ("design", 8): [
        "Place", "CarNum", "Team", "DocumentPenalty", "RawScore", "LatePenalty", "Status", "Score",
    ],
    ("presentation", 6): ["Place", "CarNum", "Team", "RawScore", "Penalty", "Score"],
    ("presentation", 7): ["Place", "CarNum", "Team", "Status", "RawScore", "Penalty", "Score"],
    ("cost", 10): [
        "Place", "CarNum", "Team", "AdjustedCost",
        "PriceScore", "CostAccuracy", "EngineeringDesign", "ScenarioScore", "Penalty", "Score",
    ],
    ("accel", 18): [
        "Place", "CarNum", "Team",
        "Run1_Time", "Run1_Cones", "Run1_AdjTime",
        "Run2_Time", "Run2_Cones", "Run2_AdjTime",
        "Run3_Time", "Run3_Cones", "Run3_AdjTime",
        "Run4_Time", "Run4_Cones", "Run4_AdjTime",
        "BestTime", "Penalty", "Score",
    ],
    ("skid", 22): [
        "Place", "CarNum", "Team",
        "D1R1_Right", "D1R1_Left", "D1R1_Cones", "D1R1_AdjTime",
        "D1R2_Right", "D1R2_Left", "D1R2_Cones", "D1R2_AdjTime",
        "D2R1_Right", "D2R1_Left", "D2R1_Cones", "D2R1_AdjTime",
        "D2R2_Right", "D2R2_Left", "D2R2_Cones", "D2R2_AdjTime",
        "BestTime", "Penalty", "Score",
    ],
    ("autocross", 22): [
        "Place", "CarNum", "Team",
        "Run1_Time", "Run1_Cones", "Run1_OffCourse", "Run1_AdjTime",
        "Run2_Time", "Run2_Cones", "Run2_OffCourse", "Run2_AdjTime",
        "Run3_Time", "Run3_Cones", "Run3_OffCourse", "Run3_AdjTime",
        "Run4_Time", "Run4_Cones", "Run4_OffCourse", "Run4_AdjTime",
        "BestTime", "Penalty", "Score",
    ],
    ("endurance", 12): [
        "Place", "CarNum", "Team", "Time", "Laps", "Cones", "OffCourse",
        "OtherPenalty", "AdjTime", "TimeScore", "LapScore", "EnduranceScore",
    ],
    ("efficiency", 11): [
        "Place", "CarNum", "Team", "AvgLapAdjTime", "CompletedLaps",
        "FuelUsed_L", "CO2_kg", "CO2PerLap", "FuelType", "FuelEfficiency", "Score",
    ],
    ("enduranceLap", 13): ["Team", "CarNum"] + [f"Lap{i}" for i in range(1, 12)],
    ("team_information", 7): [
        "CarNum", "Team", "Country", "EngineCylinders", "Displacement_cc", "Weight_kg", "Weight_lbs",
    ],

    # ── 2023: cost scoring breakdown changed (9 cols) ────────────────────────
    ("cost", 9): [
        "Place", "CarNum", "Team", "AdjustedCost",
        "PriceScore", "CostAccuracy", "EngineeringDesign", "Penalty", "Score",
    ],

    # ── 2018: cost with 11-col breakdown (AuditCost added) ───────────────────
    ("cost", 11): [
        "Place", "CarNum", "Team", "AdjustedCost",
        "PriceScore", "AuditCost", "ReportScore", "FeasibilityContent", "CaseVisual",
        "Penalty", "Score",
    ],

    # ── 2014–2019 era (char-rotated headers, fewer cols) ─────────────────────
    # design: 7-col (RawScore, Penalty, Status, Score — no DocumentPenalty/LatePenalty split)
    # reuses the same ("design", 7) schema as the 2013 reversed era
    # presentation: simplified to Place, CarNum, Team, Score
    ("presentation", 4): ["Place", "CarNum", "Team", "Score"],
    # accel/skid/autocross: no Penalty column
    ("accel", 17): [
        "Place", "CarNum", "Team",
        "Run1_Time", "Run1_Cones", "Run1_AdjTime",
        "Run2_Time", "Run2_Cones", "Run2_AdjTime",
        "Run3_Time", "Run3_Cones", "Run3_AdjTime",
        "Run4_Time", "Run4_Cones", "Run4_AdjTime",
        "BestTime", "Score",
    ],
    ("skid", 21): [
        "Place", "CarNum", "Team",
        "D1R1_Right", "D1R1_Left", "D1R1_Cones", "D1R1_AdjTime",
        "D1R2_Right", "D1R2_Left", "D1R2_Cones", "D1R2_AdjTime",
        "D2R1_Right", "D2R1_Left", "D2R1_Cones", "D2R1_AdjTime",
        "D2R2_Right", "D2R2_Left", "D2R2_Cones", "D2R2_AdjTime",
        "BestTime", "Score",
    ],
    ("autocross", 21): [
        "Place", "CarNum", "Team",
        "Run1_Time", "Run1_Cones", "Run1_OffCourse", "Run1_AdjTime",
        "Run2_Time", "Run2_Cones", "Run2_OffCourse", "Run2_AdjTime",
        "Run3_Time", "Run3_Cones", "Run3_OffCourse", "Run3_AdjTime",
        "Run4_Time", "Run4_Cones", "Run4_OffCourse", "Run4_AdjTime",
        "BestTime", "Score",
    ],
    # endurance: no separate TimeScore/LapScore
    ("endurance", 10): [
        "Place", "CarNum", "Team", "Time", "Laps", "Cones", "OffCourse",
        "OtherPenalty", "AdjTime", "EnduranceScore",
    ],

    # ── 2011–2013 reversed-text era ───────────────────────────────────────────
    ("overall", 12): [
        "Place", "CarNum", "Team", "Penalty",
        "CostScore", "PresentationScore", "DesignScore", "AccelerationScore",
        "SkidPadScore", "AutocrossScore", "EnduranceEconomyScore", "TotalScore",
    ],
    # design: 5-col (2011–2012) and 7-col (2013)
    ("design", 5): ["Place", "CarNum", "Team", "Status", "Score"],
    ("design", 7): ["Place", "CarNum", "Team", "RawScore", "Penalty", "Status", "Score"],
    # cost simple (before detailed breakdown existed)
    ("cost", 4): ["Place", "CarNum", "Team", "Score"],
    # cost detailed — uses "cost_legacy" event name to avoid schema conflict with modern 9/10-col
    ("cost_legacy", 9): [
        "Place", "CarNum", "Team", "AdjustedCost",
        "PriceScore", "RealCaseScore", "ReportFeasibility", "Penalty", "Score",
    ],
    ("cost_legacy", 10): [
        "Place", "CarNum", "Team", "AdjustedCost",
        "PriceScore", "ReportScore", "FeasibilityContent", "CaseVisual", "Penalty", "Score",
    ],
    # efficiency: old 7-col format (FuelUsed + FuelType + AdjFuel + Score)
    ("efficiency", 7): [
        "Place", "CarNum", "Team", "FuelUsed_L", "FuelType", "AdjFuelUsed_L", "Score",
    ],
    # endurance: old 9-col (no Laps column, 2010 – but data is garbled; kept for completeness)
    ("endurance", 9): [
        "Place", "CarNum", "Team", "Time", "Cones", "OffCourse",
        "OtherPenalty", "AdjTime", "EnduranceScore",
    ],
}

# Years where pdfplumber cannot cleanly extract cell boundaries
_GARBLED_YEARS = {2008, 2010}


# ---------------------------------------------------------------------------
# Page classification
# ---------------------------------------------------------------------------

def _is_rotated(cell: str | None) -> bool:
    """Header text rendered as rotated characters (one char per line)."""
    return bool(cell and "\n" in cell)


def _is_reversed(cell: str | None) -> bool:
    """Header text rendered mirrored/reversed ('Place' → 'ecalP'), 2008–2013 era."""
    return cell == "ecalP"


def classify_page(
    table: list[list],
    seen_efficiency: bool,
    seen_dynamic: bool,
    seen_design: bool,
) -> tuple[str, int] | None:
    """
    Return (event_name, skip_rows) or None if unclassifiable.
    skip_rows = number of leading header rows to discard before data.
    seen_dynamic  = True once accel/skid/autocross has been processed;
                    disambiguates events sharing a column count.
    seen_design   = True once design has been processed;
                    disambiguates 7-col design (2014–2019) from 7-col presentation (2020+).
    """
    ncols = len(table[0])
    h0 = table[0][0]

    # ── Reversed-text era (2008–2013): h0 == "ecalP" ─────────────────────────
    if _is_reversed(h0):
        if ncols in (12, 13):
            return ("overall", 1)
        if ncols == 5:
            return ("design", 1)
        if ncols == 7:
            # 2013: 7-col design appears before dynamic events;
            # 2011–2012: 7-col efficiency appears after
            return ("design", 1) if not seen_dynamic else ("efficiency", 1)
        if ncols == 4:
            return ("cost", 1)
        if ncols == 9:
            return ("cost_legacy", 1)
        if ncols == 10:
            # before dynamic events → detailed cost; after → endurance
            return ("cost_legacy", 1) if not seen_dynamic else ("endurance", 1)
        if ncols == 11:
            return ("efficiency", 1)
        return None

    # ── enduranceLap: "School"-headed table (2013–modern, variable lap count) ─
    if (h0 or "").startswith("School"):
        return ("enduranceLap", 1)

    # ── enduranceLap: team name as first cell, no header row (2011–2012) ──────
    # Distinguishing feature: many cols, non-special h0, appears after dynamic events.
    if h0 and not _is_rotated(h0) and ncols >= 20 and seen_dynamic:
        return ("enduranceLap", 0)

    # ── Dynamic events: group-header row first (h0 is None or a group label) ──
    if ncols in (17, 18) and not _is_rotated(h0):
        return ("accel", 2)

    if ncols in (21, 22) and not _is_rotated(h0):
        # Skid has "Driver" (modern) or "D1R1" (old) in the group-header row
        has_skid = any("Driver" in (c or "") or "D1R1" in (c or "") for c in table[0])
        return ("skid" if has_skid else "autocross", 2)

    # ── Rotated / char-rotated headers (2014+) ────────────────────────────────
    if not _is_rotated(h0):
        return None

    if ncols == 13:
        return ("overall", 1)

    if ncols == 8:
        return ("design", 1)

    # 2014–2019: design is 7-col and appears before presentation (seen_design=False).
    # 2020+: design is 8-col; a 7-col after that → presentation handled below.
    if ncols == 7 and not seen_design and not seen_efficiency:
        return ("design", 1)

    # presentation: 4-col (2014–2019) or 6/7-col (2020+), appears before efficiency
    if ncols in (4, 6, 7) and not seen_efficiency:
        return ("presentation", 1)

    # cost: before dynamic events; 2018 has 11-col, others 9 or 10
    if ncols in (9, 10, 11) and not seen_dynamic:
        return ("cost", 1)

    # endurance: 10 or 12 cols, appears after dynamic events
    if ncols in (10, 12) and seen_dynamic:
        return ("endurance", 1)

    # efficiency: 11-col, appears after dynamic events
    if ncols == 11 and seen_dynamic:
        return ("efficiency", 1)

    # team_information: 7 rotated cols, appears after efficiency
    if ncols == 7 and seen_efficiency:
        return ("team_information", 1)

    return None


# ---------------------------------------------------------------------------
# Cleaning
# ---------------------------------------------------------------------------

_SENTINELS = {"", "-", "DNF", "DNS", "DSQ", "pres only", "RFP", "N/A"}


def _clean_cell(val: str | None) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return None if s in _SENTINELS else s


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.map(_clean_cell)
    if "Place" in df.columns:
        # Strip tie marker: "106 T" → "106"
        df["Place"] = df["Place"].str.replace(r"\s*T$", "", regex=True)
    return df


# ---------------------------------------------------------------------------
# PDF processing
# ---------------------------------------------------------------------------

def extract_year(filename: str) -> int | None:
    m = re.search(r"(?<!\d)(20\d{2})(?!\d)", filename)
    return int(m.group(1)) if m else None


def _endurance_lap_cols(ncols: int) -> list[str]:
    """Generate column names for variable-length enduranceLap tables."""
    nlaps = ncols - 2
    return ["Team", "CarNum"] + [f"Lap{i}" for i in range(1, nlaps + 1)]


def process_pdf(
    pdf_path: Path,
    competition: str,
    year: int,
    csv_dir: str,
    parquet_dir: str,
) -> None:
    if year in _GARBLED_YEARS:
        print(f"  Skipping: PDF encoding for {year} produces garbled cell extraction")
        return

    csv_out = Path(csv_dir) / f"competition={competition}" / f"year={year}"
    parquet_out = Path(parquet_dir) / f"competition={competition}" / f"year={year}"
    csv_out.mkdir(parents=True, exist_ok=True)
    parquet_out.mkdir(parents=True, exist_ok=True)

    event_rows: dict[str, list] = {}
    seen_efficiency = False
    seen_dynamic = False
    seen_design = False

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables:
                continue
            table = tables[0]
            result = classify_page(table, seen_efficiency, seen_dynamic, seen_design)
            if result is None:
                continue
            event, skip = result
            if event == "efficiency":
                seen_efficiency = True
            if event in ("accel", "skid", "autocross"):
                seen_dynamic = True
            # Only mark design seen for 8-col modern design; 7-col design (2014–2019)
            # spans multiple pages and must not trigger the modern presentation path.
            if event == "design" and len(table[0]) == 8:
                seen_design = True

            data = [r for r in table[skip:] if any(c for c in r)]
            event_rows.setdefault(event, []).extend(data)

    for event, rows in event_rows.items():
        ncols = len(rows[0]) if rows else 0
        cols = EVENT_COLS.get((event, ncols))

        # Variable-length enduranceLap (old formats with more than 11 laps)
        if cols is None and event == "enduranceLap" and ncols > 2:
            cols = _endurance_lap_cols(ncols)

        if cols is None:
            print(f"  {event}: unknown schema ({ncols} cols) — skipped")
            continue

        rows = [r for r in rows if len(r) == len(cols)]
        df = pd.DataFrame(rows, columns=cols)
        df = clean_df(df)

        df.to_csv(csv_out / f"{event}.csv", index=False)
        df.to_parquet(parquet_out / f"{event}.parquet", index=False, compression="zstd", engine="pyarrow")
        print(f"  {event}: {len(df)} rows")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("pdfs", nargs="*", help="PDF files to process (default: all in --pdf-dir)")
    parser.add_argument("--competition", default="michigan")
    parser.add_argument("--pdf-dir", default="./pdf", help="Directory to scan for PDFs (default: ./pdf)")
    parser.add_argument("--csv-dir", default="./csv", help="CSV output root (default: ./csv)")
    parser.add_argument("--parquet-dir", default="./parquet", help="Parquet output root (default: ./parquet)")
    args = parser.parse_args()

    pdf_files = [Path(p) for p in args.pdfs] if args.pdfs else sorted(Path(args.pdf_dir).glob("*.pdf"))

    if not pdf_files:
        print(f"No PDFs found in {args.pdf_dir}")
        return

    for pdf_path in pdf_files:
        year = extract_year(pdf_path.name)
        if year is None:
            print(f"Skipping {pdf_path.name}: no year found in filename")
            continue
        print(f"\n{pdf_path.name} → {args.competition} {year}")
        process_pdf(pdf_path, args.competition, year, args.csv_dir, args.parquet_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
