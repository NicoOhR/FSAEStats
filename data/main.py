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
}


# ---------------------------------------------------------------------------
# Page classification
# ---------------------------------------------------------------------------

def _is_rotated(cell: str | None) -> bool:
    return bool(cell and "\n" in cell)


def classify_page(table: list[list], seen_efficiency: bool) -> tuple[str, int] | None:
    """
    Return (event_name, skip_rows) for a table, or None if unclassifiable.
    skip_rows is the number of leading rows that are headers, not data.
    """
    ncols = len(table[0])
    h0 = table[0][0]  # first cell of first row

    # enduranceLap: pdfplumber reads the header as readable text
    if ncols == 13 and (h0 or "").startswith("School"):
        return ("enduranceLap", 1)

    # overall: 13 rotated-header cols
    if ncols == 13 and _is_rotated(h0):
        return ("overall", 1)

    if ncols == 8 and _is_rotated(h0):
        return ("design", 1)

    # presentation: 6 or 7 cols, rotated, appears before efficiency
    if ncols in (6, 7) and _is_rotated(h0) and not seen_efficiency:
        return ("presentation", 1)

    if ncols == 10 and _is_rotated(h0):
        return ("cost", 1)

    # accel: 18 cols, first row is a group header (not rotated)
    if ncols == 18 and not _is_rotated(h0):
        return ("accel", 2)

    # skid vs autocross: both 22 cols; skid has "Driver" in group headers
    if ncols == 22 and not _is_rotated(h0):
        has_driver = any("Driver" in (c or "") for c in table[0])
        return ("skid" if has_driver else "autocross", 2)

    if ncols == 12 and _is_rotated(h0):
        return ("endurance", 1)

    if ncols == 11 and _is_rotated(h0):
        return ("efficiency", 1)

    # team_information: 7 rotated cols, appears after efficiency
    if ncols == 7 and _is_rotated(h0) and seen_efficiency:
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


def process_pdf(
    pdf_path: Path,
    competition: str,
    year: int,
    csv_dir: str,
    parquet_dir: str,
) -> None:
    csv_out = Path(csv_dir) / f"competition={competition}" / f"year={year}"
    parquet_out = Path(parquet_dir) / f"competition={competition}" / f"year={year}"
    csv_out.mkdir(parents=True, exist_ok=True)
    parquet_out.mkdir(parents=True, exist_ok=True)

    # Accumulate rows per event across pages
    event_rows: dict[str, list] = {}
    seen_efficiency = False

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables:
                continue
            table = tables[0]
            result = classify_page(table, seen_efficiency)
            if result is None:
                continue
            event, skip = result
            if event == "efficiency":
                seen_efficiency = True

            data = [r for r in table[skip:] if any(c for c in r)]
            event_rows.setdefault(event, []).extend(data)

    for event, rows in event_rows.items():
        ncols = len(rows[0]) if rows else 0
        cols = EVENT_COLS.get((event, ncols))
        if cols is None:
            print(f"  {event}: unknown schema ({ncols} cols) — skipped")
            continue

        # Drop rows where column count doesn't match (merged footer cells, etc.)
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
