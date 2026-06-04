use crate::apply::{Apply, ApplyState};
use crate::validate::Validate;
use enum_dispatch::enum_dispatch;
use polars::prelude::*;
use serde::Deserialize;
use strum_macros::{Display, EnumString};

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
#[enum_dispatch(Validate, Apply)]
pub enum PipelineOp {
    // Transform Ops
    Filter(FilterOp),
    Select(SelectOp),
    Group(GroupOp),
    Join(JoinOp),
    // Analytic Ops
    Sort(SortOp),
    Normalize(NormOp),
    Ranked(RankOp),
    YearDelta(YearDeltaOp),
    // Inference Ops
    Weakness(WeaknessOp),
}
#[derive(Deserialize)]
pub struct FilterOp {
    pub teams: Option<Vec<String>>,
    pub years: Option<Vec<u16>>,
    pub events: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub struct SelectOp {
    pub columns: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupAgg {
    Sum,
    Mean,
    Max,
    Min,
    Count,
}

#[derive(Deserialize)]
pub struct GroupOp {
    pub by: Vec<String>,
    pub agg: GroupAgg,
}

#[derive(Deserialize)]
pub struct JoinOp {
    pub view: View,
    pub on: String,
}

#[derive(Deserialize)]
pub struct SortOp {
    pub by: String,
    pub descending: Option<bool>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NormMethod {
    Zscore,
    PctGap,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NormWithin {
    Year,
    Competition,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RankWithin {
    Year,
    Competition,
    Event,
}

#[derive(Deserialize)]
pub struct NormOp {
    pub column: String,
    pub method: NormMethod,
    pub within: NormWithin,
}

#[derive(Deserialize)]
pub struct RankOp {
    pub column: String,
    pub within: RankWithin,
}

#[derive(Deserialize)]
pub struct YearDeltaOp {
    pub column: String,
    pub team: String,
}

#[derive(Deserialize)]
pub struct WeaknessOp {
    pub team: String,
}

#[derive(Display, Deserialize, EnumString, Debug)]
#[strum(serialize_all = "snake_case")]
pub enum Comps {
    MichiganIc,
    MichiganEv,
}

#[derive(Deserialize)]
pub struct Pipeline(pub Vec<PipelineOp>);

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum View {
    CompetitionResults,
    OverallStandings,
    DynamicEvents,
    StaticEvents,
    EnduranceLaps,
    TeamProfile,
    FieldSummary,
    // Event detail views
    AccelDetail,
    SkidPadDetail,
    AutocrossDetail,
    EnduranceDetail,
    EfficiencyDetail,
    DesignDetail,
    PresentationDetail,
    CostDetail,
}

#[derive(Deserialize)]
pub struct Source {
    pub view: View,
    pub years: Vec<i32>,
    pub competitions: Vec<Comps>,
}

impl View {
    /// Columns present in the `LazyFrame` produced by [`Source::create_frame`] for
    /// this view. This is the authoritative schema that pipeline ops are validated
    /// against.
    pub fn columns(&self) -> &'static [&'static str] {
        match self {
            View::CompetitionResults | View::OverallStandings | View::FieldSummary => &[
                "Place",
                "CarNum",
                "Team",
                "Penalty",
                "CostScore",
                "PresentationScore",
                "DesignScore",
                "AccelerationScore",
                "SkidPadScore",
                "AutocrossScore",
                "EnduranceScore",
                "EfficiencyScore",
                "TotalScore",
                "year",
                "competition",
            ],
            View::DynamicEvents => &[
                "Place",
                "CarNum",
                "Team",
                "AccelerationScore",
                "SkidPadScore",
                "AutocrossScore",
                "EnduranceScore",
                "EfficiencyScore",
            ],
            View::StaticEvents => &[
                "Place",
                "CarNum",
                "Team",
                "CostScore",
                "PresentationScore",
                "DesignScore",
            ],
            View::EnduranceLaps => &[
                "Team",
                "CarNum",
                "Lap1",
                "Lap2",
                "Lap3",
                "Lap4",
                "Lap5",
                "Lap6",
                "Lap7",
                "Lap8",
                "Lap9",
                "Lap10",
                "Lap11",
                "year",
                "competition",
            ],
            View::TeamProfile => &[
                "CarNum",
                "Team",
                "Country",
                "EngineCylinders",
                "Displacement_cc",
                "Weight_kg",
                "Weight_lbs",
                "year",
                "competition",
            ],
            View::AccelDetail => &[
                "Place",
                "CarNum",
                "Team",
                "Run1_Time",
                "Run1_Cones",
                "Run1_AdjTime",
                "Run2_Time",
                "Run2_Cones",
                "Run2_AdjTime",
                "Run3_Time",
                "Run3_Cones",
                "Run3_AdjTime",
                "Run4_Time",
                "Run4_Cones",
                "Run4_AdjTime",
                "BestTime",
                "Penalty",
                "Score",
                "year",
                "competition",
            ],
            View::SkidPadDetail => &[
                "Place",
                "CarNum",
                "Team",
                "D1R1_Right",
                "D1R1_Left",
                "D1R1_Cones",
                "D1R1_AdjTime",
                "D1R2_Right",
                "D1R2_Left",
                "D1R2_Cones",
                "D1R2_AdjTime",
                "D2R1_Right",
                "D2R1_Left",
                "D2R1_Cones",
                "D2R1_AdjTime",
                "D2R2_Right",
                "D2R2_Left",
                "D2R2_Cones",
                "D2R2_AdjTime",
                "BestTime",
                "Penalty",
                "Score",
                "year",
                "competition",
            ],
            View::AutocrossDetail => &[
                "Place",
                "CarNum",
                "Team",
                "Run1_Time",
                "Run1_Cones",
                "Run1_OffCourse",
                "Run1_AdjTime",
                "Run2_Time",
                "Run2_Cones",
                "Run2_OffCourse",
                "Run2_AdjTime",
                "Run3_Time",
                "Run3_Cones",
                "Run3_OffCourse",
                "Run3_AdjTime",
                "Run4_Time",
                "Run4_Cones",
                "Run4_OffCourse",
                "Run4_AdjTime",
                "BestTime",
                "Penalty",
                "Score",
                "year",
                "competition",
            ],
            View::EnduranceDetail => &[
                "Place",
                "CarNum",
                "Team",
                "Time",
                "Laps",
                "Cones",
                "OffCourse",
                "OtherPenalty",
                "AdjTime",
                "TimeScore",
                "LapScore",
                "EnduranceScore",
                "year",
                "competition",
            ],
            View::EfficiencyDetail => &[
                "Place",
                "CarNum",
                "Team",
                "AvgLapAdjTime",
                "CompletedLaps",
                "FuelUsed_L",
                "CO2_kg",
                "CO2PerLap",
                "FuelType",
                "FuelEfficiency",
                "Score",
                "year",
                "competition",
            ],
            View::DesignDetail => &[
                "Place",
                "CarNum",
                "Team",
                "DocumentPenalty",
                "RawScore",
                "LatePenalty",
                "Status",
                "Score",
                "year",
                "competition",
            ],
            // `Status` only appears in the 7-column presentation variant; some years
            // have 6 columns and omit it.
            View::PresentationDetail => &[
                "Place",
                "CarNum",
                "Team",
                "Status",
                "RawScore",
                "Penalty",
                "Score",
                "year",
                "competition",
            ],
            View::CostDetail => &[
                "Place",
                "CarNum",
                "Team",
                "AdjustedCost",
                "PriceScore",
                "CostAccuracy",
                "EngineeringDesign",
                "ScenarioScore",
                "Penalty",
                "Score",
                "year",
                "competition",
            ],
        }
    }
}

impl Source {
    pub fn create_frame(&self) -> PolarsResult<LazyFrame> {
        let years = Series::new("".into(), self.years.as_slice());
        let comps = Series::new(
            "".into(),
            self.competitions
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<String>>(),
        );

        let scan = |path: &str| -> PolarsResult<LazyFrame> {
            Ok(
                LazyFrame::scan_parquet(path.into(), ScanArgsParquet::default())?
                    .filter(col("year").is_in(lit(years.clone()), false))
                    .filter(col("competition").is_in(lit(comps.clone()), false)),
            )
        };

        let lf = match self.view {
            View::CompetitionResults | View::OverallStandings | View::FieldSummary => {
                scan("data/parquet/**/overall.parquet")?
            }
            View::DynamicEvents => scan("data/parquet/**/overall.parquet")?.select([
                col("Place"),
                col("CarNum"),
                col("Team"),
                col("AccelerationScore"),
                col("SkidPadScore"),
                col("AutocrossScore"),
                col("EnduranceScore"),
                col("EfficiencyScore"),
            ]),
            View::StaticEvents => scan("data/parquet/**/overall.parquet")?.select([
                col("Place"),
                col("CarNum"),
                col("Team"),
                col("CostScore"),
                col("PresentationScore"),
                col("DesignScore"),
            ]),
            View::EnduranceLaps => scan("data/parquet/**/enduranceLap.parquet")?,
            View::TeamProfile => scan("data/parquet/**/team_information.parquet")?,
            View::AccelDetail => scan("data/parquet/**/accel.parquet")?,
            View::SkidPadDetail => scan("data/parquet/**/skid.parquet")?,
            View::AutocrossDetail => scan("data/parquet/**/autocross.parquet")?,
            View::EnduranceDetail => scan("data/parquet/**/endurance.parquet")?,
            View::EfficiencyDetail => scan("data/parquet/**/efficiency.parquet")?,
            View::DesignDetail => scan("data/parquet/**/design.parquet")?,
            View::PresentationDetail => scan("data/parquet/**/presentation.parquet")?,
            View::CostDetail => scan("data/parquet/**/cost.parquet")?,
        };

        Ok(lf)
    }
}
