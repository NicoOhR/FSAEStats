use serde::Serialize;
use sqlx::FromRow;
use struct_iterable::Iterable;

#[derive(FromRow, Iterable, Debug, Serialize)]
pub struct AutocrossResults {
    pub place: Option<String>,
    pub carnum: Option<i32>,
    pub team: Option<String>,
    pub run1time: Option<f64>,
    pub run1cones: Option<f64>,
    pub run1offcourse: Option<f64>,
    pub run1adjtime: Option<String>,
    pub run2time: Option<f64>,
    pub run2cones: Option<f64>,
    pub run2offcourse: Option<f64>,
    pub run2adjtime: Option<String>,
    pub run3time: Option<f64>,
    pub run3cones: Option<f64>,
    pub run3offcourse: Option<f64>,
    pub run3adjtime: Option<String>,
    pub run4time: Option<f64>,
    pub run4cones: Option<f64>,
    pub run4offcourse: Option<f64>,
    pub run4adjtime: Option<String>,
    pub besttime: Option<String>,
    pub penalty: Option<f64>,
    pub score: Option<f64>,
}

#[derive(FromRow, Iterable, Debug, Serialize)]
pub struct AccelResults {
    pub place: Option<String>,
    pub carnum: Option<i32>,
    pub team: Option<String>,
    pub run1time: Option<f64>,
    pub run1cones: Option<f64>,
    pub run1adjtime: Option<String>,
    pub run2time: Option<f64>,
    pub run2cones: Option<f64>,
    pub run2adjtime: Option<String>,
    pub run3time: Option<f64>,
    pub run3cones: Option<f64>,
    pub run3adjtime: Option<String>,
    pub run4time: Option<f64>,
    pub run4cones: Option<f64>,
    pub run4adjtime: Option<String>,
    pub besttime: Option<String>,
    pub penalty: Option<f64>,
    pub score: Option<f64>,
}

#[derive(FromRow, Iterable, Debug, Serialize)]
pub struct TeamInformationResults {
    pub carnum: Option<String>,
    pub team: Option<String>,
    pub country: Option<String>,
    pub cylinders: Option<i32>,
    pub displacement: Option<f64>,
    pub weightkg: Option<f64>,
    pub weightlbs: Option<f64>,
}

#[derive(FromRow, Iterable, Debug, Serialize)]
pub struct SkidResults {
    pub place: Option<String>,
    pub carnum: Option<i32>,
    pub team: Option<String>,
    pub driver1run1timer: Option<f64>,
    pub driver1run1timel: Option<f64>,
    pub driver1run1cones: Option<f64>,
    pub driver1run1: Option<String>,
    pub driver1run1adjtime: Option<f64>,
    pub driver1run2timer: Option<f64>,
    pub driver1run2timel: Option<f64>,
    pub driver1run2cones: Option<String>,
    pub driver1run2adjtime: Option<f64>,
    pub driver2run1timer: Option<f64>,
    pub driver2run1timel: Option<f64>,
    pub driver2run1cones: Option<String>,
    pub driver2run1adjtime: Option<f64>,
    pub driver2run2timer: Option<f64>,
    pub driver2run2timel: Option<f64>,
    pub driver2run2cones: Option<String>,
    pub driver2run2adjtime: Option<String>,
    pub besttime: Option<f64>,
    pub penalty: Option<f64>,
    pub score: Option<f64>,
}

#[derive(FromRow, Iterable, Debug, Serialize)]
pub struct OverallResults {
    pub place: Option<f64>,
    pub carnum: Option<i32>,
    pub team: Option<String>,
    pub penalty: Option<f64>,
    pub costscore: Option<f64>,
    pub presentationscore: Option<f64>,
    pub designscore: Option<f64>,
    pub accelscore: Option<f64>,
    pub skidpadscore: Option<f64>,
    pub autocrossscore: Option<f64>,
    pub endurance: Option<f64>,
    pub efficiencyscore: Option<f64>,
    pub totalscore: Option<String>,
}

#[derive(FromRow, Iterable, Debug, Serialize)]
pub struct EfficiencyResults {
    pub place: Option<String>,
    pub carnum: Option<String>,
    pub team: Option<String>,
    pub averageadj: Option<String>,
    pub lapscompleted: Option<String>,
    pub fuelused: Option<String>,
    pub adjco2: Option<String>,
    pub avgadjco2perlap: Option<String>,
    pub fueltype: Option<String>,
    pub fuelefficiencyfactor: Option<String>,
    pub fuelefficiencyscore: Option<String>,
}

#[derive(FromRow, Iterable, Debug, Serialize)]
pub struct DesignResults {
    pub place: Option<String>,
    pub carnum: Option<i32>,
    pub team: Option<String>,
    pub documentpenalty: Option<f64>,
    pub rawscore: Option<f64>,
    pub latepenalty: Option<f64>,
    pub status: Option<String>,
    pub score: Option<f64>,
}

#[derive(FromRow, Iterable, Debug, Serialize)]
pub struct CostResults {
    pub place: Option<f64>,
    pub carnum: Option<i32>,
    pub team: Option<String>,
    pub adjusted: Option<String>,
    pub pricescore30: Option<f64>,
    pub costaccuracy15: Option<f64>,
    pub engineeringdrawings15: Option<f64>,
    pub scenarioscore40: Option<f64>,
    pub penalty: Option<f64>,
    pub score: Option<f64>,
}

#[derive(FromRow, Iterable, Debug, Serialize)]
pub struct PresentationResults {
    pub place: Option<String>,
    pub carnum: Option<i32>,
    pub team: Option<String>,
    pub rawscore: Option<f64>,
    pub penalty: Option<f64>,
    pub score: Option<f64>,
}

#[derive(FromRow, Iterable, Debug, Serialize)]
pub struct EnduranceResults {
    pub place: Option<f64>,
    pub carnum: Option<i32>,
    pub team: Option<String>,
    pub time: Option<f64>,
    pub laps: Option<i32>,
    pub cones: Option<f64>,
    pub offcourse: Option<f64>,
    pub otherpenalty: Option<f64>,
    pub adjtime: Option<String>,
    pub timescore: Option<String>,
    pub lapsscore: Option<i32>,
    pub endurancescore: Option<f64>,
}
