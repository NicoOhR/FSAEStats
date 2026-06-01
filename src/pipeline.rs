use serde::Deserialize;

#[derive(Deserialize)]
enum PipeLineOp {}

#[derive(Deserialize)]
enum Comps {
    MichiganIc,
    MichiganEv,
}

#[derive(Deserialize)]
pub struct Pipeline(Vec<PipeLineOp>);

#[derive(Deserialize)]
pub struct Source {
    view: String,
    years: Vec<u16>,
    competitions: Vec<Comps>,
}
