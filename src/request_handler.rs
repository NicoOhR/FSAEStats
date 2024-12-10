use crate::request_parser::{self, Event, EventRequest, ParseError};

use sqlx::{
    database, query,
    sqlite::{SqlitePool, SqliteRow},
    Row,
};

pub async fn create_pool() -> Result<SqlitePool, sqlx::Error> {
    let database_url = "sqlite://race.db";
    Ok(SqlitePool::connect(database_url).await?)
}

pub async fn make_event_query(
    request: EventRequest,
    pool: SqlitePool,
) -> Result<sqlx::sqlite::SqliteRow, sqlx::Error> {
    let table = match request.event {
        Event::Autocross => "autocross_results",
        Event::Accel => "accel_results",
        Event::Endurance => "endurance_results",
        Event::Skidpad => "skipad_results",
    };
    let query = format!("SELECT * FROM {} WHERE Team = ?", table);
    let row = sqlx::query(&query)
        .bind(request.team)
        .fetch_one(&pool)
        .await?;
    Ok(row)
}

pub async fn request_handler(
    request: EventRequest,
    pool: SqlitePool,
) -> Result<SqliteRow, ParseError> {
    let row = make_event_query(request, pool).await.unwrap();
    //println!("{:?}", row.columns());
    Ok(row)
}
