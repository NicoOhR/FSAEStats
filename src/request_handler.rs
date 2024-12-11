use crate::db_structs;
use crate::request_parser::{Event, EventRequest};
use serde::Serialize;
use sqlx::{sqlite::SqlitePool, FromRow};

pub async fn create_pool() -> Result<SqlitePool, sqlx::Error> {
    let database_url = "sqlite://race.db";
    Ok(SqlitePool::connect(database_url).await?)
}

pub async fn make_event_query<T>(request: EventRequest, pool: SqlitePool) -> Result<T, sqlx::Error>
where
    T: for<'r> FromRow<'r, sqlx::sqlite::SqliteRow> + Send + Unpin,
{
    let table = match request.event {
        Event::Autocross => "autocross_results",
        Event::Accel => "accel_results",
        Event::Endurance => "endurance_results",
        Event::Skidpad => "skipad_results",
    };
    let query = format!("SELECT * FROM {} WHERE Team = ?", table);
    let row = sqlx::query_as::<_, T>(&query)
        .bind(request.team)
        .fetch_one(&pool)
        .await?;
    Ok(row)
}

//I dislike this
#[derive(Debug, Serialize)]
pub enum EventResult {
    Autocross(db_structs::AutocrossResults),
    Accel(db_structs::AccelResults),
    Endurance(db_structs::EnduranceResults),
    Skidpad(db_structs::SkidResults),
}

pub async fn request_handler(
    request: EventRequest,
    pool: SqlitePool,
) -> Result<EventResult, Box<dyn std::error::Error + Send + Sync>> {
    match request.event {
        Event::Autocross => {
            let result: db_structs::AutocrossResults = make_event_query(request, pool).await?;
            Ok(EventResult::Autocross(result))
        }
        Event::Accel => {
            let result: db_structs::AccelResults = make_event_query(request, pool).await?;
            Ok(EventResult::Accel(result))
        }
        Event::Endurance => {
            let result: db_structs::EnduranceResults = make_event_query(request, pool).await?;
            Ok(EventResult::Endurance(result))
        }
        Event::Skidpad => {
            let result: db_structs::SkidResults = make_event_query(request, pool).await?;
            Ok(EventResult::Skidpad(result))
        }
    }
}
