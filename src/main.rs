use actix_web::{HttpResponse, HttpServer, web, App};

#[actix_web::main]
async fn main() -> std::io::Result<()>{
    HttpServer::new(|| App::new.route("/", web::get().to(HttpResponse::Ok()))) //creates workers based on cores on system
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}