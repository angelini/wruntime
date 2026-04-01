use anyhow::{Context, Result};
use clap::{Args, Subcommand};

use wr_engine::config::EngineConfig;
use wr_engine::pool::module_schema;

#[derive(Args)]
pub struct DbArgs {
    #[command(subcommand)]
    pub command: DbCommand,
}

#[derive(Subcommand)]
pub enum DbCommand {
    /// Drop and recreate every module schema listed in an engine config.
    /// Removes all tables, data, and migration history.
    Reset {
        /// Path to an engine TOML config file
        config: String,
        /// Override the database URL from the config file
        #[arg(long, env = "WRUNTIME_EXAMPLE_DB_URL")]
        database_url: Option<String>,
    },
}

pub async fn run(args: DbArgs) -> Result<()> {
    match args.command {
        DbCommand::Reset {
            config,
            database_url,
        } => reset(&config, database_url.as_deref()).await,
    }
}

async fn reset(config_path: &str, database_url: Option<&str>) -> Result<()> {
    let config = EngineConfig::load(config_path)?;

    let db = config
        .database
        .as_ref()
        .context("no [database] section in engine config")?;

    let url = database_url.unwrap_or(&db.url);
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .context("failed to connect to database")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("database connection error: {e}");
        }
    });

    let mut reset_count = 0u32;
    for module in &config.modules {
        if !module.database {
            continue;
        }
        let schema = module_schema(&module.namespace, &module.name);

        client
            .execute(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"), &[])
            .await
            .with_context(|| format!("failed to drop schema '{schema}'"))?;

        client
            .execute(&format!("CREATE SCHEMA \"{schema}\""), &[])
            .await
            .with_context(|| format!("failed to recreate schema '{schema}'"))?;

        println!("  reset {schema}");
        reset_count += 1;
    }

    if reset_count == 0 {
        println!("No database-enabled modules found in config.");
    } else {
        println!(
            "Reset {reset_count} module schema(s). Migrations will re-run on next engine start."
        );
    }
    Ok(())
}
