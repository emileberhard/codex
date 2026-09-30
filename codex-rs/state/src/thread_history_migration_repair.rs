use sqlx::Executor;
use sqlx::Sqlite;
use sqlx::SqlitePool;
use sqlx::migrate::Migration;
use sqlx::migrate::Migrator;

const UPSTREAM_LIFECYCLE_TIMESTAMPS_VERSION: i64 = 7;
const LEGACY_STARTED_AT_VERSION: i64 = 10;
const LEGACY_STARTED_AT_CHECKSUM: &str = "C4676D588F66B243C48F5B9ECE2DCF70338475DC176D45C94DCB201BE711232C3DB6F15D62BCB16202E0C68C54DE7E69";

struct LegacyPrivateMigration {
    legacy_version: i64,
    legacy_checksum: &'static str,
    private_version: i64,
}

const LEGACY_LIFECYCLE: LegacyPrivateMigration = LegacyPrivateMigration {
    legacy_version: 7,
    legacy_checksum: "797AF21EC65D4A53AA438BB2F147203AE900F4C753B3C5CB5B8C33031BBD8AFDEBC41AFA81F327971DCD9A0508FC73A4",
    private_version: 1_000_001,
};

const LEGACY_PRIVATE_MIGRATIONS: [LegacyPrivateMigration; 3] = [
    LEGACY_LIFECYCLE,
    LegacyPrivateMigration {
        legacy_version: 8,
        legacy_checksum: "F80B721E6B83601CA48FDE446D7CB8B61D7DB6FFBC47F202EDF26D28B5070FD18E78BC073D74BEC934FBF9BE222A0E04",
        private_version: 1_000_002,
    },
    LegacyPrivateMigration {
        legacy_version: 9,
        legacy_checksum: "99F9D1AE40E67909874069780B70D60EB872758FF69332F43AB149D5EA2CDD22919999ABAC5335BB1D0C7C065AC56B67",
        private_version: 1_000_003,
    },
];

pub(crate) async fn repair_legacy_private_thread_history_versions(
    pool: &SqlitePool,
    migrator: &Migrator,
) -> anyhow::Result<()> {
    let Some(upstream_lifecycle) = migration(migrator, UPSTREAM_LIFECYCLE_TIMESTAMPS_VERSION)
    else {
        return Ok(());
    };
    let Some(private_migrations) = LEGACY_PRIVATE_MIGRATIONS
        .iter()
        .map(|legacy| migration(migrator, legacy.private_version).map(|private| (legacy, private)))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(());
    };
    let migrations_table_exists = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'",
    )
    .fetch_optional(pool)
    .await?
    .is_some();
    if !migrations_table_exists || !legacy_lifecycle_applied(pool).await? {
        return Ok(());
    }

    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    if !legacy_lifecycle_applied(&mut *transaction).await? {
        return Ok(());
    }
    let started_at_exists = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM pragma_table_info('thread_items') WHERE name = 'started_at_ms'",
    )
    .fetch_optional(&mut *transaction)
    .await?
    .is_some();
    if !started_at_exists {
        sqlx::query("ALTER TABLE thread_items ADD COLUMN started_at_ms INTEGER")
            .execute(&mut *transaction)
            .await?;
    }
    for (legacy, private) in private_migrations {
        sqlx::query(
            "UPDATE _sqlx_migrations SET version = ?, description = ?, checksum = ? WHERE version = ? AND hex(checksum) = ?",
        )
        .bind(private.version)
        .bind(private.description.as_ref())
        .bind(private.checksum.as_ref())
        .bind(legacy.legacy_version)
        .bind(legacy.legacy_checksum)
        .execute(&mut *transaction)
        .await?;
    }
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = ? AND hex(checksum) = ?")
        .bind(LEGACY_STARTED_AT_VERSION)
        .bind(LEGACY_STARTED_AT_CHECKSUM)
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES (?, ?, TRUE, ?, 0)",
    )
    .bind(upstream_lifecycle.version)
    .bind(upstream_lifecycle.description.as_ref())
    .bind(upstream_lifecycle.checksum.as_ref())
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

fn migration(migrator: &Migrator, version: i64) -> Option<&Migration> {
    migrator
        .migrations
        .iter()
        .find(|migration| migration.version == version)
}

async fn legacy_lifecycle_applied<'e>(
    executor: impl Executor<'e, Database = Sqlite>,
) -> sqlx::Result<bool> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM _sqlx_migrations WHERE version = ? AND hex(checksum) = ?",
    )
    .bind(LEGACY_LIFECYCLE.legacy_version)
    .bind(LEGACY_LIFECYCLE.legacy_checksum)
    .fetch_optional(executor)
    .await?
    .is_some())
}

#[cfg(test)]
#[path = "thread_history_migration_repair_tests.rs"]
mod tests;
