use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use sqlx::AssertSqlSafe;
use sqlx::Row;
use sqlx::SqlSafeStr;
use sqlx::SqlitePool;
use sqlx::migrate::Migration;
use sqlx::migrate::Migrator;
use std::borrow::Cow;

use super::LEGACY_PRIVATE_MIGRATIONS;
use super::LEGACY_STARTED_AT_CHECKSUM;
use super::LEGACY_STARTED_AT_VERSION;
use super::UPSTREAM_LIFECYCLE_TIMESTAMPS_VERSION;
use crate::migrations::THREAD_HISTORY_MIGRATOR;
use crate::migrations::runtime_thread_history_migrator;

fn current_migration(version: i64) -> &'static Migration {
    THREAD_HISTORY_MIGRATOR
        .migrations
        .iter()
        .find(|migration| migration.version == version)
        .expect("thread history migration should exist")
}

fn legacy_migration(version: i64, description: &'static str, sql: String) -> Migration {
    let template = current_migration(1_000_001);
    Migration::new(
        version,
        Cow::Borrowed(description),
        template.migration_type,
        AssertSqlSafe(sql).into_sql_str(),
        template.no_tx,
    )
}

fn thread_history_migrator(migrations: Vec<Migration>) -> Migrator {
    Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: true,
        locking: THREAD_HISTORY_MIGRATOR.locking,
        table_name: THREAD_HISTORY_MIGRATOR.table_name.clone(),
        create_schemas: THREAD_HISTORY_MIGRATOR.create_schemas.clone(),
        no_tx: THREAD_HISTORY_MIGRATOR.no_tx,
    }
}

fn shared_upstream_migrations() -> Vec<Migration> {
    THREAD_HISTORY_MIGRATOR
        .migrations
        .iter()
        .filter(|migration| migration.version < UPSTREAM_LIFECYCLE_TIMESTAMPS_VERSION)
        .cloned()
        .collect()
}

fn legacy_fork_migrator(latest_version: i64) -> Migrator {
    let lifecycle_sql = current_migration(1_000_001).sql.as_str().replacen(
        "ALTER TABLE thread_items ADD COLUMN is_complete INTEGER NOT NULL DEFAULT 1;\n",
        "ALTER TABLE thread_items ADD COLUMN is_complete INTEGER NOT NULL DEFAULT 1;\nALTER TABLE thread_items ADD COLUMN completed_at_ms INTEGER;\n",
        1,
    );
    let mut migrations = shared_upstream_migrations();
    migrations.extend([
        legacy_migration(7, "thread item lifecycle", lifecycle_sql),
        legacy_migration(
            8,
            "thread observation versions",
            current_migration(1_000_002).sql.as_str().to_string(),
        ),
        legacy_migration(
            9,
            "legacy projection state",
            current_migration(1_000_003).sql.as_str().to_string(),
        ),
        legacy_migration(
            LEGACY_STARTED_AT_VERSION,
            "thread item started at",
            "ALTER TABLE thread_items ADD COLUMN started_at_ms INTEGER;\n".to_string(),
        ),
    ]);
    migrations.retain(|migration| migration.version <= latest_version);
    thread_history_migrator(migrations)
}

fn upstream_migrator() -> Migrator {
    let mut migrations = shared_upstream_migrations();
    migrations.push(current_migration(UPSTREAM_LIFECYCLE_TIMESTAMPS_VERSION).clone());
    thread_history_migrator(migrations)
}

fn hex_checksum(migration: &Migration) -> String {
    migration
        .checksum
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect()
}

async fn applied_migrations(pool: &SqlitePool) -> Vec<(i64, String, Vec<u8>)> {
    sqlx::query("SELECT version, description, checksum FROM _sqlx_migrations ORDER BY version")
        .fetch_all(pool)
        .await
        .expect("applied migrations should load")
        .into_iter()
        .map(|row| {
            (
                row.get("version"),
                row.get("description"),
                row.get("checksum"),
            )
        })
        .collect()
}

#[test]
fn legacy_fork_migrations_match_deployed_checksums() {
    let legacy = legacy_fork_migrator(LEGACY_STARTED_AT_VERSION)
        .migrations
        .iter()
        .filter(|migration| migration.version >= UPSTREAM_LIFECYCLE_TIMESTAMPS_VERSION)
        .map(|migration| (migration.version, hex_checksum(migration)))
        .collect::<Vec<_>>();
    let deployed = LEGACY_PRIVATE_MIGRATIONS
        .iter()
        .map(|legacy| (legacy.legacy_version, legacy.legacy_checksum.to_string()))
        .chain([(
            LEGACY_STARTED_AT_VERSION,
            LEGACY_STARTED_AT_CHECKSUM.to_string(),
        )])
        .collect::<Vec<_>>();

    assert_eq!(legacy, deployed);
}

#[tokio::test]
async fn repairs_database_migrated_through_legacy_observation_versions() {
    assert_legacy_database_repairs(/*latest_version*/ 9).await;
}

#[tokio::test]
async fn repairs_database_migrated_through_legacy_started_at() {
    assert_legacy_database_repairs(LEGACY_STARTED_AT_VERSION).await;
}

async fn assert_legacy_database_repairs(latest_version: i64) {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let legacy_pool = sqlite
        .open_read_write_pool(&sqlite.thread_history_db_path())
        .await
        .expect("legacy database should open");
    legacy_fork_migrator(latest_version)
        .run(&legacy_pool)
        .await
        .expect("legacy fork migrations should apply");
    sqlx::query(
        "INSERT INTO thread_items (thread_id, turn_id, item_id, rollout_ordinal, created_at_ms, item_json, is_complete, completed_at_ms) VALUES ('thread', 'turn', 'item', 3, 100, '{}', 1, 250)",
    )
    .execute(&legacy_pool)
    .await
    .expect("legacy item should insert");
    legacy_pool.close().await;

    let pool = sqlite
        .open_thread_history_db(
            &runtime_thread_history_migrator(),
            /*telemetry_override*/ None,
        )
        .await
        .expect("current runtime should open a legacy fork database");
    let expected = THREAD_HISTORY_MIGRATOR
        .migrations
        .iter()
        .map(|migration| {
            (
                migration.version,
                migration.description.to_string(),
                migration.checksum.to_vec(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(applied_migrations(&pool).await, expected);
    upstream_migrator()
        .run(&pool)
        .await
        .expect("upstream migrations should accept the repaired database");
    let item = sqlx::query_as::<_, (Option<i64>, Option<i64>, i64)>(
        "SELECT started_at_ms, completed_at_ms, is_complete FROM thread_items WHERE item_id = 'item'",
    )
    .fetch_optional(&pool)
    .await
    .expect("derived legacy item invalidation should be readable");
    assert_eq!(item, None);
    pool.close().await;

    let reopened = sqlite
        .open_thread_history_db(
            &runtime_thread_history_migrator(),
            /*telemetry_override*/ None,
        )
        .await
        .expect("repaired database should reopen");
    assert_eq!(applied_migrations(&reopened).await, expected);
    reopened.close().await;
}
