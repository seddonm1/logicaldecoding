use anyhow::Result;
use sqlx::PgConnection;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Tenant {
    pub xmin: Option<i64>,
    pub tenant_id: Uuid,
    pub id: Uuid,
    pub name: String,
    pub short_description: Option<String>,
    pub long_description: Option<String>,
}

impl Tenant {
    #[allow(dead_code)]
    pub async fn create(&mut self, conn: &mut PgConnection) -> Result<u64> {
        self.xmin = sqlx::query_file!(
            "src/types/tenant/queries/create.sql",
            self.id,
            self.name,
            self.short_description,
            self.long_description
        )
        .fetch_one(&mut *conn)
        .await?
        .xmin;

        Ok(1)
    }

    #[allow(dead_code)]
    pub async fn retrieve(conn: &mut PgConnection, id: Uuid) -> Result<Self> {
        Ok(
            sqlx::query_file_as!(Self, "src/types/tenant/queries/retrieve.sql", id)
                .fetch_one(&mut *conn)
                .await?,
        )
    }

    #[allow(dead_code)]
    pub async fn retrieve_many(conn: &mut PgConnection, ids: &[Uuid]) -> Result<Vec<Self>> {
        Ok(
            sqlx::query_file_as!(Self, "src/types/tenant/queries/retrieve_many.sql", ids)
                .fetch_all(&mut *conn)
                .await?,
        )
    }

    #[allow(dead_code)]
    pub async fn retrieve_all(conn: &mut PgConnection) -> Result<Vec<Self>> {
        Ok(
            sqlx::query_file_as!(Self, "src/types/tenant/queries/retrieve_all.sql")
                .fetch_all(&mut *conn)
                .await?,
        )
    }

    #[allow(dead_code)]
    pub async fn update(&mut self, conn: &mut PgConnection) -> Result<u64> {
        self.xmin = sqlx::query_file!(
            "src/types/tenant/queries/update.sql",
            self.id,
            self.name,
            self.short_description,
            self.long_description
        )
        .fetch_one(&mut *conn)
        .await?
        .xmin;

        Ok(1)
    }

    #[allow(dead_code)]
    pub async fn delete(&self, conn: &mut PgConnection) -> Result<u64> {
        Ok(
            sqlx::query_file!("src/types/tenant/queries/delete.sql", self.id)
                .execute(&mut *conn)
                .await?
                .rows_affected(),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use sqlx::PgPool;

    #[sqlx::test(fixtures("tenants"))]
    async fn test_retrieve(db: PgPool) -> Result<()> {
        let mut conn = db.acquire().await.unwrap();

        Tenant::retrieve(
            &mut conn,
            Uuid::parse_str("c497c1be-cf70-41aa-8665-971e2ffaefcd")?,
        )
        .await?;

        Ok(())
    }

    #[sqlx::test(fixtures("tenants"))]
    async fn test_retrieve_many(db: PgPool) -> Result<()> {
        let mut conn = db.acquire().await.unwrap();

        let tenants = Tenant::retrieve_many(
            &mut conn,
            &[Uuid::parse_str("c497c1be-cf70-41aa-8665-971e2ffaefcd")?],
        )
        .await?;

        assert_eq!(tenants.len(), 1);

        Ok(())
    }

    #[sqlx::test(fixtures("tenants"))]
    async fn test_retrieve_all(db: PgPool) -> Result<()> {
        let mut conn = db.acquire().await.unwrap();

        let tenants = Tenant::retrieve_all(&mut conn).await?;

        assert_eq!(tenants.len(), 1);

        Ok(())
    }

    #[sqlx::test(fixtures("tenants"))]
    async fn test_update(db: PgPool) -> Result<()> {
        let mut txn = db.begin().await.unwrap();
        let mut tenant = Tenant::retrieve(
            &mut *txn,
            Uuid::parse_str("c497c1be-cf70-41aa-8665-971e2ffaefcd")?,
        )
        .await?;

        tenant.name = "NAME".to_string();
        tenant.short_description = Some("SHORT_DESCRIPTION".to_string());
        tenant.long_description = Some("LONG_DESCRIPTION".to_string());

        let rows_affected = tenant.update(&mut *txn).await?;
        assert_eq!(rows_affected, 1);

        txn.commit().await.unwrap();

        let mut conn = db.acquire().await.unwrap();
        let new_tenant = Tenant::retrieve(
            &mut conn,
            Uuid::parse_str("c497c1be-cf70-41aa-8665-971e2ffaefcd")?,
        )
        .await?;

        assert_eq!(tenant.name, new_tenant.name);
        assert_eq!(tenant.short_description, new_tenant.short_description);
        assert_eq!(tenant.long_description, new_tenant.long_description);

        Ok(())
    }

    #[sqlx::test(fixtures("tenants"))]
    async fn test_delete(db: PgPool) -> Result<()> {
        let mut conn = db.acquire().await.unwrap();

        let tenant = Tenant::retrieve(
            &mut conn,
            Uuid::parse_str("c497c1be-cf70-41aa-8665-971e2ffaefcd")?,
        )
        .await?;

        let rows_affected = tenant.delete(&mut conn).await?;
        assert_eq!(rows_affected, 1);

        assert!(Tenant::retrieve(
            &mut conn,
            Uuid::parse_str("c497c1be-cf70-41aa-8665-971e2ffaefcd")?,
        )
        .await
        .map_err(|err| err.to_string())
        .unwrap_err()
        .contains("no rows returned"));

        Ok(())
    }
}
