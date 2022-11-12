mod tenant;
use crate::replication::decoderbufs::RowMessage;
pub use tenant::Tenant;

#[derive(Debug)]
#[allow(dead_code)]
struct Transaction {
    xid: u32,
    commit_time: u64,
    changes: Vec<RowMessage>,
}

#[cfg(test)]
mod test {
    use crate::replication::Transaction;

    use super::Tenant;
    use crate::replication::decoderbufs::{datum_message::Datum, Op};
    use anyhow::Result;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use sqlx::PgPool;
    use std::{collections::HashMap, time::Duration};
    use tokio::task;
    use tracing::trace;
    use uuid::Uuid;

    fn gen_uuid(rng: &mut StdRng) -> Uuid {
        let mut b = [0u8; 16];
        rng.fill(&mut b[..]);
        Uuid::from_bytes(b)
    }

    async fn subscriber(
        tx: tokio::sync::broadcast::Sender<Transaction>,
        mut done: tokio::sync::mpsc::Receiver<()>,
    ) -> (usize, HashMap<Uuid, Tenant>) {
        let mut rx = tx.subscribe();
        let mut transactions = 0;
        let mut tenants = HashMap::new();

        loop {
            tokio::select! {
                _ = done.recv() => {
                    break
                }
                Ok(transaction) = rx.recv() => {
                    transactions += 1;
                                println!("SUBSCRIBER {:?}", transaction.xid );

            transaction.events.iter().for_each(|event| {
                match Op::from_i32(event.op.unwrap()).unwrap() {
                    Op::Insert => {
                        let inserts = event
                            .new_tuple
                            .iter()
                            .map(|datum| (datum.column_name(), datum))
                            .collect::<HashMap<_, _>>();

                        let id = match &inserts.get("id").unwrap().datum.as_ref().unwrap() {
                            Datum::DatumString(value) => Uuid::parse_str(&value).unwrap(),
                            _ => unimplemented!(),
                        };

                        tenants.insert(
                            id,
                            Tenant {
                                xmin: Some(event.transaction_id() as i64),
                                tenant_id: match &inserts
                                    .get("tenant_id")
                                    .unwrap()
                                    .datum
                                    .as_ref()
                                    .unwrap()
                                {
                                    Datum::DatumString(value) => Uuid::parse_str(&value).unwrap(),
                                    _ => unimplemented!(),
                                },
                                id: match &inserts.get("id").unwrap().datum.as_ref().unwrap() {
                                    Datum::DatumString(value) => Uuid::parse_str(&value).unwrap(),
                                    _ => unimplemented!(),
                                },
                                name: match &inserts.get("name").unwrap().datum.as_ref().unwrap() {
                                    Datum::DatumString(value) => value.to_string(),
                                    _ => unimplemented!(),
                                },
                                short_description: match &inserts
                                    .get("short_description")
                                    .unwrap()
                                    .datum
                                    .as_ref()
                                {
                                    Some(Datum::DatumString(value)) => Some(value.to_string()),
                                    _ => None,
                                },
                                long_description: match &inserts
                                    .get("long_description")
                                    .unwrap()
                                    .datum
                                    .as_ref()
                                {
                                    Some(Datum::DatumString(value)) => Some(value.to_string()),
                                    _ => None,
                                },
                            },
                        );
                    }
                    Op::Update => {
                        let mut updates = event
                            .new_tuple
                            .iter()
                            .map(|datum| (datum.column_name(), datum))
                            .collect::<HashMap<_, _>>();

                        let id = match &updates.remove("id").unwrap().datum.as_ref().unwrap() {
                            Datum::DatumString(value) => Uuid::parse_str(&value).unwrap(),
                            _ => unimplemented!(),
                        };

                        let mut tenant = tenants.get_mut(&id).unwrap();
                        tenant.xmin = Some(event.transaction_id() as i64);

                        updates.into_iter().for_each(|(k, v)| match k {
                            "tenant_id" => {
                                tenant.tenant_id = match v.datum.as_ref().unwrap() {
                                    Datum::DatumString(value) => Uuid::parse_str(&value).unwrap(),
                                    _ => unimplemented!(),
                                }
                            }
                            "name" => {
                                tenant.name = match v.datum.as_ref().unwrap() {
                                    Datum::DatumString(value) => value.to_string(),
                                    _ => unimplemented!(),
                                }
                            }
                            "short_description" => {
                                tenant.short_description = match v.datum.as_ref() {
                                    Some(Datum::DatumString(value)) => Some(value.to_string()),
                                    _ => None,
                                }
                            }
                            "long_description" => {
                                tenant.long_description = match v.datum.as_ref() {
                                    Some(Datum::DatumString(value)) => Some(value.to_string()),
                                    _ => None,
                                }
                            }
                            k => panic!("unimplemented key: {:?}", k),
                        })
                    }
                    Op::Delete => {
                        let mut deletes = event
                            .old_tuple
                            .iter()
                            .map(|datum| (datum.column_name(), datum))
                            .collect::<HashMap<_, _>>();

                        let id = match &deletes.remove("id").unwrap().datum.as_ref().unwrap() {
                            Datum::DatumString(value) => Uuid::parse_str(&value).unwrap(),
                            _ => unimplemented!(),
                        };

                        tenants.remove(&id);
                    }
                    Op::Begin => unreachable!(),
                    Op::Commit => unreachable!(),
                    Op::Unknown => unreachable!(),
                };
            });
                }
            }
        }

        (transactions, tenants)
    }

    #[sqlx::test]
    async fn test_create(db: PgPool) -> Result<()> {
        let iterations = 1000;
        let interval = Duration::from_millis(100);

        let current_database = {
            let mut conn = db.acquire().await.unwrap();
            sqlx::query!("SELECT current_database()")
                .fetch_one(&mut conn)
                .await?
                .current_database
                .unwrap()
        };

        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
        let (tx, _) = tokio::sync::broadcast::channel::<Transaction>(100);

        let tx_clone = tx.clone();
        let listener_handle = task::spawn(async move {
            crate::replication::start_streaming_changes(current_database, ready_tx, tx_clone).await
        });

        // block waiting for replication
        ready_rx.await.unwrap();

        let tx_clone = tx.clone();

        let (done_tx, done_rx) = tokio::sync::mpsc::channel::<()>(1);

        let subscriber_handle = task::spawn(async move { subscriber(tx_clone, done_rx).await });

        let db_clone = db.clone();
        let generator_handle = task::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            let mut rng = StdRng::seed_from_u64(42);
            let mut tenants: HashMap<Uuid, Tenant> = HashMap::new();
            let mut transactions = 0_usize;

            for _ in 0..iterations {
                interval.tick().await;

                // create transactions
                let mut txn = db_clone.begin().await.unwrap();
                let xid = sqlx::query!("SELECT pg_current_xact_id()::TEXT::BIGINT AS xid")
                    .fetch_one(&mut *txn)
                    .await
                    .unwrap()
                    .xid
                    .unwrap();
                trace!("BEGIN {:?}", xid);

                let rollback = tenants.clone();

                for _ in 0..rng.gen_range(1..10) {
                    match rng.gen_range(0..=2) {
                        // create
                        0 => {
                            let key = gen_uuid(&mut rng);
                            let mut tenant = Tenant {
                                xmin: None,
                                tenant_id: key,
                                id: key,
                                name: gen_uuid(&mut rng).to_string(),
                                short_description: Some(gen_uuid(&mut rng).to_string()),
                                long_description: Some(gen_uuid(&mut rng).to_string()),
                            };

                            tenant.create(&mut *txn).await.unwrap();
                            trace!("CREATE {:?} {:?}", tenant.xmin, tenant.id);

                            tenants.insert(key, tenant);
                        }
                        // update
                        1 => {
                            if !tenants.is_empty() {
                                let keys = tenants.keys().cloned().collect::<Vec<_>>();
                                if let Some(key) = keys.get(rng.gen_range(0..keys.len())) {
                                    let mut tenant = tenants.remove(key).unwrap();
                                    tenant.name = gen_uuid(&mut rng).to_string();
                                    tenant.short_description = Some(gen_uuid(&mut rng).to_string());
                                    tenant.long_description = if rng.gen_bool(0.5) {
                                        Some(gen_uuid(&mut rng).to_string())
                                    } else {
                                        None
                                    };

                                    tenant.update(&mut *txn).await.unwrap();
                                    trace!("UPDATE {:?} {:?}", tenant.xmin, tenant.id);
                                    tenants.insert(*key, tenant);
                                }
                            }
                        }
                        // delete
                        2 => {
                            if !tenants.is_empty() {
                                let keys = tenants.keys().cloned().collect::<Vec<_>>();
                                if let Some(key) = keys.get(rng.gen_range(0..keys.len())) {
                                    let tenant = tenants.remove(key).unwrap();
                                    trace!("DELETE {:?} {:?}", tenant.xmin, tenant.id);
                                    tenant.delete(&mut *txn).await.unwrap();
                                }
                            }
                        }
                        _ => (),
                    };
                }

                match rng.gen_bool(0.1) {
                    true => {
                        trace!("ROLLBACK {:?}", xid);
                        txn.rollback().await.unwrap();
                        tenants = rollback;
                    }
                    false => {
                        trace!("COMMIT {:?}", xid);
                        txn.commit().await.unwrap();
                        transactions += 1;
                    }
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
            (transactions, tenants)
        });

        // shutdown generator
        let (generator_transactions, tenants) = generator_handle.await.unwrap();
        done_tx.send(()).await.unwrap();
        listener_handle.abort();
        let (subscriber_transactions, subscriber) = subscriber_handle.await.unwrap();
        println!(
            "generator_transactions {:?} subscriber_transactions {:?}",
            generator_transactions, subscriber_transactions
        );

        // retrieve tenants from db
        let mut conn = db.acquire().await.unwrap();
        let actual_tenants = Tenant::retrieve_all(&mut conn)
            .await
            .unwrap()
            .into_iter()
            .map(|tenant| (tenant.id, tenant))
            .collect::<HashMap<_, _>>();

        // test the generator / database / subscriber produce the same results
        assert_eq!(tenants, actual_tenants);
        assert_eq!(subscriber, actual_tenants);

        Ok(())
    }
}
