pub mod decoderbufs {
    include!(concat!(env!("OUT_DIR"), "/decoderbufs.rs"));
}
use bytes::{BufMut, BytesMut};
use decoderbufs::{Op, RowMessage};
use futures::{
    future::{self},
    ready, Sink, StreamExt,
};
use prost::Message;
use std::{
    task::Poll,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{broadcast, oneshot};
use tokio_postgres::{NoTls, SimpleQueryMessage};
use tracing::{debug, trace};

static MICROSECONDS_FROM_UNIX_EPOCH_TO_2000: u128 = 946_684_800_000_000;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Transaction {
    pub xid: u32,
    pub commit_time: u64,
    pub events: Vec<RowMessage>,
}

/// starts streaming changes
pub async fn start_streaming_changes(
    database: impl Into<String> + std::fmt::Display,
    ready: oneshot::Sender<()>,
    tx: broadcast::Sender<Transaction>,
) -> Result<(), tokio_postgres::Error> {
    let db_config = format!(
        "user=postgres password=password host=localhost port=5432 dbname={} replication=database",
        database
    );
    println!("CONNECT");

    // connect to the database
    let (client, connection) = tokio_postgres::connect(&db_config, NoTls).await.unwrap();

    // the connection object performs the actual communication with the database, so spawn it off to run on its own
    tokio::spawn(async move { connection.await });

    //let slot_name = "slot";
    let slot_name = "slot_".to_owned()
        + &SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();
    let slot_query = format!(
        "CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL \"decoderbufs\"",
        slot_name
    );

    let lsn = client
        .simple_query(&slot_query)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>()
        .first()
        .unwrap()
        .get("consistent_point")
        .unwrap()
        .to_owned();

    let query = format!("START_REPLICATION SLOT {} LOGICAL {}", slot_name, lsn);
    let duplex_stream = client
        .copy_both_simple::<bytes::Bytes>(&query)
        .await
        .unwrap();
    let mut duplex_stream_pin = Box::pin(duplex_stream);

    // see here for format details: https://www.postgresql.org/docs/current/protocol-replication.html
    let mut keepalive = BytesMut::with_capacity(34);
    keepalive.put_u8(b'r');
    // the last 8 bytes of these are overwritten with a timestamp to meet the protocol spec
    keepalive.put_bytes(0, 32);
    keepalive.put_u8(1);

    // set the timestamp of the keepalive message
    keepalive[26..34].swap_with_slice(
        &mut ((SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
            - MICROSECONDS_FROM_UNIX_EPOCH_TO_2000) as u64)
            .to_be_bytes(),
    );

    // send the keepalive to ensure connection is functioning
    future::poll_fn(|cx| {
        ready!(duplex_stream_pin.as_mut().poll_ready(cx)).unwrap();
        duplex_stream_pin
            .as_mut()
            .start_send(keepalive.clone().into())
            .unwrap();
        ready!(duplex_stream_pin.as_mut().poll_flush(cx)).unwrap();
        Poll::Ready(())
    })
    .await;

    // notify ready
    ready.send(()).unwrap();

    let mut transaction = None;
    loop {
        match duplex_stream_pin.as_mut().next().await {
            None => break,
            Some(Err(_)) => continue,
            // type: XLogData (WAL data, ie. change of data in db)
            Some(Ok(event)) if event[0] == b'w' => {
                let row_message = RowMessage::decode(&event[25..]).unwrap();
                debug!("Got XLogData/data-change event: {:?}", row_message);

                match row_message.op {
                    Some(op) if op == Op::Begin as i32 => {
                        transaction = Some(Transaction {
                            xid: row_message.transaction_id(),
                            commit_time: row_message.commit_time(),
                            events: vec![],
                        })
                    }
                    Some(op) if op == Op::Commit as i32 => {
                        debug!("{:?}", &transaction.as_ref().unwrap());
                        let transaction = transaction.take().unwrap();
                        tx.send(transaction).unwrap();
                    }
                    Some(_) => {
                        transaction.as_mut().unwrap().events.push(row_message);
                    }
                    None => unimplemented!(),
                }
            }
            // type: keepalive message
            Some(Ok(event)) if event[0] == b'k' => {
                let last_byte = event.last().unwrap();
                let timeout_imminent = last_byte == &1;
                trace!(
                    "Got keepalive message:{:x?} @timeoutImminent:{}",
                    event,
                    timeout_imminent
                );
                if timeout_imminent {
                    keepalive[26..34].swap_with_slice(
                        &mut ((SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_micros()
                            - MICROSECONDS_FROM_UNIX_EPOCH_TO_2000)
                            as u64)
                            .to_be_bytes(),
                    );

                    trace!(
                        "Trying to send response to keepalive message/warning!:{:x?}",
                        keepalive
                    );

                    future::poll_fn(|cx| {
                        ready!(duplex_stream_pin.as_mut().poll_ready(cx)).unwrap();
                        duplex_stream_pin
                            .as_mut()
                            .start_send(keepalive.clone().into())
                            .unwrap();
                        ready!(duplex_stream_pin.as_mut().poll_flush(cx)).unwrap();
                        Poll::Ready(())
                    })
                    .await;

                    trace!(
                        "Sent response to keepalive message/warning!:{:x?}",
                        keepalive
                    );
                }
            }
            _ => (),
        }
    }

    Ok(())
}
