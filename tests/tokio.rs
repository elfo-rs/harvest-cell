#![cfg(feature = "tokio")]

use std::{
    cell::Cell,
    future::Future,
    mem,
    pin::pin,
    task::{Context, Waker},
};

use tokio::sync::oneshot;

use harvest_cell::{HarvestCell, Harvester};

#[tokio::test]
async fn async_harvest_smoke() {
    let (_, mut handle) = HarvestCell::new([vec![1], Vec::new()]).into_split();

    let mut harvester = Harvester::start(&mut handle);
    let values = harvester.async_next_with(mem::take).await;

    assert_eq!(values, Some(vec![1]));
    assert_eq!(
        harvester
            .async_next_with(|_| panic!("unexpected callback"))
            .await,
        None,
    );
}

#[tokio::test]
async fn cancelling_preserves_pending_buffer() {
    let (mut writer, mut handle) = HarvestCell::new([Vec::new(), Vec::new()]).into_split();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let writer = tokio::task::spawn_blocking(move || {
        writer.write(|buffer| {
            entered_tx.send(()).unwrap();
            buffer.push(42);
            let _ = release_rx.blocking_recv();
        });
    });
    entered_rx.await.unwrap();

    let mut ctx = Context::from_waker(Waker::noop());
    let mut harvester = Harvester::start(&mut handle);
    let called = Cell::new(false);
    for polls in 1..6 {
        {
            let mut next = pin!(harvester.async_next_with(|buffer| {
                called.set(true);
                mem::take(buffer)
            }));
            // Cancellation must preserve the buffer for a later harvest.
            for _ in 0..polls {
                assert!(next.as_mut().poll(&mut ctx).is_pending());
            }
        }
        assert!(!called.get());
    }

    release_tx.send(()).unwrap();
    writer.await.unwrap();
    assert_eq!(harvester.async_next_with(mem::take).await, Some(vec![42]));
    assert_eq!(harvester.async_next_with(mem::take).await, None);
}
