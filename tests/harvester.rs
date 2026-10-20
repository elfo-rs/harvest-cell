use std::{
    mem,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::mpsc,
    thread,
};

use harvest_cell::{Borrowed, HarvestCell, HarvestHandle, HarvestResult, Harvester};

#[test]
fn empty_harvest_set_is_done() {
    let no_handles: Vec<HarvestHandle<Borrowed<'_, u64>>> = vec![];
    let mut harvester = Harvester::start(no_handles);
    for _ in 0..2 {
        assert_eq!(
            harvester.try_harvest(|_| panic!("unexpected callback")),
            HarvestResult::Done,
        );
    }
}

#[test]
fn harvests_ready_buffers_before_busy_ones() {
    let (mut writer, mut busy) = HarvestCell::new([0, 0]).into_split();
    let (_, mut ready_a) = HarvestCell::new([2, 0]).into_split();
    let (_, mut ready_b) = HarvestCell::new([3, 0]).into_split();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let thread = thread::spawn(move || {
        writer.write(|value| {
            *value = 1;
            entered_tx.send(()).unwrap();
            // Also release the writer if an assertion drops the sender.
            let _ = release_rx.recv();
        });
    });
    entered_rx.recv().unwrap();

    let handles = [
        busy.as_borrowed(),
        ready_a.as_borrowed(),
        ready_b.as_borrowed(),
    ];
    let mut harvester = Harvester::start(handles);
    let mut values = Vec::new();
    for _ in 0..2 {
        let HarvestResult::Item(value) = harvester.try_harvest(mem::take) else {
            panic!("expected a ready buffer");
        };
        values.push(value);
    }
    assert_eq!(values, vec![2, 3]);
    assert_eq!(
        harvester.try_harvest(|_| panic!("unexpected callback")),
        HarvestResult::Busy,
    );

    release_tx.send(()).unwrap();
    thread.join().unwrap();
    assert_eq!(harvester.try_harvest(mem::take), HarvestResult::Item(1));
    assert_eq!(
        harvester.try_harvest(|_| panic!("unexpected callback")),
        HarvestResult::Done,
    );
}

#[test]
fn dropping_preserves_pending_buffers() {
    let (mut writer_a, mut handle_a) = HarvestCell::new([1, 10]).into_split();
    let (mut writer_b, mut handle_b) = HarvestCell::new([2, 20]).into_split();

    {
        let mut harvester = Harvester::start([&mut handle_a, &mut handle_b]);
        assert_eq!(harvester.try_harvest(mem::take), HarvestResult::Item(1));
    }

    writer_a.write(|value| *value += 100);
    writer_b.write(|value| *value += 200);

    let mut harvest_a = Harvester::start(&mut handle_a);
    let mut harvest_b = Harvester::start(&mut handle_b);
    assert_eq!(harvest_a.try_harvest(mem::take), HarvestResult::Item(110));
    // Collect the buffer left pending by the first harvest.
    assert_eq!(harvest_b.try_harvest(mem::take), HarvestResult::Item(2));
}

#[test]
fn panicking_callback_keeps_its_buffer_pending() {
    let (mut writer, mut handle) = HarvestCell::new([40, 0]).into_split();
    let mut harvester = Harvester::start(&mut handle);
    let panic = catch_unwind(AssertUnwindSafe(|| {
        harvester.try_harvest(|value| {
            *value += 2;
            panic!("callback failed");
        })
    }));
    assert!(panic.is_err());

    writer.write(|value| *value = 7);
    assert_eq!(harvester.try_harvest(mem::take), HarvestResult::Item(42));
    assert_eq!(harvester.try_harvest(mem::take), HarvestResult::Done);
    drop(harvester);

    let mut harvester = Harvester::start(&mut handle);
    assert_eq!(harvester.try_harvest(mem::take), HarvestResult::Item(7));
}
