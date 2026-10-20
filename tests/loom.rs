#![cfg(all(harvest_cell_loom, feature = "loom"))]

use loom::thread;

use harvest_cell::{HarvestCell, HarvestHandle, HarvestResult, Harvester, Owned};

#[test]
fn concurrent_writes_and_harvest() {
    loom::model(|| {
        let (mut writer, mut handle) = HarvestCell::new([Vec::new(), Vec::new()]).into_split();
        let writer = thread::spawn(move || {
            for value in 0..3 {
                writer.write(|buffer| {
                    thread::yield_now();
                    buffer.push(value);
                });
            }
        });

        let mut values = Vec::new();
        harvest(&mut handle, &mut values);
        harvest(&mut handle, &mut values);
        writer.join().unwrap();

        harvest(&mut handle, &mut values);
        assert_eq!(values, [0, 1, 2]);
    });
}

fn harvest(handle: &mut HarvestHandle<Owned<Vec<u64>>>, values: &mut Vec<u64>) {
    let mut harvester = Harvester::start(handle);
    loop {
        match harvester.try_harvest(|buffer| {
            thread::yield_now();
            values.append(buffer);
        }) {
            HarvestResult::Item(()) => {}
            HarvestResult::Busy => thread::yield_now(),
            HarvestResult::Done => break,
        }
    }
}
