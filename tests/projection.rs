use std::{
    cell::Cell,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

use static_assertions::{assert_impl_all, assert_not_impl_all};

use harvest_cell::{HarvestCell, Harvester};

struct Shard {
    cell: HarvestCell<Cell<u64>>,
    drops: Arc<AtomicUsize>,
    not_sync: PhantomData<Cell<()>>,
}

assert_impl_all!(Shard: Send);
assert_not_impl_all!(Shard: Sync);

impl Drop for Shard {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn storage_drops_with_last_handle() {
    for writer_first in [false, true] {
        let drops = Arc::new(AtomicUsize::new(0));

        let shard = Shard {
            cell: HarvestCell::new([Cell::new(0), Cell::new(0)]),
            drops: Arc::clone(&drops),
            not_sync: PhantomData,
        };

        let (mut writer, mut handle) = HarvestCell::map_split(shard, |shard| &mut shard.cell);

        writer = thread::spawn(move || {
            writer.write(|value| value.set(42));
            writer
        })
        .join()
        .unwrap();

        handle = thread::spawn(move || {
            let mut harvester = Harvester::start(&mut handle);
            assert_eq!(harvester.next_with(|value| value.take()), Some(42));
            drop(harvester);
            handle
        })
        .join()
        .unwrap();

        if writer_first {
            drop(writer);
            assert_eq!(drops.load(Ordering::Relaxed), 0);
            thread::spawn(move || drop(handle)).join().unwrap();
        } else {
            drop(handle);
            assert_eq!(drops.load(Ordering::Relaxed), 0);
            writer.write(|value| value.set(7));
            thread::spawn(move || drop(writer)).join().unwrap();
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}
