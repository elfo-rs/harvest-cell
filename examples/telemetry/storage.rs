use std::{cell::RefCell, sync::Mutex};

use thread_local::ThreadLocal;

use harvest_cell::{HarvestCell, HarvestHandle, Harvester, Owned, Writer};

type Samples = Vec<u64>;

#[derive(Default)]
pub(crate) struct Telemetry {
    writers: ThreadLocal<RefCell<Writer<Owned<Samples>>>>,
    handles: ThreadLocal<Mutex<HarvestHandle<Owned<Samples>>>>,
}

impl Telemetry {
    #[inline]
    pub(crate) fn record(&self, latency_us: u64) {
        self.write(|samples| samples.push(latency_us));
    }

    #[cfg(test)]
    pub(crate) fn record_with(&self, get_value: impl FnOnce() -> u64) {
        self.write(|samples| {
            samples.push(get_value());
        });
    }

    #[inline]
    fn write(&self, f: impl FnOnce(&mut Samples)) {
        let writer = self.writers.get_or(|| {
            let (writer, handle) = HarvestCell::new([Samples::new(), Samples::new()]).into_split();
            self.handles.get_or(|| Mutex::new(handle));
            RefCell::new(writer)
        });
        writer.borrow_mut().write(f);
    }

    pub(crate) fn harvest(&self) -> Samples {
        // Only the harvester locks handles; writers use their local RefCells.
        let mut handles: Vec<_> = self
            .handles
            .iter()
            .map(|handle| handle.lock().unwrap())
            .collect();
        let mut harvester = Harvester::start(handles.iter_mut().map(|handle| handle.as_borrowed()));
        let mut values = Vec::new();
        while harvester
            .next_with(|samples| values.append(samples))
            .is_some()
        {}
        values
    }
}
