use std::cell::Cell;

use static_assertions::{assert_impl_all, assert_not_impl_any};

use harvest_cell::{Borrowed, HarvestCell, HarvestHandle, Harvester, Owned, Writer};

#[test]
fn buffers_need_send_but_not_sync() {
    type Buffer = Cell<u64>;

    assert_impl_all!(Buffer: Send);
    assert_not_impl_any!(Buffer: Sync);
    assert_impl_all!(HarvestCell<Buffer>: Send, Sync);
    assert_impl_all!(Borrowed<'static, Buffer>: Send, Sync);
    assert_impl_all!(Owned<Buffer>: Send, Sync);
    assert_impl_all!(Writer<Borrowed<'static, Buffer>>: Send, Sync);
    assert_impl_all!(HarvestHandle<Borrowed<'static, Buffer>>: Send, Sync);
    assert_impl_all!(Writer<Owned<Buffer>>: Send, Sync);
    assert_impl_all!(HarvestHandle<Owned<Buffer>>: Send, Sync);
    assert_impl_all!(Harvester<'static, Buffer>: Send, Sync);
}

#[test]
fn projected_storage_can_be_sent_and_shared() {
    type Shard = (HarvestCell<Cell<u64>>, String);
    type Storage = Owned<Cell<u64>, Shard>;

    assert_impl_all!(Shard: Send, Sync);
    assert_impl_all!(Storage: Send, Sync);
    assert_impl_all!(Writer<Storage>: Send, Sync);
    assert_impl_all!(HarvestHandle<Storage>: Send, Sync);
}

#[test]
fn non_sync_storage_can_be_sent_and_shared() {
    type Shard = (HarvestCell<u64>, Cell<u64>);
    type Storage = Owned<u64, Shard>;

    assert_impl_all!(Shard: Send);
    assert_not_impl_any!(Shard: Sync);
    assert_impl_all!(Storage: Send, Sync);
    assert_impl_all!(Writer<Storage>: Send, Sync);
    assert_impl_all!(HarvestHandle<Storage>: Send, Sync);
}
