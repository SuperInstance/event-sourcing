//! event-sourcing: Event sourcing primitives for building audit-friendly state machines.

use std::collections::HashMap;

pub trait Event: Clone + Send + Sync + std::fmt::Debug + 'static {}
impl<T: Clone + Send + Sync + std::fmt::Debug + 'static> Event for T {}

pub trait Aggregate: Default + Send + Sync {
    type Event: Event;

    fn apply(&mut self, event: &Self::Event);
    fn aggregate_type() -> &'static str;
}

#[derive(Debug, Clone)]
pub struct EventEnvelope<E: Event> {
    pub aggregate_id: String,
    pub sequence: u64,
    pub event: E,
    pub timestamp: u64,
}

pub struct EventStore<A: Aggregate> {
    streams: HashMap<String, Vec<EventEnvelope<A::Event>>>,
    snapshots: HashMap<String, (u64, A)>,
    _marker: std::marker::PhantomData<A>,
}

impl<A: Aggregate> EventStore<A> {
    pub fn new() -> Self {
        Self {
            streams: HashMap::new(),
            snapshots: HashMap::new(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn append(&mut self, aggregate_id: &str, events: Vec<A::Event>, timestamp: u64) {
        let stream = self.streams.entry(aggregate_id.to_string()).or_default();
        let mut seq = stream.len() as u64;
        for event in events {
            seq += 1;
            stream.push(EventEnvelope {
                aggregate_id: aggregate_id.to_string(),
                sequence: seq,
                event,
                timestamp,
            });
        }
    }

    pub fn load(&self, aggregate_id: &str) -> A {
        let mut aggregate = A::default();
        if let Some(stream) = self.streams.get(aggregate_id) {
            for envelope in stream {
                aggregate.apply(&envelope.event);
            }
        }
        aggregate
    }

    pub fn events_for(&self, aggregate_id: &str) -> &[EventEnvelope<A::Event>] {
        self.streams
            .get(aggregate_id)
            .map(|s| s.as_slice())
            .unwrap_or(&[])
    }

    pub fn save_snapshot(&mut self, aggregate_id: &str, sequence: u64, aggregate: A) {
        self.snapshots.insert(aggregate_id.to_string(), (sequence, aggregate));
    }
}

impl<A: Aggregate> Default for EventStore<A> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    enum BankEvent {
        Deposited(u64),
        Withdrawn(u64),
    }

    #[derive(Default)]
    struct BankAccount {
        balance: u64,
    }

    impl Aggregate for BankAccount {
        type Event = BankEvent;

        fn apply(&mut self, event: &Self::Event) {
            match event {
                BankEvent::Deposited(amount) => self.balance += amount,
                BankEvent::Withdrawn(amount) => self.balance -= amount,
            }
        }

        fn aggregate_type() -> &'static str {
            "bank_account"
        }
    }

    #[test]
    fn test_event_sourcing() {
        let mut store: EventStore<BankAccount> = EventStore::new();
        store.append("acc-1", vec![BankEvent::Deposited(100)], 1);
        store.append("acc-1", vec![BankEvent::Withdrawn(30)], 2);

        let account = store.load("acc-1");
        assert_eq!(account.balance, 70);
        assert_eq!(store.events_for("acc-1").len(), 2);
    }
}

/// FNV-1a 64 — the digest every substrate in the SuperInstance fleet agrees on.
pub const FNV_OFFSET: u64 = 0xcbf29ce484222325;
pub const FNV_PRIME: u64 = 0x100000001b3;

#[inline]
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    for &b in bytes {
        h = (h ^ b as u64).wrapping_mul(FNV_PRIME);
    }
    h
}

/// True if this crate's FNV-1a still agrees with the rest of the fleet.
pub fn canary_holds() -> bool {
    fnv1a64("café Δ 日本語".as_bytes()) == 0x024a555471370b18d
}
