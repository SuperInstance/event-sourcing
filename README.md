# Event Sourcing

**event-sourcing** is a Rust library that implements the event sourcing pattern for building audit-friendly, replayable state machines. Instead of storing current state, it appends immutable events to a log and reconstructs state by replaying them through typed `Aggregate` implementations.

## Why It Matters

Event sourcing is the backbone of financial systems, event-driven microservices, and CQRS architectures. By storing every state transition as an immutable fact, you get a complete audit trail (required by SOX, GDPR, and PCI-DSS), time-travel debugging (reconstruct state at any historical point), and natural integration with message queues. Systems like Kafka, EventStoreDB, and AWS EventBridge are built on this model. This library provides the core abstraction — aggregates, event stores, and snapshots — in pure Rust with no external dependencies.

## How It Works

### Core Abstraction

An **Aggregate** is a domain entity whose state is derived entirely from a sequence of events. The trait requires:

```rust
trait Aggregate: Default + Send + Sync {
    type Event: Event;
    fn apply(&mut self, event: &Self::Event);  // Mutate state from event
    fn aggregate_type() -> &'static str;        // Type identifier
}
```

The `apply` function is a **left-fold** over events. Given events *e₁, e₂, ..., eₙ*, state is:

```
state = eₙ.apply(...e₂.apply(e₁.apply(Aggregate::default())))
```

This is pure functional reduction — `apply` must be deterministic and side-effect-free.

### Event Store

The `EventStore<A>` maintains:
- `streams: HashMap<AggregateId, Vec<EventEnvelope>>` — append-only log per aggregate
- `snapshots: HashMap<AggregateId, (u64, A)>` — point-in-time state cache

**Append** writes events with monotonically increasing sequence numbers. Each `EventEnvelope` wraps the domain event with:
- `aggregate_id` — which entity this belongs to
- `sequence` — 1-indexed position in the stream
- `timestamp` — logical clock for ordering

**Load** reconstructs an aggregate by folding all events since the latest snapshot (or from the beginning if no snapshot exists). Without snapshots, load is O(N) where N = events in the stream. With snapshots taken every K events, load is O(K + N − snapshot_seq).

**Snapshots** are an optimization: `save_snapshot(id, seq, state)` caches the folded state at a sequence number, so subsequent loads skip replaying earlier events. This is critical for aggregates with thousands of events.

### Complexity

| Operation | Without Snapshot | With Snapshot |
|-----------|-----------------|---------------|
| `append` | O(1) amortized | O(1) amortized |
| `load` | O(N) | O(N − S) where S = snapshot position |
| `events_for` | O(1) | O(1) |
| `save_snapshot` | — | O(1) |

## Quick Start

```rust
use event_sourcing::{Aggregate, EventStore};

#[derive(Debug, Clone, PartialEq)]
enum BankEvent {
    Deposited(u64),
    Withdrawn(u64),
}

#[derive(Default)]
struct BankAccount { balance: u64 }

impl Aggregate for BankAccount {
    type Event = BankEvent;

    fn apply(&mut self, event: &Self::Event) {
        match event {
            BankEvent::Deposited(amt) => self.balance += amt,
            BankEvent::Withdrawn(amt) => self.balance -= amt,
        }
    }

    fn aggregate_type() -> &'static str { "bank_account" }
}

fn main() {
    let mut store = EventStore::<BankAccount>::new();

    // Append events
    store.append("acc-1", vec![BankEvent::Deposited(100)], 1);
    store.append("acc-1", vec![BankEvent::Withdrawn(30)], 2);
    store.append("acc-1", vec![BankEvent::Deposited(50)], 3);

    // Reconstruct state
    let account = store.load("acc-1");
    assert_eq!(account.balance, 120); // 100 - 30 + 50

    // Audit trail
    let events = store.events_for("acc-1");
    assert_eq!(events.len(), 3);
    println!("Balance: ${}", account.balance);
    for e in events {
        println!("  #{}: {:?}", e.sequence, e.event);
    }
}
```

## API

### `Aggregate` Trait
```rust
pub trait Aggregate: Default + Send + Sync {
    type Event: Event;
    fn apply(&mut self, event: &Self::Event);
    fn aggregate_type() -> &'static str;
}
```

### `EventStore<A: Aggregate>`
- `new() → EventStore<A>` — Empty store
- `append(aggregate_id, events, timestamp)` — Append events to a stream
- `load(aggregate_id) → A` — Reconstruct aggregate by replaying events
- `events_for(aggregate_id) → &[EventEnvelope]` — Read-only access to event stream
- `save_snapshot(aggregate_id, sequence, aggregate)` — Cache state at a point

### `EventEnvelope<E>`
Wraps an event with `aggregate_id`, `sequence`, `event`, and `timestamp` metadata.

## Architecture Notes

This crate provides the persistence model for SuperInstance's stateful operations. Aggregates represent the γ (gamma) layer's consensus decisions; events are the η (eta) layer's notification units. Together they form the γ + η = C consistency model: every committed decision is an immutable event, and every notification is a projection of those events.

See [ARCHITECTURE.md](https://github.com/SuperInstance/SuperInstance/blob/main/ARCHITECTURE.md) for the full design.

## References

- Fowler, M. (2005). *Event Sourcing*. [martinfowler.com](https://martinfowler.com/eaaDev/EventSourcing.html)
- Young, G. (2017). *Versioning in an Event Sourced System*. Leanpub.
- Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly. Chapter 11 on stream processing.

## License

MIT
