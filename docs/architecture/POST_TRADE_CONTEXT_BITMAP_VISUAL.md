# Post-trade context bitmap model

```mermaid
flowchart LR
    TX["Transactions<br/>one row per transaction<br/><br/>transaction_id<br/>status<br/>counterparty<br/>amount<br/>status_recorded_at"]

    DEF1["Context definition<br/>settlement_intervention_required"]
    DEF2["Context definition<br/>deepsee_settlement_risk"]
    FUTURE["Future context definition<br/>for example liquidity_attention<br/>(planned, not populated)"]

    SNAP1["Immutable snapshot v7<br/>definition hash + data_as_of<br/><br/>Roaring bitmap<br/>{ 12, 41, 88, 104, ... }"]
    SNAP2["Immutable snapshot v3<br/>definition hash + source watermark<br/><br/>Roaring bitmap<br/>{ 7, 41, 63, 104, ... }"]
    SNAP3["Its own future snapshot<br/><br/>Roaring bitmap<br/>{ transaction IDs }"]

    HIST["Shared membership history<br/><br/>context_id<br/>transaction_id<br/>added / removed / score_changed<br/>effective_at<br/>recorded_at<br/>snapshot version"]

    QUERY["ContextQL algebra<br/><br/>IN / ALL / NOT<br/>union / intersection / difference<br/>AT / BETWEEN / AT VERSION"]
    RESULT["Surviving transaction rows<br/>plus scores and narrowed evidence"]

    TX -->|"evaluate properties"| DEF1
    TX -->|"evaluate properties"| DEF2
    TX -.->|"later, without changing existing contexts"| FUTURE

    DEF1 -->|"refresh"| SNAP1
    DEF2 -->|"sync / refresh"| SNAP2
    FUTURE -.->|"populate later"| SNAP3

    SNAP1 --> HIST
    SNAP2 --> HIST
    SNAP3 -.-> HIST

    SNAP1 --> QUERY
    SNAP2 --> QUERY
    SNAP3 -.-> QUERY
    TX --> QUERY
    QUERY --> RESULT
```

Each context has a stable `context_id` and owns versioned immutable membership
snapshots. A snapshot is the Roaring bitmap; timestamps, definition hash,
watermark, and version are snapshot metadata. The shared history records when
individual transaction memberships or scores changed.

Adding another context later creates a new definition and its own snapshots.
It does not require altering the transaction table or rewriting existing
context bitmaps.
