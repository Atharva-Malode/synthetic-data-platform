# AI-Assisted Synthetic Data Modeling Considerations

## 1. Purpose

The AI assistant should help a user describe a synthetic dataset, discover missing requirements, propose a relational model, and produce a validated generation specification. It should not directly invent and execute millions of rows.

The workflow is:

```text
User intent
  -> clarifying questions
  -> proposed entities and relationships
  -> conditions and distributions
  -> scale and cost estimate
  -> validation warnings
  -> user confirmation
  -> executable JSON specification
  -> deterministic generation job
```

The AI is responsible for planning and explanation. The deterministic application is responsible for validation, generation, persistence, streaming, and exports.

## 2. Responsibilities and Boundaries

### AI responsibilities

- Understand the requested business domain.
- Identify likely entities and suggest missing ones.
- Ask targeted questions in a sensible order.
- Explain relationships and assumptions.
- Convert confirmed answers into a versioned JSON specification.
- Identify contradictions, ambiguity, and unsupported requirements.
- Summarize row estimates, storage estimates, and execution risks.

### Application responsibilities

- Parse and validate the JSON specification.
- Reject invalid expressions, references, cycles, and unsafe values.
- Build the dependency graph.
- Enforce hard limits and authorization.
- Generate deterministic records from a seed.
- Preserve referential integrity.
- Publish Kafka events, write PostgreSQL data, manage Redis state, and create exports.
- Record the exact specification used for every job.

The AI must never have unrestricted shell access, arbitrary SQL access, direct production credentials, or permission to bypass validation and confirmation.

## 3. Conversation Goals

The assistant should gather enough information to answer these questions:

1. What domain is being modeled?
2. Which entities and events are required?
3. What does each entity represent?
4. How are entities related?
5. How many records are required for each entity?
6. Which conditions control whether records are created?
7. Which fields are generated directly, derived, or copied from related entities?
8. How should values be distributed?
9. What time period and temporal behavior are required?
10. Which quality issues or anomalies should be intentionally created?
11. Which output formats and destinations are required?
12. What scale is safe for the selected environment?
13. Does the user understand and confirm the estimate?

The assistant should ask only the next questions needed to remove meaningful ambiguity. It should offer sensible defaults, but every default must be visible in the final plan.

## 4. Required Question Areas

### 4.1 Domain and purpose

Ask:

- What business or operational domain should the data represent?
- What will the dataset be used for: development, testing, analytics, demos, load testing, or training?
- Should the data represent normal behavior, edge cases, failures, or a mixture?
- Should the output resemble an existing schema or be designed from scratch?

The purpose affects the required realism, anomalies, volume, and output behavior.

### 4.2 Entity discovery

Ask the user to identify primary entities, supporting entities, and event entities.

Examples:

- Users or customers
- Accounts
- Products
- Plans
- Subscriptions
- Orders
- Payments
- Sessions
- Activity events
- Support tickets

For every entity, capture:

- Name
- Description
- Business meaning
- Estimated or exact count
- Identifier strategy
- Required fields
- Optional fields
- Creation and update behavior
- Whether it is a current-state table or an event/history table

The assistant should distinguish an entity from an event. A subscription is usually a stateful entity; a subscription-created record is an event.

### 4.3 Relationships

For each relationship, capture:

- Parent entity
- Child entity
- Parent key
- Child foreign key
- Cardinality: one-to-one, one-to-many, or many-to-many
- Whether the relationship is required or optional
- Minimum and maximum related records
- Selection strategy: random, weighted, cohort-based, or business-rule-based
- Whether relationships change over time
- Delete or cancellation behavior

The assistant must ask about orphan behavior explicitly. A child record should not reference a nonexistent parent unless the user specifically requests invalid test data.

### 4.4 Field definitions

For every field, capture:

- Name
- Data type
- Required or nullable
- Default behavior
- Uniqueness
- Format or pattern
- Minimum and maximum value
- Allowed values
- Weighted values
- Null percentage
- Generation method
- Derived expression
- Sensitivity classification

Supported initial field types should be deliberately limited, for example:

```text
uuid
string
integer
decimal
boolean
date
datetime
email
phone
country
currency
json
reference
choice
```

A field should not accept arbitrary executable code. Derived fields should use a restricted expression language with a documented function set.

## 5. Conditions and Business Rules

Conditions are central to realistic correlated data. Represent them as structured rules rather than natural-language strings whenever possible.

A condition should contain:

- Stable rule ID
- Human-readable description
- Scope or entity
- Evaluation timing
- Predicate
- Action
- Priority when rules overlap
- Behavior when no rule matches

Example:

```json
{
  "ruleId": "cancelled-users-stop-activity",
  "scope": "activity_events",
  "when": {
    "all": [
      { "field": "users.status", "operator": "equals", "value": "cancelled" },
      { "field": "activity_events.occurred_at", "operator": "after", "fieldRef": "users.cancelled_at" }
    ]
  },
  "then": {
    "action": "do_not_generate"
  }
}
```

The system should support safe operators such as:

```text
equals, not_equals, in, not_in, greater_than, greater_or_equal,
less_than, less_or_equal, between, before, after, is_null, is_not_null
```

It should support logical groups:

```text
all, any, not
```

The assistant should ask:

- Does the rule apply to all records or a segment?
- Is the rule absolute or probabilistic?
- Does it apply at creation time or across the timeline?
- What happens when two rules conflict?
- Should the rule prevent a record, change a field, or create a related event?

## 6. Filters and Selection Rules

Filters determine which records can participate in a relationship or activity. They must be explicit because a vague filter can produce unrealistic correlations.

Example:

```json
{
  "relationship": "orders.customer_id -> customers.id",
  "parentSelection": {
    "filter": {
      "all": [
        { "field": "status", "operator": "equals", "value": "active" },
        { "field": "country", "operator": "in", "values": ["US", "CA"] }
      ]
    },
    "weightBy": "lifetime_value",
    "fallback": "use_any_parent"
  }
}
```

The assistant must clarify:

- Whether filters apply before or after distribution.
- Whether an empty result is an error or should use a fallback.
- Whether selection is with or without replacement.
- Whether the same parent can receive unlimited children.
- Whether filters can reference related entities.
- Whether filter evaluation changes as time advances.

## 7. Correlations and Realism

Independent random fields usually do not look realistic. The assistant should identify important correlations and describe their direction and strength.

Examples:

- Country influences timezone, currency, language, and working hours.
- Subscription plan influences price, feature usage, and activity volume.
- Account age influences retention and purchase frequency.
- Device type influences session duration.
- Cancellation changes future activity generation.
- User cohort influences onboarding events.
- Payment failures increase the probability of churn.

Represent correlations as controlled rules or distributions:

```json
{
  "correlations": [
    {
      "name": "plan-affects-activity",
      "source": "subscriptions.plan",
      "target": "activity_events.daily_count",
      "mapping": {
        "free": { "distribution": "poisson", "lambda": 2 },
        "pro": { "distribution": "poisson", "lambda": 8 },
        "enterprise": { "distribution": "poisson", "lambda": 20 }
      }
    }
  ]
}
```

The assistant should avoid claiming that data is statistically realistic without a defined basis. It should describe whether realism comes from rules, supplied distributions, or a learned model.

## 8. Time and Lifecycle Modeling

The assistant must collect:

- Start date and end date
- Time zone strategy
- Event timestamp precision
- Business hours and quiet periods
- Weekday versus weekend behavior
- Seasonal or monthly patterns
- Entity creation dates
- State transitions
- Late-arriving events
- Backdated records
- Clock skew, if needed

For stateful entities, define valid transitions:

```text
trial -> active -> paused -> cancelled
```

Every transition should specify:

- Allowed previous states
- New state
- Probability or triggering condition
- Earliest and latest transition time
- Whether a corresponding event is created
- Whether future child records are allowed

The generator must not create events after an entity has been permanently deactivated unless the user explicitly requests invalid data.

## 9. Scale and Execution Estimates

The assistant must separate these concepts:

- Number of primary users
- Number of entities
- Number of events
- Rows per entity
- Events per user per day
- Total date range
- Peak rate
- Batch size
- Output size
- Kafka message volume
- PostgreSQL storage
- Temporary disk requirements
- Expected execution time

A simple estimate may begin with:

```text
estimated_events = users x average_events_per_user_per_day x number_of_days
```

The final estimate should include a range when distributions are probabilistic. It should also call out peak-day behavior.

Before execution, show:

- Estimated rows per table
- Estimated Kafka messages
- Estimated PostgreSQL storage
- Estimated export size
- Estimated runtime
- Local or cloud resource requirements
- A warning when the request exceeds configured limits

The user must explicitly confirm jobs above the configured threshold.

## 10. Output and Delivery Requirements

Ask which outputs are needed:

- CSV
- Parquet
- ZIP archive
- PostgreSQL tables
- Kafka events
- Object storage such as S3

Capture:

- One file per entity or partitioned files
- Compression format
- Partition columns for Parquet
- File naming convention
- Include schema JSON in the ZIP
- Include data dictionary
- Include validation report
- Include checksums
- Retention period
- Download access and expiration

The generated ZIP should ideally contain:

```text
manifest.json
schema.json
data_dictionary.json
validation_report.json
users/*.csv or *.parquet
activity_events/*.csv or *.parquet
checksums.txt
```

## 11. Data Quality and Anomaly Controls

The user should choose whether the dataset is clean, imperfect, or deliberately adversarial.

Clean-data checks:

- Unique primary keys
- Valid foreign keys
- Required fields populated
- Values within bounds
- Valid state transitions
- Expected row counts
- Timestamp ordering
- No duplicate event IDs

Optional anomaly controls:

- Duplicate events
- Missing events
- Invalid foreign keys
- Null bursts
- Late-arriving events
- Out-of-order events
- Schema-version differences
- Repeated failed payments
- Suspicious activity spikes

Every anomaly must be labeled and measurable. The assistant must never silently produce invalid data when the user asked for valid data.

## 12. Privacy and Safety

Synthetic data must not reproduce personal data from a source dataset unless the user has a lawful and approved process for doing so.

The assistant should ask:

- Is a real dataset being supplied?
- Does it contain personal, financial, health, or confidential information?
- Is the goal to learn distributions or copy values?
- Should direct identifiers be generated from scratch?
- Which fields require masking or removal?

Default behavior:

- Generate identifiers from scratch.
- Use reserved or clearly synthetic domains for emails where appropriate.
- Never expose secrets or credentials in generated output.
- Never copy source rows directly.
- Avoid generating real-looking payment credentials.
- Add a synthetic-data marker to metadata.

## 13. Schema and Expression Validation

Before generation, validate:

- JSON syntax and schema version
- Entity names and field names
- Duplicate identifiers
- Unsupported data types
- Missing references
- Relationship cardinality
- Cycles in required dependencies
- Invalid count expressions
- Invalid date ranges
- Invalid probability values
- Distribution totals
- Rule references
- Type compatibility in derived fields
- Conflicting uniqueness and null settings
- Unsupported filter operators
- Maximum scale limits

Do not evaluate arbitrary C#, Go, SQL, JavaScript, or shell code from the specification. Use a small parser and allowlist of functions and operators.

## 14. Versioning and Reproducibility

Every generation job should record:

- Specification version
- Generator version
- Application build version
- Random seed
- Dependency versions where relevant
- Input specification checksum
- Resolved plan checksum
- Generation timestamp
- Environment name

A rerun with the same version, seed, specification, and configuration should produce equivalent logical records. If exact byte-for-byte output is not guaranteed, document why.

Changing the schema should create a new specification version. Never silently reinterpret an old specification.

## 15. Agent Conversation State

The requirements session should track:

```text
started
collecting_domain
collecting_entities
collecting_relationships
collecting_conditions
collecting_scale
collecting_outputs
reviewing_plan
awaiting_confirmation
submitted
cancelled
```

The assistant should retain answers in a structured session object instead of relying only on conversational history. Redis may hold short-lived session state, while the final plan and job record belong in PostgreSQL.

The assistant should distinguish:

- User-provided requirements
- AI suggestions
- Defaults
- Assumptions
- Validation errors
- Warnings
- Confirmed decisions

The final review should show these separately.

## 16. Recommended Question Order

1. Ask for the domain and purpose.
2. Ask for primary entities.
3. Ask for important relationships.
4. Ask for user count and total scale.
5. Ask for date range and lifecycle behavior.
6. Ask for important fields and sensitive fields.
7. Ask for conditions, filters, and correlations.
8. Ask for distributions and anomaly requirements.
9. Ask for output formats and destinations.
10. Generate the proposed model and relationship explanation.
11. Validate the specification.
12. Show estimates, warnings, and assumptions.
13. Ask the user to correct or confirm.
14. Start the job only after explicit confirmation.

Do not ask every possible question up front. Use progressive disclosure and stop asking once the specification is complete enough to generate safely.

## 17. Review Screen or Agent Summary

Before execution, show a concise summary such as:

```text
Dataset: subscription-platform
Period: 2026-01-01 through 2026-06-30
Users: 100,000
Entities: users, subscriptions, payments, activity_events
Estimated total rows: 182,400,000
Estimated PostgreSQL storage: 42 GB
Estimated export size: 9 GB
Outputs: PostgreSQL, Kafka, CSV, Parquet, ZIP
Seed: 20260911

Important relationships:
- One user can have multiple activity events.
- Each subscription belongs to one user.
- Payment amount depends on subscription plan.
- Cancelled users stop generating normal activity.

Warnings:
- Estimated volume exceeds the local development limit.
- This job requires confirmation.
```

The user should be able to edit the plan before confirming it.

## 18. MCP Tool Design

Tools should be narrow, typed, observable, and safe to retry.

Recommended tools:

```text
start_requirements_session
save_requirement_answer
get_requirements_summary
validate_generation_spec
build_relationship_graph
estimate_generation
create_generation_plan
confirm_generation_plan
start_generation_job
get_generation_status
cancel_generation_job
list_exports
```

Tool responses should include structured data, not only prose. For example, `estimate_generation` should return numeric estimates, warnings, limits, and an explicit `requiresConfirmation` value.

The MCP layer should pass a plan ID rather than accepting arbitrary generation instructions at execution time. `start_generation_job` should require a confirmed plan and should be idempotent.

## 19. Definition of a Complete Plan

A plan is ready for execution only when it has:

- A versioned schema
- At least one primary entity
- Valid identifiers
- Valid relationship references
- Resolved counts
- A valid date range
- A seed
- Explicit output configuration
- Resolved required assumptions
- Validation results
- Scale and storage estimates
- A confirmation policy result

If any required item is missing, the agent should ask a question or mark the plan as not executable.

## 20. Initial Scope Recommendation

For the first implementation, support:

- Two entities: `users` and `activity_events`
- One-to-many relationships
- UUIDs and basic scalar fields
- Weighted choices
- Date ranges
- Simple filters
- Seeded generation
- CSV, Parquet, and ZIP output
- PostgreSQL persistence
- Kafka publication
- Redis job progress

Add many-to-many relationships, complex rule precedence, learned distributions, anomaly injection, and multi-region behavior only after the core conversation-to-plan-to-generation path is reliable.
