---
name: dv-when
description: Decide whether Data Vault 2.0 is the right architecture for a given use case, and what to consider before starting
enabled: true
---

# /dv-when — When to Choose Data Vault 2.0

Help the user decide whether DV2.0 is the right fit, and what to consider before committing.

## Ask first

If the user hasn't described their context, ask:

> "Tell me about your situation: How many source systems? What's driving the project (audit, BI, integration, compliance)? Do schemas change often? How big is the team?"

Then use the guide below to give a recommendation.

---

## Strong fit — choose Data Vault

✅ **Multiple source systems** (3+) feeding the same business entities from different angles

✅ **Schema volatility** — sources change structure frequently; you need the vault to absorb changes without breaking downstream consumers

✅ **Full history required** — you need to reconstruct any past state of the business (regulatory, audit, dispute resolution)

✅ **Multi-source integration** — same business entity (e.g. customer) exists in CRM, ERP, billing, and you need to reconcile them

✅ **Separation of concerns matters** — business rules change often; you need raw history separated from business logic so you can re-derive without re-loading

✅ **Long-lived platform** — you're building a backbone that will be extended over 5+ years by multiple teams

✅ **Regulated industries** — finance, healthcare, government — where lineage and traceability to source are non-negotiable

---

## Weak fit — consider alternatives

⚠️ **One or two stable sources** → Medallion (bronze/silver/gold) is simpler and faster to deliver

⚠️ **Speed to first delivery is the primary constraint** → Vault has upfront design cost; Medallion gets you to BI faster

⚠️ **Small team with no Vault experience** → The insert-only discipline and hash key patterns require training; Medallion is more forgiving

⚠️ **Purely operational reporting** (today's data, no history needed) → A simple staging + reporting layer is sufficient

⚠️ **One-time analytics project** → Not worth the structural investment

---

## Not either/or

Data Vault and Medallion can coexist:

```
Sources → Raw Vault (DV2.0) → Information Mart → BI tools
                                     ↑
              (Marts can be star schema / dimensional — built on top of the vault)
```

The vault handles multi-source history. The Information Mart handles BI access. They serve different layers.

---

## Common objections and answers

**"Data Vault is too complex."**
The initial design (hash keys, insert-only, satellite variants) has a learning curve. Once it's running, adding a new source is mechanical — you add a satellite without touching existing tables. That's the payoff.

**"We don't need full history."**
You don't know that yet. Regulations change. Disputes happen. The cost of having history you don't use is storage. The cost of not having history when you need it is a re-load from source — if the source still has it.

**"Hash keys are ugly in reports."**
They never appear in reports. Information Mart views substitute business keys. End users never see a BINARY column.

**"We tried it and it was slow to query."**
Usually caused by querying Raw Vault directly from BI tools, skipping PIT tables, or not building Information Mart views. Vault + PIT + IM views is fast.

---

## Before you start

Confirm these four things before designing the vault:

1. **Business keys are identified** — you know the natural identifier for each entity across all source systems
2. **Record source granularity is agreed** — how specific your RSRC values will be (system + schema + table, not just "CRM")
3. **Load frequency is known** — batch daily, near-real-time, or event-driven? This affects staging design
4. **History scope is agreed** — full history from day one, or from a cutover date?

If any of these are unknown, resolve them before modeling. Wrong business key choices are expensive to fix.

---

## Output

Summarize the recommendation:

```
RECOMMENDATION
==============
Pattern: Data Vault 2.0  /  Medallion  /  Start with Medallion, migrate to Vault later
Confidence: high / medium / low

Reasons for:
  - <reason 1>
  - <reason 2>

Reasons against / risks:
  - <risk 1>

Next step: /dv-discover to begin source analysis
       or: discuss further with /dv-explain
```
