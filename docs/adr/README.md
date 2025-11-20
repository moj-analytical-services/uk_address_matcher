# Architectural Decision Records

## What is an ADR?

> An Architectural Decision (AD) is a justified design choice that addresses a functional or non-functional requirement that is architecturally significant. An Architecturally Significant Requirement (ASR) is a requirement that has a measurable effect on the architecture and quality of a software and/or hardware system.

ADRs are a way to capture the context, decision, and consequences of a design choice. They are a lightweight method to document the decisions made in a project.

## What is our plan for recording the architectural decisions?

We will document design decisions to ensure we preserve the context of our choices. These will be written in the format proposed by Michael Nygard in his [blog post](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions).

All existing ADRs can be found within the [decisions directory](./decisions/). For a working example of effective ADR use, see the [GOV.UK AWS repository](https://github.com/alphagov/govuk-aws/tree/main/docs/architecture).

### Tooling

We will use [adr-tools](https://github.com/npryce/adr-tools) to help manage the decisions.

Install adr-tools:
```sh
brew install adr-tools
```

Create a new ADR:
```sh
adr new 'Decision to record'
```

Please ensure that this tool is used at the root of the repository only.

> 📝 If you wish to create an ADR manually, you can use the template provided in [0000-architectural-decision-record-template.md](./0000-architectural-decision-record-template.md).

<hr>

### Guidance

#### When should I write an ADR?

- **When you need to make a decision** - If you are making a decision that is hard to reverse, write an ADR.
- **When you need to record a decision** - If you are making a decision that is likely to be revisited, write an ADR.
- **When you need to communicate a decision** - If you are making a decision that needs to be communicated to others, write an ADR.

#### What should I include in an ADR?

To keep things simple, we've opted to implement a [lightweight template for our ADRs](https://github.com/peter-evans/lightweight-architecture-decision-records). Each ADR should include the following sections:

- **Title** - A short, descriptive title.
- **Status** - The current status of the decision (e.g., proposed, accepted, rejected).
- **Context** - The issue motivating the decision, and any context that influences or constrains the decision.
- **Decision** - The change that is being proposed or agreed to.
- **Consequences** - What becomes easier or more difficult to do, and any risks introduced by the change that will need to be mitigated.

<hr>

### Useful Links

- [ADR Github organisation](https://adr.github.io/)
- Case Study: [**govuk-aws**](https://github.com/alphagov/govuk-aws/tree/main/docs/architecture/decisions)
- ["How to create Architectural Design Records (ADRs) - and how not to"](https://www.ozimmer.ch/practices/2023/04/03/ADRCreation.html)
- [Spotify blog - "When should I write an ADR"](https://engineering.atspotify.com/2020/04/when-should-i-write-an-architecture-decision-record/)

