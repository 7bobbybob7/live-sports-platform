# Schema evolution

Protobuf is the canonical schema layer. See **[`proto/README.md`](proto/README.md)** for the evolution rules.

The Pydantic-style aliases in [`schemas/mlb.py`](mlb.py) are thin re-exports over the generated proto classes and have no evolution story of their own — any evolution happens in the `.proto` source.
