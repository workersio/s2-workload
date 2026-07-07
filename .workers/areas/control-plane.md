---
key: control-plane
title: Control plane
description: >-
  Stream create / reconfigure / delete / list — the metadata plane. All of
  it commits bare SerializableSnapshot txns with ZERO durability gating
  (streams.rs:212, 319, 376: txn.commit().await then 200), in contrast to
  the data plane where acks wait on durable_seq (durability_notifier.rs).
order: 50
---

# Control plane

The uncovered plane: every promise so far attacks record durability; the
metadata ops that CREATE the streams have no durability gate at all. An
acked create/reconfigure/delete that un-happens across a SIGKILL is
acked-write-loss with a different key prefix.

Open (beyond the promoted promise): ensure-reconfigure config clobber
(empty PUT resets to defaults, streams.rs:133-151); OCC conflict storm
(#466, error.rs:88-89); list-pagination census (has_more truthfulness,
streams.rs:35-77).
