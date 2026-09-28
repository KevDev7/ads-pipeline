# Earlier attempts: preserve the failure history

These attempts preceded the completed three-pair comparison. They are **not**
pooled into its timing medians. The initial suite failed; the later standalone
repeat succeeded. Both used clean commit `71ca428e42978ff4597fd5156bd5f72ba9dff32c`
and identical source hashes, runtime/SQL settings, `local[4]`, and a 2 GiB heap.

| Attempt | Outcome | Report |
| --- | --- | --- |
| Initial control | All eight tables validated; 35.514 s processing, 6.933 s Gold | [Report](initial-control.json) |
| Initial profile variant | Campaign validated; audience failed during sorting | [Failed report and exception](initial-variant-failed.json) |
| Standalone profile repeat | All eight tables validated; 44.864 s processing, 17.919 s Gold | [Report](variant-confirmation.json) |

The [initial comparison](initial-comparison.json) and [suite status](initial-suite.json)
remain failed. The requested three pairs were aborted at the first variant;
full-data cross-run equality, suite join checks, and codec checks were not reached.
The standalone repeat ran in-pipeline validations, not a separate exact cross-run
comparison. The later completed series supplies the 40 exact comparisons.

[Extracted execution and task evidence](prior-attempts.json) includes SQL-end
error status. The initial campaign completed successfully; the audience SQL
execution ended with an error. Its latest plan is a **failed execution plan**,
not proof of a successful result. Task counters from that run describe incomplete
work and must not be compared as if the full pipeline finished.

The failure was `SparkOutOfMemoryError` / `UNABLE_TO_ACQUIRE_MEMORY` while
`UnsafeExternalSorter` tried to acquire another 65,536 bytes. The full exception
is retained in the failed report. Available evidence identifies the allocation
site, not every contributor to the run-to-run memory variation.

Latest plan snapshots:

- [Initial control campaign](initial-control-gold.campaign_daily.txt)
- [Initial control audience](initial-control-gold.audience_daily.txt)
- [Initial variant campaign](initial-variant-failed-gold.campaign_daily.txt)
- [Initial variant failed audience](initial-variant-failed-gold.audience_daily.txt)
- [Successful repeat campaign](variant-confirmation-gold.campaign_daily.txt)
- [Successful repeat audience](variant-confirmation-gold.audience_daily.txt)

Raw reports are copied unchanged. Plans replace the workspace path with `<repo>`.
The small extracted JSON was derived by reading `SparkListenerStageSubmitted`,
latest `sparkPlanInfo`, and `SparkListenerSQLExecutionEnd` events, plus the existing
measurement module's task-counter summarizer. Raw event/process logs remain local:

- `outputs/comparisons/profile-join-05-20260927/`
- `outputs/comparisons/profile-join-05-20260927-suite-logs/`
- `outputs/05_profile_shuffle_join/confirm-2g-20260927/`
- `work/profile-join-confirm.log`

[Return to the main findings](../README.md).
