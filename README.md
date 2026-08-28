# Blog Monitor on Kestra

Wrapping a working Python script in [Kestra](https://kestra.io) without rewriting it.

A script checks ~130 blog sources once a day, ranks new posts with Claude, and
sends one grouped email. It worked fine as a cron job. This repository is the
record of moving it onto an orchestration platform, and of what that surfaced.

## Why this exists

The interesting part is not that Kestra can run a Python script. It is that
wrapping a script which had been running correctly for months made a real bug
visible within a day.

The digest run normally takes four minutes. One morning it took fifty. Nobody
had noticed, because the mail still arrived, just later, and cron reports
neither runtime nor failure.

Two sources, one rendering client-side and one behind a CDN, were opening a TCP
connection and then going silent. Not failing, not timing out: silent. The
script fetches through a `ThreadPoolExecutor` and waits on `as_completed()` with
no timeout, so a single future that never returns holds the entire run. A socket
timeout does not help, because it restarts on every byte received.

What made it visible was not better code. It was a runtime display and a
timeout, both of them configuration in the orchestration layer.

## Approach

Strangler fig, in phases, with production untouched throughout.

| Phase | What happens |
|---|---|
| 1. Wrap | The script runs unchanged inside Kestra, against a parallel copy of its directory with its own state. Kestra provides the schedule, timeout, concurrency limit and secrets. Zero Python changes. |
| 2. Run in parallel | Cron at 08:00 sends the real mail. Kestra at 10:00 does a dry run: no mail, no state written. Compare daily. |
| 3. Cut over | Disable cron, switch mode to send, point at the production directory. Keep cron commented out for rollback. |
| 4. Go native | Decompose the fetch loop into tasks, emit metrics, fix `as_completed()`. Only now touch the Python. |

Phase 1 delivers most of the operational value with none of the rewrite risk,
which is the argument. Teams stall on migrations because they assume
orchestration means rewriting. It does not.

## What is here

| File | Purpose |
|---|---|
| `blog_monitor_flow.yml` | Phase 1 wrapper. Runs the unmodified script in a container with the working directory mounted. |
| `source_health.yml` | Weekly sweep across every source, sequential with a hard budget each. Opens a Kestra Case when sources fail, and attaches itself as a one-click re-check. |
| `source_check_single.yml` | Probe one source on demand. |
| `check_sources.py` | The diagnostic. Sequential, bounded, read-only. Reports hung sources, failing sources, and parked sources that have recovered. |
| `park_sources.py` | Moves a broken source into `parked_sources` instead of deleting it, with the reason and what a fix would need. |
| `deploy.sh` | Deploys every flow in this directory. |
| `blog_monitor.py` | The script being migrated. Unmodified by phase 1; carries the `as_completed` fix from phase 4. |
| `config.json` | Sources, pillars, ranking settings, and the `parked_sources` block. Email addresses are placeholders. |

Not in the repository, and gitignored: `.env`, `seen_posts.json` (~2 MB of post
IDs, rewritten every run) and `kestra.auth`.

## The fix

Three changes to `blog_monitor.py`, roughly ten lines, all in service of one
rule: no single source may hold the run.

```python
# 1. as_completed with no timeout waits forever on a future that never returns
for fut in as_completed(futures, timeout=total_budget):
    ...
except FuturesTimeout:
    pass

# 2. anything that never came back is reported, not silently dropped
for fut, src in futures.items():
    if fut not in finished:
        results.append({..., "error": f"no response within {total_budget}s, abandoned"})

# 3. no `with` block: leaving one calls shutdown(wait=True), which joins the
#    hung threads and blocks anyway
pool.shutdown(wait=False, cancel_futures=True)
```

And a fourth that only shows up once the first three work:

```python
if __name__ == "__main__":
    main()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)
```

A thread stuck in a socket read cannot be cancelled, and Python joins every
non-daemon thread at interpreter exit. Without `os._exit` the process hangs
*after* the mail has been sent and the state written. Under an orchestrator
that means a successful run reported as a timeout failure. Verified in an
isolated test: the logic finished in 2 seconds and the process still had to be
killed.

Measured, same scenario with two deliberately silent sources:

| | Result |
|---|---|
| Before | Hangs indefinitely. Killed at 20s in the test. |
| After | Completes in 3s, both sources reported as `no response within 3s, abandoned`, exits 0. |

## Two ideas worth stealing

**Parked sources.** Deleting a broken source loses the reason it was removed.
Three months later nobody remembers whether it is worth retrying. A
`parked_sources` block in `config.json` is ignored by the digest, so it cannot
hang the run, but the health check still measures it. A retired source stays
visible and comes back automatically if it recovers.

**Hanging is not failing.** A source returning 403 costs a fraction of a
second. A source that connects and then goes silent costs the whole run. The
health check reports these separately, because they need different responses:
one is a decision about content, the other is a bug in the fetch loop.

## Setup

Kestra Enterprise, because this uses Cases. The flows themselves work on OSS if
you remove the `CreateCase` task.

```bash
cp kestra.auth.example kestra.auth   # fill in host, token, namespace
chmod 600 kestra.auth
./deploy.sh flow
```

On the Kestra host, create a parallel copy of the script directory so the
migration never shares state with production:

```bash
mkdir -p ~/blog_monitor_kestra
cp ~/blog_monitor/{blog_monitor.py,config.json,seen_posts.json} ~/blog_monitor_kestra/
chmod 644 ~/blog_monitor_kestra/*
```

Then add `ANTHROPIC_API_KEY`, `BLOG_MONITOR_PASSWORD` and `KESTRA_API_TOKEN` as
namespace secrets. No `.env` file is needed: the script reads real environment
variables in preference to `.env`, so Kestra's secrets win.

## Notes for anyone doing the same

Things that cost time and are not obvious from the docs:

- Input type `BOOLEAN` does not exist. Use `SELECT` with two values, which also
  gives you a dropdown instead of a field a typo can silently break.
- Retry is `maxAttempts`, not `maxAttempt`.
- Pebble has no `?:` operator. Use `??`, and note it only fires on null, not on
  an empty string.
- Case actions in 2.0 take no inputs and fire immediately, so any flow attached
  as an action must have defaults for every input.
- Dashboard charts group by metric **name** only, not by metric tags. Every
  dimension you want to slice by has to be encoded in the metric name.
- A script task derives its paths from `__file__`, not the working directory,
  so mounting a different directory is enough to isolate a parallel run.
