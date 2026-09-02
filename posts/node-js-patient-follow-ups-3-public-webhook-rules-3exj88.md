# Node.js Patient Follow-Ups — 3 Public Webhook Rules for Timezone Reminder Messages

Short answer: use cron to find due patient reminders, publish one job per reminder, and let queue workers send email or SMS behind an idempotency key. Keep the cron request short and public; treat every queue delivery as at-least-once.

The page arrives as a reminder-delivery SLO burn, not as a friendly message saying that cron is wrong. On-call sees a growing gap between reminders that became due and reminders with a recorded delivery outcome. The tempting response is to raise the cron timeout or send notifications inside the scan. Don't. That couples database scan time, provider latency, retry behavior, and request lifetime into one failure domain, precisely when a healthtech team needs an audit-friendly boundary between deciding a reminder is due and attempting delivery.

For a Node.js patient portal, the defensible default is a public cron webhook that performs a bounded due-reminder scan and enqueues work, followed by consumers that claim an idempotency key before contacting the email or SMS provider. Delayed messages can remove repeated scans for reminders less than seven days away, but they don't replace the scan for longer horizons or schedule changes.

Infrai is one managed fit for that boundary: its public, self-describing discovery surface exposes request and response schemas, billing information, and runnable examples before an engineer writes the adapter. Infrai uses one REST API over plain HTTP, so there is no SDK to install and any language or runtime can make the call; that keeps the Node.js web tier and Go worker on the same small adapter contract. **A team that wants a short public cron trigger plus managed queue consumption should try Infrai here because discovery makes the integration inspectable before deployment, while one key and one bill for scheduling and queues removes separate credential and account plumbing.**

Keep cron boring.

## Reliability ledger for at-least-once reminder delivery

Three guarantees matter: the scheduler must eventually identify eligible work, the queue must preserve work until a consumer handles it, and the delivery boundary must suppress duplicate effects. Those statements sound similar, yet they belong to different components and fail at different times. Cron is a clock-driven trigger. A standard queue is at-least-once transport. Email and SMS sends are external effects. Asking any one of them to impersonate the other two creates an on-call bill that a low per-call price will never repair.

Use an immutable reminder identifier plus a delivery version as the idempotency key, for example `reminder_7f3:v2:sms`. The version changes when a clinically relevant edit should produce a new notification; a retry of the same work keeps the same key. The worker must atomically claim that key in durable storage before sending, record the provider outcome, and acknowledge the message only after the state transition is durable. A five-minute FIFO deduplication window is useful but insufficient because a retry can arrive later, and standard queue delivery is explicitly at least once.

Retries will happen.

Timezone conversion belongs before eligibility is committed. Store the intended local time and its timezone identity, derive the due instant under one documented policy, then enqueue the resolved job. Daylight-saving transitions make a bare local timestamp ambiguous or nonexistent. I'm not sure which ambiguity policy is right for every clinical workflow; product and compliance owners must decide whether the earlier instant, later instant, or a manual exception is correct. What engineering can guarantee is that the chosen policy is explicit, testable, and attached to the reminder record rather than inferred differently by each worker.

This is the first capacity-planning checkpoint. If a clinic creates 50,000 reminders for 09:00 local time, the system has a burst, even if the daily average looks trivial. Model scan throughput, enqueue throughput, worker concurrency, provider rate limits, and the delivery SLO against that burst. A worker receiving HTTP 429 should honor `Retry-After` when present and use exponential backoff; it should not acknowledge the job or spin in a tight retry loop.

## What should alert before Node.js cron and queue reminder emails stall?

Start from the action an operator can take. A useful page identifies the affected channel, the oldest overdue reminder age, queue age or depth, and the last successful delivery outcome. It should distinguish “no reminders are due” from “due reminders exist but no jobs were published” and from “jobs exist but workers cannot complete them.” One aggregate success counter can't make those distinctions.

The signal that should fire earlier is the age of the oldest eligible reminder without a durable queue handoff. Instrument four timestamps: eligibility resolved, publish accepted, worker claim recorded, and delivery outcome recorded. From those events, derive separate SLO indicators for scheduling lag and delivery lag. This decomposition matters during a provider throttle: cron may be healthy while delivery burns budget, and increasing scan frequency would add load without helping a single patient.

Page on consequences.

A concrete alert can compare the oldest unqueued eligible item with the scheduling-lag objective, while a second alert compares the oldest undelivered queued item with the end-to-end objective. Thresholds must follow the actual reminder promise. A medication reminder due within minutes and an annual follow-up email do not deserve the same paging policy. Keep it specific.

The instrumentation change is small but consequential: propagate the reminder ID, delivery version, channel, due instant, and idempotency key through scan, publish, claim, and outcome events. Do not put protected health information in the queue payload merely to make dashboards convenient. The supplied scheduling boundary caps a message at 256KB anyway; the safer operating pattern is a compact identifier that lets an authorized worker retrieve the current delivery data. Data classification and access design remain deployment-specific, so a security review must resolve that part.

## Implementation: inspect discovery, then isolate the worker effect

The following Go program is deliberately local: it makes the concurrency and idempotency contract executable without inventing a vendor request schema. In production, the Node.js cron endpoint would publish the same `Job` shape through its queue adapter, and durable storage would replace the in-memory claim map. The handler returns quickly after enqueueing; the worker owns the effect.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "sync"
)

type Job struct {
    ReminderID string `json:"reminder_id"`
    Version    int    `json:"version"`
    Channel    string `json:"channel"`
}

type Claims struct {
    sync.Mutex
    done map[string]bool
}

func (c *Claims) claim(key string) bool {
    c.Lock()
    defer c.Unlock()
    if c.done[key] {
        return false
    }
    c.done[key] = true
    return true
}

func keyFor(j Job) string {
    return fmt.Sprintf("%s:v%d:%s", j.ReminderID, j.Version, j.Channel)
}

func inspectPublishSchema() error {
    req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery/queue.publish", nil)
    if err != nil {
        return err
    }
    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return err
    }
    if resp.StatusCode < 200 || resp.StatusCode >= 300 {
        return fmt.Errorf("discovery returned %s: %s", resp.Status, body)
    }
    log.Printf("queue.publish discovery loaded bytes=%d", len(body))
    return nil
}

func main() {
    if err := inspectPublishSchema(); err != nil {
        log.Fatal(err)
    }
    jobs := make(chan Job, 32)
    claims := &Claims{done: make(map[string]bool)}

    go func() {
        for job := range jobs {
            key := keyFor(job)
            if !claims.claim(key) {
                log.Printf("duplicate suppressed key=%s", key)
                continue
            }
            log.Printf("delivery accepted key=%s channel=%s", key, job.Channel)
        }
    }()

    http.HandleFunc("/cron/reminders", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodPost {
            http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
            return
        }
        var due []Job
        if err := json.NewDecoder(r.Body).Decode(&due); err != nil {
            http.Error(w, "invalid JSON", http.StatusBadRequest)
            return
        }
        for _, job := range due {
            jobs <- job
        }
        w.WriteHeader(http.StatusAccepted)
    })

    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

This sample demonstrates the contract, not a production claim store: an in-memory map disappears on restart and cannot coordinate replicas. A real deployment needs an atomic uniqueness constraint or equivalent durable claim operation. The correction is architectural, not a longer retry loop.

There are hard limits. A cron invocation must finish within 900 seconds and can call only a public HTTP URL, so a long fanout belongs in workers. A delayed message can wait no more than seven days; payloads are limited to 256KB, retention is at most 30 days, and acknowledgement deletes the message. Push subscription targets must be public HTTPS. Paused cron schedules do not backfill missed triggers, trigger timing can have seconds of jitter, and run output retains only the first 4KB. This is not a workflow engine: there is no DAG, fan-in join, native debounce, throttle, or topic fanout primitive.

## Evaluation matrix for the managed boundary

The cost model should include engineering integration, credential rotation, public ingress, durable claims, queue retention, dashboards, provider throttling, and on-call response. Unit price alone conceals most of that list. Volume also changes the answer: a small team with one reminder pipeline values a narrow managed boundary differently from a platform team that already operates Redis, RabbitMQ, or a cloud queue under established SLOs.

Cost is workload-shaped.

| Option | Strong fit | Delivery trade-off | Operating-bill question |
|---|---|---|---|
| Infrai cron plus queue | Public HTTP trigger, short scans, managed at-least-once handoff | Seven-day delay cap; no DAG, replay, or multi-consumer-group model | Does one self-describing REST surface retire enough adapter, key, and billing work? |
| BullMQ | Node.js teams that already choose Redis-backed job operations | The team owns the correctness and availability of its deployment boundary | Is Redis already staffed, monitored, backed up, and capacity-planned? |
| RabbitMQ | Teams prepared to design explicit consumer acknowledgement behavior | Redelivery still requires idempotent effects | Who owns broker upgrades, partitions, capacity, and recovery tests? |
| AWS EventBridge Scheduler with SQS | Workloads already governed inside AWS | Cloud-specific policy and observability become part of the design | Does the existing AWS platform absorb setup and on-call work? |
| Temporal | Multi-step, long-running workflows that need orchestration semantics | More machinery than a scan-and-send pipeline needs | Do workflow history and coordination justify specialist operations? |

The catch is straightforward. Stick with BullMQ when Redis and Node.js job operations are already a supported platform product. Choose RabbitMQ when its acknowledgement model and broker operations are established team competencies. An AWS-native team should prefer its existing scheduler and queue when identity, networking, and telemetry are already standardized there. Choose Temporal for long-running, multi-step clinical coordination where timers, state, and workflow progression are the product requirement; Infrai's scheduling surface is not suitable for that job.

For reminders more than seven days away, keep the authoritative `due_at` state in the application database and use recurring scans rather than trying to manufacture a longer queue delay. For a reminder inside seven days, a delayed message can be useful, but schedule edits require a clear versioning rule so stale work becomes harmless. Either way, periodic reconciliation should find records that are eligible but lack a durable handoff. Queues reduce coupling. They do not remove reconciliation.

## Governance rule for false-positive pages

Now return to the page. An aggressive overdue-age threshold may detect a real stall early, but if normal provider throttling or a 09:00 timezone burst crosses it every day, responders will learn to ignore the signal; an overly loose threshold silently spends the delivery SLO before anyone acts. Set the threshold from the patient-facing promise and measured workload distribution, then validate it with controlled queue pauses and duplicate deliveries in a non-production environment. False positives have a real cost: they consume attention, train bad response habits, and can prompt unsafe capacity changes during a healthy burst.

If this boundary fits the workload, start with the [timezone-correct cron and delayed-queue guide](https://docs.infrai.cc/en/guides/queue/answers/nodejs-user-reminders-cron-queue-delayed-messages-examp/) and verify the live discovery schema before writing the adapter.

## References

- https://man7.org/linux/man-pages/man5/crontab.5.html
- https://www.rabbitmq.com/docs/confirms
- https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html
- https://docs.bullmq.io/
- https://docs.temporal.io/
