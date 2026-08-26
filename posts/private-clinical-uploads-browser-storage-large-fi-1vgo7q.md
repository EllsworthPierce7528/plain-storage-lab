# Private Clinical Uploads: Browser Storage, Large Files, and Expiring Links

For private clinical uploads, choose the browser upload method from the deletion contract first: use a single presigned PUT for small, restartable objects, and use multipart when a large file must survive interrupted transfers without restarting from byte zero. The storage protocol is only one part of the decision. Retention, download expiry, verification, and orphan cleanup determine whether the design is safe to operate.

This is a runbook, not a feature checklist. A patient document that has passed an upload progress bar is still untrusted until the server verifies its object metadata and records the retention deadline.

## Start with the retention boundary

Create an application record before issuing any upload authorization. It should contain an opaque object key, the owning user or case identifier, the expected size, the allowed content type, an upload state, and two different times: `retention_until` for deletion and `download_expires_at` for a particular link. Do not derive either deadline from a browser clock.

The browser receives a short-lived upload authorization, sends bytes directly to object storage, and calls the application to finalize. The application then checks the stored size and any checksum the storage contract supports. Only after that check should the object become available through a download-link endpoint. The endpoint can issue a new short-lived link after checking authorization; it should not expose a permanent public URL.

Keep it explicit.

A deletion worker owns the other half of the contract. It queries records whose `retention_until` has passed, deletes the object, and marks the record deleted only after the storage operation succeeds. A retry must address the same object key, and a missing object should be treated as an already-satisfied deletion rather than a reason to create a new record. This is where the retention SLO belongs: measure the delay between the deadline and confirmed deletion, not merely the number of worker runs.

## What should a browser storage upload do for large private files?

Choose a single PUT when retrying the whole object is acceptable and the browser can finish within the signed request's lifetime. Choose multipart when the file is large, the network is likely to drop, or resuming matters more than client simplicity. Multipart stores more state: an upload identifier, part numbers, and returned part identifiers. That state must be durable enough to survive a tab refresh, and abandoned uploads need an explicit abort policy.

Presigned POST is useful when the browser should submit a form-like multipart request with a policy that constrains fields such as key, size, or content type. Presigned PUT is a simpler byte-stream path. Neither one proves that a clinical object is acceptable; authorization and post-upload validation still belong to the application. Multipart changes the retry unit, not the trust model.

The application server should sign and finalize control-plane requests, not proxy every byte. Capacity planning should therefore cover signing bursts, finalize calls, metadata checks, deletion throughput, and the database queue. Track the byte path separately. A useful SLO set includes upload-finalization success, time to first usable download link, and deletion completion latency; request volume alone will miss the failure that matters.

For a minimal state transition, make terminal states hard to re-open. The handler below is deliberately provider-neutral: the storage adapter supplies `Delete`, while the database supplies an atomic claim and state update.

```go
package retention

import "context"

type ObjectStore interface {
	Delete(ctx context.Context, objectKey string) error
}

type Record struct {
	ObjectKey string
	State     string
}

func DeleteExpired(ctx context.Context, store ObjectStore, record Record) error {
	if record.State == "deleted" {
		return nil
	}
	if err := store.Delete(ctx, record.ObjectKey); err != nil {
		return err
	}
	// The caller must persist this transition with an atomic compare on State.
	record.State = "deleted"
	return nil
}
```

In production, the comment is a contract: the worker must not mark the record deleted after an arbitrary retry has changed its key or owner. Claim the record with a compare-and-set, perform deletion, then commit the terminal state. If the process stops between those steps, the next run repeats deletion against the same key. That is less clever and much easier to audit.

## Where do POST, PUT, and multipart fail differently?

The failure mode should decide the method.

| Method | Good fit | Cost to carry | Recovery check |
| --- | --- | --- | --- |
| Presigned POST | Constrained browser form submission | Policy fields and form construction add client detail | Confirm the object key and stored size after submission |
| Presigned PUT | Small files and dependable connections | A failed transfer usually means retrying the whole object | Reconcile an ambiguous completion before retrying |
| Multipart | Large files or interrupted connections | More state, completion bookkeeping, and orphan cleanup | Retry parts, then verify the assembled object |

The catch is that multipart is not automatically better for a healthtech workflow. It increases the number of control-plane events and creates more ways for a tab to disappear mid-operation. It is not suitable when your team cannot own abandoned-upload cleanup or durable client state; use a single PUT for smaller objects, or choose a managed ingestion component whose lifecycle behavior you can verify. Conversely, a single PUT is a poor fit when restarting a multi-gigabyte clinical recording would consume the user's connection and your upload SLO.

Test the ambiguous cases, not just the happy path: the browser closes after a part upload, finalization times out after storage accepted it, the link expires while a download is in progress, and deletion is retried after a worker restart. A `200` from an authorization endpoint is not proof that the object exists, and a successful delete request is not proof that your application record is consistent until the state transition is durable.

## Buy versus build for the deletion runbook

The meaningful comparison is operational ownership. A native object-storage integration may expose more lifecycle controls, while a self-hosted service can offer placement control at the cost of durability, upgrades, replication, and on-call responsibility. A managed abstraction can reduce integration surface, but its retention semantics, conditional writes, event delivery, and browser CORS behavior still need explicit verification.

| Approach | What it buys | What the platform team still owns |
| --- | --- | --- |
| Native storage integration | Direct access to provider lifecycle and multipart primitives | Credential policy, signer compatibility, cleanup jobs, and portability risk |
| Managed storage abstraction | A smaller client contract and less provider-specific code | Capability boundaries, deletion evidence, outages in the dependency, and exit planning |
| Self-hosted object storage | Control over placement and deployment | Replication, durability SLOs, upgrades, capacity, and incident response |

For private uploads, select the approach that can produce an auditable answer to three questions: which object key belongs to this user, when may it be deleted, and how do we prove deletion completed? Price can influence the decision, but it should not outrank those controls or the on-call load they create.

## Rollback and verification

Keep signer changes backward-compatible with already-issued authorizations, since a deploy can overlap with active browser transfers. On finalization failure, query metadata before creating another object. On validation failure, quarantine or delete according to the documented policy, and never issue a download link from an unverified record. On retention-worker failure, leave the record retryable and alert on deadline age.

I have not found a universal part size that works across every browser, region, and clinical file mix; your mileage may vary. Start with a measured default, cap browser concurrency, and revisit it using part retry rate, finalize latency, abandoned-upload age, and deletion SLOs.

The decision rule is short: POST when policy-constrained form upload is the useful primitive, PUT when restarting is cheap, and multipart when recovery from interruption is worth the state machine. In every case, expiring links are presentation policy; retention and deletion are the system of record.

## References

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition
- https://www.backblaze.com/cloud-storage/pricing
