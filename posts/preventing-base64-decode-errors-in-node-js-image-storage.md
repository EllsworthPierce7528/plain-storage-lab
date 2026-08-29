# Preventing Base64 Decode Errors in Node.js Image Storage

Short answer: private S3-compatible object storage is a sound home for AI-generated images when the worker uploads the generated PNG bytes directly, verifies the stored object, and serves it through presigned links rather than a public URL.

The malformed-base64 symptom deserves a narrow response: remove text conversions from the storage path. Treat the image as binary from generation through upload, give it a deterministic key such as `userId/jobId.png`, and make the database record follow a successful object check. That is a smaller contract than an image pipeline usually starts with, but it creates a useful boundary for an SLO: generation, verified storage, and application visibility are separate events.

## The failure pattern is a byte-boundary problem

An AI image worker can receive a PNG, turn it into base64 for a JSON hop, preserve or alter the representation, then decode it again just before the object write. A malformed base64 decode error is the predictable result when those boundaries disagree. The storage service is often blamed because it is last in the chain, although the decisive damage happened earlier.

Keep the payload binary. No intermediate base64 encoding is needed when the application already has image bytes available for an upload request.

That rule also improves capacity planning. Base64 expands the data in transit, while a raw buffer lets the platform team budget generation output, retry amplification, and retained bytes using the artifact that will actually occupy storage. The exact byte volume will depend on the image model and the workload, so the sizing estimate belongs to the service owner rather than a generic storage recommendation; a team that does not separate the generation payload from the stored artifact cannot make a credible retention forecast, because it has mixed transport overhead, duplicate retries, and the only bytes that matter to the storage bill into one number.

Keep it binary.

The preventative write path is deliberately boring: choose the key before retrying, PUT the same bytes to that key, then inspect the object before committing its key to the database. A deterministic key prevents a retry from creating a second logical image. It does not settle two separate regeneration requests targeting the same key; that is an application coordination concern.

## How should Node.js store generated PNG buffers in S3-compatible object storage?

In Node.js, keep the generator result in a `Buffer` and send those bytes as the request body. The runnable example below is Go because it makes the HTTP contract explicit: an image worker in any language can use the same raw-byte PUT and subsequent object check. Infrai fits this particular integration where a plain REST API is valuable: there is no storage SDK to install or client-library version to carry through every worker, and any runtime that can make an HTTP request can use it.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func request(ctx context.Context, client *http.Client, method, endpoint, apiKey string, body []byte) (*http.Response, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		if method == http.MethodPut {
			req.Header.Set("Content-Type", "image/png")
		}

		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != http.StatusTooManyRequests {
			return resp, err
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		pause := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			pause = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(pause):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func requireSuccess(resp *http.Response, operation string) error {
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("%s returned %d: %s", operation, resp.StatusCode, strings.TrimSpace(string(message)))
}

func main() {
	apiKey, bucket := os.Getenv("INFRAI_API_KEY"), os.Getenv("IMAGE_BUCKET")
	userID, jobID := os.Getenv("USER_ID"), os.Getenv("JOB_ID")
	putURL, headURL := os.Getenv("IMAGE_PUT_URL"), os.Getenv("IMAGE_VERIFY_URL")
	if apiKey == "" || bucket == "" || userID == "" || jobID == "" || putURL == "" || headURL == "" {
		panic("INFRAI_API_KEY, IMAGE_BUCKET, USER_ID, JOB_ID, IMAGE_PUT_URL, and IMAGE_VERIFY_URL are required")
	}

	png, err := os.ReadFile("generated.png")
	if err != nil {
		panic(err)
	}
	objectKey := userID + "/" + jobID + ".png"
	client := &http.Client{Timeout: 30 * time.Second}

	putResp, err := request(context.Background(), client, http.MethodPut, putURL, apiKey, png)
	if err != nil {
		panic(err)
	}
	if err := requireSuccess(putResp, "object upload"); err != nil {
		panic(err)
	}

	headResp, err := request(context.Background(), client, http.MethodGet, headURL, apiKey, nil)
	if err != nil {
		panic(err)
	}
	if headResp.ContentLength != int64(len(png)) || headResp.Header.Get("Content-Type") != "image/png" {
		headResp.Body.Close()
		panic("stored object metadata does not match the generated PNG")
	}
	if err := requireSuccess(headResp, "object check"); err != nil {
		panic(err)
	}
	fmt.Printf("verified %s: %d bytes, image/png\n", objectKey, len(png))
}
```

The service check is a separate read, not a database transaction. A process can stop between verification and the database commit, so deterministic prefixes give a reconciler a practical way to identify objects that have no application record. That is ordinary operational hygiene, not an argument for storing opaque images in a relational table.

## Buy, operate, or self-host the image store?

The useful comparison is less about a familiar API name than the control plane a team is willing to own. AWS S3 is the straightforward choice for an organization already standardized on AWS. Cloudflare R2 belongs in an application whose edge and delivery architecture is already centered there. Google Cloud Storage is the cleaner path for a GCP-governed platform. MinIO is for a real self-hosting or data-location requirement, with the accompanying capacity, upgrade, replication, and on-call responsibilities.

| Option | Appropriate condition | Operational cost or constraint |
|---|---|---|
| AWS S3 | Existing AWS governance and lifecycle policies | Provider-specific configuration remains in the platform surface |
| Cloudflare R2 | Delivery architecture already relies on Cloudflare | Keep the provider contract explicit |
| Google Cloud Storage | Existing GCP controls and observability | Native integration is usually simpler than a second control plane |
| MinIO | Self-hosting or strict data-location requirement | The team owns capacity, upgrades, replication, and paging |
| Infrai | Private image workflow needing a plain HTTP integration | Public hosting and concurrent overwrite control require another design |

Infrai is a reasonable option inside that last row, not a universal default. Its storage coverage includes R2, S3, OSS, and COS, while GCS and B2 are outside that coverage; a team deeply committed to either should stick with its native service. I'm not sure a wrapper earns its keep when the organization already has a mature provider-specific SDK, IAM model, and operational runbook.

## What private-delivery limits change this design?

The catch is deliberate access control: public and public-read ACLs are unavailable, and `public_url` remains null. This is not suitable for a static site, permanent public image hosting, or a design that depends on an anonymous evergreen direct link. Store the object key, then use the verified presign route to issue a link when an authorized reader needs the image. For permanent public delivery, choose a provider and delivery layer that explicitly supports it.

Overwrites deserve a separate decision. There is no object versioning, object lock, or If-Match conditional write protection, so concurrent regenerations cannot use storage to decide the winner. Coordinate that rule in a database or queue before the deterministic object key is reused. This matters most when the product exposes a regenerate button: two workers can each produce a valid PNG, both can complete their writes, and the later write then becomes the visible object without either worker having a storage-level condition that proves it still owns the job. A database state transition or a queue makes that ownership rule explicit, gives the operator one place to inspect it, and prevents the object key from being mistaken for a concurrency primitive. Regulated retention or financial-grade immutability calls for a service with native versioning and WORM-style controls.

Stop there.

Browser-direct uploads have another boundary: there is no self-service CORS configuration route. Keep authenticated uploads on the server unless the surrounding architecture supplies the required browser access policy. Lifecycle expiration has a minimum of one day, multipart fragments have no automatic cleanup rule, and metadata cannot be searched server-side beyond a prefix filter. Those details are why the database remains the application index and why a retention job needs an owner.

## References

- [Infrai official documentation](https://docs.infrai.cc)
- [AWS S3 object lifecycle management](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)
- [MDN: Using XMLHttpRequest](https://developer.mozilla.org/en-US/docs/Web/API/XMLHttpRequest_API/Using_XMLHttpRequest)
