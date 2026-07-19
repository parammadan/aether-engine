//! S3 snapshot tier: snapshots are uploaded after they are built and fetched at cold boot
//! when no local state exists.
//!
//! # The media split, on purpose
//! S3 has no fsync semantics — an object either exists whole or not at all — which makes
//! it exactly WRONG for a write-ahead log (where the unit of durability is an appended,
//! synced record) and exactly RIGHT for snapshots (immutable, atomic, versioned recovery
//! points). Local disk owns the log; S3 owns recovery points. A node that loses its whole
//! disk cold-boots from the newest object and rejoins.
//!
//! Configured by env: `AETHER_S3_BUCKET` (required to enable), `AETHER_S3_PREFIX`
//! (namespacing, e.g. per shard group), `AETHER_S3_ENDPOINT` (optional — points at any
//! S3-compatible store such as MinIO, which is how CI exercises this path on every push).
//! Credentials/region come from the standard AWS environment.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

pub struct SnapshotS3 {
    client: Client,
    bucket: String,
    prefix: String,
}

impl SnapshotS3 {
    /// Build from env; `None` when `AETHER_S3_BUCKET` is unset (the tier is opt-in).
    /// Creates the bucket if it doesn't exist (idempotent; matters for MinIO in CI).
    pub async fn from_env() -> Option<Self> {
        let bucket = std::env::var("AETHER_S3_BUCKET").ok()?;
        let prefix = std::env::var("AETHER_S3_PREFIX").unwrap_or_else(|_| "snapshots".to_string());
        let endpoint = std::env::var("AETHER_S3_ENDPOINT").ok();

        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region));
        if let Some(ep) = &endpoint {
            loader = loader.endpoint_url(ep);
        }
        let shared = loader.load().await;
        let mut builder = aws_sdk_s3::config::Builder::from(&shared);
        if endpoint.is_some() {
            // MinIO and friends serve buckets by path, not virtual host.
            builder = builder.force_path_style(true);
        }
        let client = Client::from_conf(builder.build());

        // Idempotent bucket creation; "already exists/owned" is success.
        if let Err(e) = client.create_bucket().bucket(&bucket).send().await {
            let msg = format!("{e:?}");
            if !msg.contains("BucketAlreadyOwnedByYou") && !msg.contains("BucketAlreadyExists") {
                eprintln!("s3: bucket check: {msg}");
            }
        }

        println!("s3 snapshots: bucket={bucket} prefix={prefix} endpoint={endpoint:?}");
        Some(Self { client, bucket, prefix })
    }

    fn key(&self, name: &str) -> String {
        format!("{}/{}", self.prefix, name)
    }

    /// Upload one snapshot object. Best-effort by design: a failed upload is logged, never
    /// fatal — the local tier already holds the snapshot, and the next one retries.
    pub async fn upload(&self, name: &str, bytes: Vec<u8>) {
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.key(name))
            .body(ByteStream::from(bytes))
            .send()
            .await;
        match result {
            Ok(_) => println!("s3: uploaded snapshot {name}"),
            Err(e) => eprintln!("s3: snapshot upload failed (local copy kept): {e}"),
        }
    }

    /// Newest snapshot object under the prefix, if any. Snapshot names embed a
    /// zero-padded log index, so lexicographic max == numeric max.
    pub async fn latest(&self) -> Option<Vec<u8>> {
        let listing = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(format!("{}/", self.prefix))
            .send()
            .await
            .ok()?;
        let newest = listing
            .contents()
            .iter()
            .filter_map(|o| o.key().map(str::to_string))
            .max()?;
        let obj = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&newest)
            .send()
            .await
            .ok()?;
        let bytes = obj.body.collect().await.ok()?.into_bytes().to_vec();
        println!("s3: fetched snapshot {newest} ({} bytes)", bytes.len());
        Some(bytes)
    }
}
