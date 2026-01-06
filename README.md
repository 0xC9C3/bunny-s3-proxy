# bunny-s3-proxy

S3-compatible proxy for [Bunny.net](https://bunny.net) storage.

## Docker

```bash
docker pull ghcr.io/0xc9c3/bunny-s3-proxy:latest

docker run -p 9000:9000 \
  -e BUNNY_STORAGE_ZONE=myzone \
  -e BUNNY_ACCESS_KEY=your-key \
  ghcr.io/0xc9c3/bunny-s3-proxy:latest -l 0.0.0.0:9000
```

Multi-arch images available for `linux/amd64` and `linux/arm64` (~9MB).

## Usage

```bash
# Via environment variables
export BUNNY_STORAGE_ZONE=myzone
export BUNNY_ACCESS_KEY=your-key
bunny-s3-proxy

# Via CLI arguments
bunny-s3-proxy -z myzone -k your-key -r de -l 0.0.0.0:9000

# Unix socket
bunny-s3-proxy -z myzone -k your-key -s /tmp/s3.sock
```

Then use any S3 client:

```bash
aws --endpoint-url http://127.0.0.1:9000 s3 ls
aws --endpoint-url http://127.0.0.1:9000 s3 cp file.txt s3://myzone/file.txt
```

## Options

| Flag | Env | Description |
|------|-----|-------------|
| `-z, --storage-zone` | `BUNNY_STORAGE_ZONE` | Bunny storage zone name |
| `-k, --access-key` | `BUNNY_ACCESS_KEY` | Bunny storage access key |
| `-r, --region` | `BUNNY_REGION` | Region: `de` (default), `uk`, `ny`, `la`, `sg`, `se`, `br`, `jh`, `syd` |
| `-l, --listen-addr` | `LISTEN_ADDR` | Listen address (default: `127.0.0.1:9000`) |
| `-s, --socket-path` | `SOCKET_PATH` | Unix socket path (alternative to TCP) |
| `--s3-access-key-id` | `S3_ACCESS_KEY_ID` | S3 auth access key (default: `bunny`) |
| `--s3-secret-access-key` | `S3_SECRET_ACCESS_KEY` | S3 auth secret key (default: `bunny`) |
| `-v, --verbose` | `VERBOSE` | Enable debug logging |

## Supported S3 Operations

- ListBuckets, HeadBucket
- ListObjectsV2 (with prefix/delimiter)
- GetObject (with Range requests), HeadObject, PutObject, DeleteObject
- CopyObject, DeleteObjects (batch)
- Multipart uploads (CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts)

## Multipart Uploads

Since Bunny doesn't support native multipart uploads, parts are stored as temporary files on Bunny:

1. `CreateMultipartUpload` → Creates `__multipart/{upload_id}/_meta`
2. `UploadPart` → Stores part at `__multipart/{upload_id}/{part_number}`
3. `CompleteMultipartUpload` → Streams parts from Bunny, uploads concatenated file, deletes temp parts
4. `AbortMultipartUpload` → Deletes all temp parts

This keeps the proxy stateless and horizontally scalable. Trade-off: complete uses double bandwidth (download + re-upload).

## Limitations

- Single storage zone per instance (bucket = storage zone)
- CreateBucket/DeleteBucket are no-ops (manage zones via Bunny dashboard)

## Building

```bash
cargo build --release
```

## License

AGPL-3.0
