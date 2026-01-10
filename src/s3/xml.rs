use super::types::{S3Bucket, S3CommonPrefix, S3Object, S3Owner};
use chrono::{DateTime, Utc};

pub struct ListObjectsV2Params<'a> {
    pub bucket: &'a str,
    pub prefix: Option<&'a str>,
    pub delimiter: Option<&'a str>,
    pub max_keys: u32,
    pub objects: &'a [S3Object],
    pub common_prefixes: &'a [S3CommonPrefix],
    pub is_truncated: bool,
    pub next_continuation_token: Option<&'a str>,
    pub key_count: u32,
    pub continuation_token: Option<&'a str>,
    pub start_after: Option<&'a str>,
}

pub fn list_buckets_response(buckets: &[S3Bucket], owner: &S3Owner) -> String {
    let buckets_xml: String = buckets
        .iter()
        .map(|b| {
            format!(
                "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
                esc(&b.name),
                b.creation_date.format("%Y-%m-%dT%H:%M:%S%.3fZ")
            )
        })
        .collect();

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Owner><ID>{}</ID><DisplayName>{}</DisplayName></Owner>
<Buckets>{}</Buckets>
</ListAllMyBucketsResult>"#,
        esc(&owner.id),
        esc(&owner.display_name),
        buckets_xml
    )
}

pub fn list_objects_v2_response(params: ListObjectsV2Params<'_>) -> String {
    let contents: String = params.objects.iter().map(|obj| {
        let owner_xml = obj.owner.as_ref().map(|o| format!("<Owner><ID>{}</ID><DisplayName>{}</DisplayName></Owner>", esc(&o.id), esc(&o.display_name))).unwrap_or_default();
        format!(r#"<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>"{}"</ETag><Size>{}</Size><StorageClass>{}</StorageClass>{}</Contents>"#,
            esc(&obj.key), obj.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"), esc(&obj.etag), obj.size, obj.storage_class, owner_xml)
    }).collect();

    let cp_xml: String = params
        .common_prefixes
        .iter()
        .map(|cp| {
            format!(
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                esc(&cp.prefix)
            )
        })
        .collect();
    let prefix_xml = params
        .prefix
        .map(|p| format!("<Prefix>{}</Prefix>", esc(p)))
        .unwrap_or_default();
    let delim_xml = params
        .delimiter
        .map(|d| format!("<Delimiter>{}</Delimiter>", esc(d)))
        .unwrap_or_default();
    let cont_xml = params
        .continuation_token
        .map(|t| format!("<ContinuationToken>{}</ContinuationToken>", esc(t)))
        .unwrap_or_default();
    let next_xml = params
        .next_continuation_token
        .map(|t| format!("<NextContinuationToken>{}</NextContinuationToken>", esc(t)))
        .unwrap_or_default();
    let start_xml = params
        .start_after
        .map(|s| format!("<StartAfter>{}</StartAfter>", esc(s)))
        .unwrap_or_default();

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>{}</Name>{}{}<MaxKeys>{}</MaxKeys><KeyCount>{}</KeyCount><IsTruncated>{}</IsTruncated>{}{}{}{}{}
</ListBucketResult>"#,
        esc(params.bucket),
        prefix_xml,
        delim_xml,
        params.max_keys,
        params.key_count,
        params.is_truncated,
        cont_xml,
        next_xml,
        start_xml,
        contents,
        cp_xml
    )
}

pub fn copy_object_response(etag: &str, last_modified: DateTime<Utc>) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult><ETag>"{}"</ETag><LastModified>{}</LastModified></CopyObjectResult>"#,
        esc(etag),
        last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ")
    )
}

pub fn delete_objects_response(
    deleted: &[(String, Option<String>)],
    errors: &[(String, String, String)],
    quiet: bool,
) -> String {
    let del_xml: String = if quiet {
        String::new()
    } else {
        deleted
            .iter()
            .map(|(key, ver)| {
                let v = ver
                    .as_ref()
                    .map(|v| format!("<VersionId>{}</VersionId>", esc(v)))
                    .unwrap_or_default();
                format!("<Deleted><Key>{}</Key>{}</Deleted>", esc(key), v)
            })
            .collect()
    };
    let err_xml: String = errors
        .iter()
        .map(|(k, c, m)| {
            format!(
                "<Error><Key>{}</Key><Code>{}</Code><Message>{}</Message></Error>",
                esc(k),
                esc(c),
                esc(m)
            )
        })
        .collect();

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{}{}</DeleteResult>"#,
        del_xml, err_xml
    )
}

pub fn initiate_multipart_upload_response(bucket: &str, key: &str, upload_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId>
</InitiateMultipartUploadResult>"#,
        esc(bucket),
        esc(key),
        esc(upload_id)
    )
}

pub fn list_parts_response(
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: &[(i32, String, i64, DateTime<Utc>)],
    is_truncated: bool,
    next_marker: Option<i32>,
    max_parts: u32,
) -> String {
    let parts_xml: String = parts.iter().map(|(n, e, s, lm)| {
        format!(r#"<Part><PartNumber>{}</PartNumber><ETag>"{}"</ETag><Size>{}</Size><LastModified>{}</LastModified></Part>"#,
            n, esc(e), s, lm.format("%Y-%m-%dT%H:%M:%S%.3fZ"))
    }).collect();
    let next_xml = next_marker
        .map(|n| format!("<NextPartNumberMarker>{}</NextPartNumberMarker>", n))
        .unwrap_or_default();

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId><IsTruncated>{}</IsTruncated><MaxParts>{}</MaxParts>{}{}
</ListPartsResult>"#,
        esc(bucket),
        esc(key),
        esc(upload_id),
        is_truncated,
        max_parts,
        next_xml,
        parts_xml
    )
}

pub fn list_multipart_uploads_response(
    bucket: &str,
    uploads: &[(String, String, DateTime<Utc>)],
    prefix: Option<&str>,
    delimiter: Option<&str>,
    max_uploads: u32,
    is_truncated: bool,
) -> String {
    let uploads_xml: String = uploads.iter().map(|(k, u, i)| {
        format!(r#"<Upload><Key>{}</Key><UploadId>{}</UploadId><Initiated>{}</Initiated><StorageClass>STANDARD</StorageClass></Upload>"#,
            esc(k), esc(u), i.format("%Y-%m-%dT%H:%M:%S%.3fZ"))
    }).collect();
    let prefix_xml = prefix
        .map(|p| format!("<Prefix>{}</Prefix>", esc(p)))
        .unwrap_or_default();
    let delim_xml = delimiter
        .map(|d| format!("<Delimiter>{}</Delimiter>", esc(d)))
        .unwrap_or_default();

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Bucket>{}</Bucket>{}{}<MaxUploads>{}</MaxUploads><IsTruncated>{}</IsTruncated>{}
</ListMultipartUploadsResult>"#,
        esc(bucket),
        prefix_xml,
        delim_xml,
        max_uploads,
        is_truncated,
        uploads_xml
    )
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
