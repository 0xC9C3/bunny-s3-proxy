# Changelog

## [0.3.18](https://github.com/0xC9C3/bunny-s3-proxy/compare/bunny-s3-proxy-v0.3.17...bunny-s3-proxy-v0.3.18) (2026-01-10)


### Features

* add centralized log-level control ([28a95fa](https://github.com/0xC9C3/bunny-s3-proxy/commit/28a95fabafad353ed246f87034999d030bcfc645))
* add CI checks before release ([f835f89](https://github.com/0xC9C3/bunny-s3-proxy/commit/f835f89bbf09edc5d71ea7c0be4df6b979fce80b))
* add connect timeout for http client ([cdcc2d3](https://github.com/0xC9C3/bunny-s3-proxy/commit/cdcc2d33460f352f0dbe2ae65cefa4b80b9f4a33))
* add release-please for semantic releases ([3c46b22](https://github.com/0xC9C3/bunny-s3-proxy/commit/3c46b22184f04c7ce4f661f8a77605400cd36da8))
* stream all uploads including multipart parts ([4bb678b](https://github.com/0xC9C3/bunny-s3-proxy/commit/4bb678b2c835390aadc3996b86b6e6ddff999744))
* stream large uploads without body size limit ([27f5cf3](https://github.com/0xC9C3/bunny-s3-proxy/commit/27f5cf3a172390fdc636393efa0c273933661df1))
* streaming multipart complete with memory efficiency ([91c4c17](https://github.com/0xC9C3/bunny-s3-proxy/commit/91c4c17f53d493f7126b70cfbb82313ac2c62c02))
* streaming uploads with post-upload hash verification ([0685cc0](https://github.com/0xC9C3/bunny-s3-proxy/commit/0685cc03ae0e4d4714f04c2f28daee99cccd4243))


### Bug Fixes

* combine release and build workflows to fix trigger issue ([9342ee4](https://github.com/0xC9C3/bunny-s3-proxy/commit/9342ee4a723976a3b58fe70c1a28d7ab8b98ecba))
* detect HTTP/2 preface and apply flow control limits ([4185123](https://github.com/0xC9C3/bunny-s3-proxy/commit/4185123f3ce55670c0f24184ce03923a38adeaed))
* disable HTTP/2 adaptive window to prevent large window updates ([4a33870](https://github.com/0xC9C3/bunny-s3-proxy/commit/4a33870c314f052e2ba2c72976abbf85bc947bfc))
* extract version correctly from release tag ([17ab1e3](https://github.com/0xC9C3/bunny-s3-proxy/commit/17ab1e3d383b859fb3961ddf16a1db74e166728d))
* limit HTTP/2 flow control windows on reqwest client to Bunny ([75bb168](https://github.com/0xC9C3/bunny-s3-proxy/commit/75bb1682e315cb98f2bcbed7ef58ca8bcc639a3a))
* limit HTTP/2 flow control windows to prevent OOM ([60f6f4e](https://github.com/0xC9C3/bunny-s3-proxy/commit/60f6f4e046041ed94d2b06f01615f5e04d06ca6f))
* store ETags in metadata files to avoid re-downloading parts ([b51df8a](https://github.com/0xC9C3/bunny-s3-proxy/commit/b51df8a683bb783476f1509cde118c2d8dc14766))
* stream all PUT requests regardless of content hash header ([f1bedf0](https://github.com/0xC9C3/bunny-s3-proxy/commit/f1bedf0be268673079735ad7677a3395d33baf00))
* use fresh client per upload to prevent memory accumulation ([d499fcc](https://github.com/0xC9C3/bunny-s3-proxy/commit/d499fccdb4c5982a02174fcf45cd6671729acdc7))

## [0.3.17](https://github.com/0xC9C3/bunny-s3-proxy/compare/bunny-s3-proxy-v0.3.16...bunny-s3-proxy-v0.3.17) (2026-01-10)


### Features

* streaming multipart complete with memory efficiency ([91c4c17](https://github.com/0xC9C3/bunny-s3-proxy/commit/91c4c17f53d493f7126b70cfbb82313ac2c62c02))


### Bug Fixes

* use fresh client per upload to prevent memory accumulation ([d499fcc](https://github.com/0xC9C3/bunny-s3-proxy/commit/d499fccdb4c5982a02174fcf45cd6671729acdc7))

## [0.3.16](https://github.com/0xC9C3/bunny-s3-proxy/compare/bunny-s3-proxy-v0.3.15...bunny-s3-proxy-v0.3.16) (2026-01-10)


### Bug Fixes

* limit HTTP/2 flow control windows on reqwest client to Bunny ([75bb168](https://github.com/0xC9C3/bunny-s3-proxy/commit/75bb1682e315cb98f2bcbed7ef58ca8bcc639a3a))

## [0.3.15](https://github.com/0xC9C3/bunny-s3-proxy/compare/bunny-s3-proxy-v0.3.14...bunny-s3-proxy-v0.3.15) (2026-01-10)


### Bug Fixes

* detect HTTP/2 preface and apply flow control limits ([4185123](https://github.com/0xC9C3/bunny-s3-proxy/commit/4185123f3ce55670c0f24184ce03923a38adeaed))

## [0.3.14](https://github.com/0xC9C3/bunny-s3-proxy/compare/bunny-s3-proxy-v0.3.13...bunny-s3-proxy-v0.3.14) (2026-01-10)


### Bug Fixes

* disable HTTP/2 adaptive window to prevent large window updates ([4a33870](https://github.com/0xC9C3/bunny-s3-proxy/commit/4a33870c314f052e2ba2c72976abbf85bc947bfc))

## [0.3.13](https://github.com/0xC9C3/bunny-s3-proxy/compare/bunny-s3-proxy-v0.3.12...bunny-s3-proxy-v0.3.13) (2026-01-10)


### Bug Fixes

* limit HTTP/2 flow control windows to prevent OOM ([60f6f4e](https://github.com/0xC9C3/bunny-s3-proxy/commit/60f6f4e046041ed94d2b06f01615f5e04d06ca6f))

## [0.3.12](https://github.com/0xC9C3/bunny-s3-proxy/compare/bunny-s3-proxy-v0.3.11...bunny-s3-proxy-v0.3.12) (2026-01-10)


### Bug Fixes

* store ETags in metadata files to avoid re-downloading parts ([b51df8a](https://github.com/0xC9C3/bunny-s3-proxy/commit/b51df8a683bb783476f1509cde118c2d8dc14766))

## [0.3.11](https://github.com/0xC9C3/bunny-s3-proxy/compare/bunny-s3-proxy-v0.3.10...bunny-s3-proxy-v0.3.11) (2026-01-10)


### Features

* stream all uploads including multipart parts ([4bb678b](https://github.com/0xC9C3/bunny-s3-proxy/commit/4bb678b2c835390aadc3996b86b6e6ddff999744))

## [0.3.10](https://github.com/0xC9C3/bunny-s3-proxy/compare/bunny-s3-proxy-v0.3.9...bunny-s3-proxy-v0.3.10) (2026-01-10)


### Bug Fixes

* stream all PUT requests regardless of content hash header ([f1bedf0](https://github.com/0xC9C3/bunny-s3-proxy/commit/f1bedf0be268673079735ad7677a3395d33baf00))

## [0.3.9](https://github.com/0xC9C3/bunny-s3-proxy/compare/bunny-s3-proxy-v0.3.8...bunny-s3-proxy-v0.3.9) (2026-01-10)


### Features

* add connect timeout for http client ([cdcc2d3](https://github.com/0xC9C3/bunny-s3-proxy/commit/cdcc2d33460f352f0dbe2ae65cefa4b80b9f4a33))
* streaming uploads with post-upload hash verification ([0685cc0](https://github.com/0xC9C3/bunny-s3-proxy/commit/0685cc03ae0e4d4714f04c2f28daee99cccd4243))

## [0.3.8](https://github.com/0xC9C3/bunny-s3-proxy/compare/bunny-s3-proxy-v0.3.7...bunny-s3-proxy-v0.3.8) (2026-01-10)


### Features

* stream large uploads without body size limit ([27f5cf3](https://github.com/0xC9C3/bunny-s3-proxy/commit/27f5cf3a172390fdc636393efa0c273933661df1))

## [0.3.7](https://github.com/0xC9C3/bunny-s3-proxy/compare/bunny-s3-proxy-v0.3.6...bunny-s3-proxy-v0.3.7) (2026-01-09)


### Features

* add centralized log-level control ([28a95fa](https://github.com/0xC9C3/bunny-s3-proxy/commit/28a95fabafad353ed246f87034999d030bcfc645))

## [0.3.6](https://github.com/0xC9C3/bunny-s3-proxy/compare/bunny-s3-proxy-v0.3.5...bunny-s3-proxy-v0.3.6) (2026-01-08)


### Features

* add CI checks before release ([f835f89](https://github.com/0xC9C3/bunny-s3-proxy/commit/f835f89bbf09edc5d71ea7c0be4df6b979fce80b))


### Bug Fixes

* extract version correctly from release tag ([17ab1e3](https://github.com/0xC9C3/bunny-s3-proxy/commit/17ab1e3d383b859fb3961ddf16a1db74e166728d))

## [0.3.5](https://github.com/0xC9C3/bunny-s3-proxy/compare/bunny-s3-proxy-v0.3.4...bunny-s3-proxy-v0.3.5) (2026-01-08)


### Bug Fixes

* combine release and build workflows to fix trigger issue ([9342ee4](https://github.com/0xC9C3/bunny-s3-proxy/commit/9342ee4a723976a3b58fe70c1a28d7ab8b98ecba))

## [0.3.4](https://github.com/0xC9C3/bunny-s3-proxy/compare/bunny-s3-proxy-v0.3.3...bunny-s3-proxy-v0.3.4) (2026-01-08)


### Features

* add release-please for semantic releases ([3c46b22](https://github.com/0xC9C3/bunny-s3-proxy/commit/3c46b22184f04c7ce4f661f8a77605400cd36da8))
