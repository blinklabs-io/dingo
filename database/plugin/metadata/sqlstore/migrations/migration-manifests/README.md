Each SQL migration added after `active-payload-indexes` adds one JSON manifest
named with its stable version. The manifest contains `version`, `name`, and
`after`; its SQL lives under the matching `v<version>/<dialect>/` directory.
`active-payload-indexes` itself is listed in `schemaVersions` and keeps its SQL
under `v76/`.
