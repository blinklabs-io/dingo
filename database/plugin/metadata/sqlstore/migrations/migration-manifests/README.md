Each post-v75 SQL migration adds one JSON manifest named with its stable
version. The manifest contains `version`, `name`, and `after`; its SQL lives
under the matching `v<version>/<dialect>/` directory.
