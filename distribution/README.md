# Distribution

Texts published with each release. Fill them in before you ship.

| File | Where it shows | Limit |
|---|---|---|
| `release-notes.md` | the GitHub release (versioned and beta) | none |

Sampler-Hub ships on GitHub only: hub binaries and installers, the install script, and the prebuilt clients Opera pulls for Android, iOS and the web.

## release-notes.md

Markdown. `release.yml` publishes it as the notes of the tag release and writes it again on every run, so the file is the source of truth: an edit made on GitHub is overwritten by the next run of that tag.

Write it for hub owners, not as a commit list. Say when a change needs Opera to be updated too.

## Production release

1. Merge the work into `main`. A release is always tagged on `main`, so the release matches the published code and the workflow is the one `main` carries.
2. On `main`, bump `version` in the three crates: `crates/sampler-hub`, `crates/sampler-hub-client` and `crates/sampler-hub-proto`. Run `cargo check` so `Cargo.lock` follows, the build runs with `--locked`.
3. Write a new `release-notes.md`, then commit and push `main`.
4. Tag the `main` commit and push the tag, it must match the crate version:

   ```bash
   git checkout main
   git pull
   git tag v1.2.3
   git push origin v1.2.3
   ```

5. `release.yml` builds the hub for Linux, macOS and Windows, the Windows installers and the macOS disk image, plus the Android, iOS and web clients. Everything lands on the GitHub release `v1.2.3` with a `SHA256SUMS` file, marked as latest. The run fails before publishing if the tag and the crate version differ.
6. Running hubs see the new version on their own and offer the update to admins. `install.sh` and the update check both follow the latest release.

## Beta release

A beta can come from any branch, no need to merge into `main` first.

1. Set the crate versions to the version the beta leads to, `1.2.3` for the betas of `1.2.3`.
2. Write a new `release-notes.md`, then commit and push the branch.
3. Tag the commit `v<version>-beta.<n>` and push the tag, counting `n` up from 1 for each beta of the same version:

   ```bash
   git tag v1.2.3-beta.1
   git push origin v1.2.3-beta.1
   ```

4. The workflow builds the same files and attaches them to the GitHub release `v1.2.3-beta.1`, marked as a pre-release. It never shows as latest, so running hubs and `install.sh` stay on the stable version. Beta testers download the files from the release page.

## Opera

Opera builds against a pinned hub release, set in `sampler-hub-client.json` at the root of the Opera repository. The desktop build checks out Sampler-Hub at that tag, Android and the web download its prebuilt clients.

After a production release that Opera needs, pin it from the Opera repository:

```bash
node scripts/pin-hub-client.mjs v1.2.3
```

The script only takes stable versions, a beta can't be pinned. Ship the hub before the Opera release that depends on it: a new protocol, like `sampler/login/1`, needs a hub that serves it, and older hubs answer it as outdated.


## After a release: old branches

The tag is what keeps a release, not the branch: once `v1.2.3` is tagged on `main`, the `1.2.3` branch no longer holds anything the release needs, so it can be deleted. Betas work the same way, their tags (`v1.2.3-beta.1`) stay and only the working branch goes.

1. Check that the branch is fully merged into `main`, the command must print nothing. If it lists commits, merge what you want to keep first.
2. Delete the branch on GitHub and locally, then drop the stale remote references:

   ```bash
   git log main..origin/1.2.3
   git push origin --delete 1.2.3
   git branch -d 1.2.3
   git fetch --prune
   ```

Branches that aren't tied to a version (`sampler-hub`, `mcp-sandbox`, ect) stay as they are. If a fix is needed on an old release later, a branch can be recreated from its tag with `git checkout -b 1.2.3-hotfix v1.2.3`.
