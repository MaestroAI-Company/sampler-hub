## Safer MCP servers

The hub checks the shared folders of an MCP server again every time it starts, so a folder replaced by a link can no longer reach outside it.

On a hub where the system has no sandbox (Linux without Landlock, macOS without `sandbox-exec`), member servers no longer start. Admin servers still run. To allow member servers anyway, add `unsandboxed_member_mcp = true` to `config.toml`, knowing they then reach every file the hub can read.

## Removed devices are cut off

A removed device, profile or role change now closes the connections a device already had open, within a few seconds.

## Ollama models stay with admins

Members using the hub's Ollama can chat with its models but can no longer pull, delete or create them. Provider requests must stay on the provider's own paths.

## Updates restart on Linux

On Linux, a hub updated from Opera kept running its old version until the service was restarted. It now restarts on the new one. Hubs older than this release still have the issue for this update: if Opera still shows the old version afterwards, restart the hub once with `systemctl --user restart maestroai-sampler-hub`.

## Other fixes

- Password tries are counted together, even when many arrive at once.
- A message only takes memory as its data arrives.
- Updates only accept a plain version number.
- An interrupted llama.cpp install is never started.
- Opera refuses password settings a hub could use to freeze it. Update Opera to get this part.
