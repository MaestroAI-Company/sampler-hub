## Update checks you control

Sampler Hub now has a **Check for updates** setting in Opera, under the Sampler preferences. Turn it off and neither the hub nor llama.cpp looks for new versions on GitHub on its own. llama.cpp keeps the build it has installed, the first build is still downloaded when none is installed.

You can still check by hand:

- **Tray icon**: a new **Check for updates** item.
- **Terminal**: `sampler-hub update` checks the hub, `sampler-hub llama` checks llama.cpp.
- **Opera**: buttons in the Sampler screen to check the hub and to update llama.cpp.

Setting `SAMPLER_HUB_NO_UPDATE_CHECK=1` also turns the automatic checks off, for servers without a settings screen.

Opera 1.0.2 is needed for the new buttons and the setting. Older versions keep working.
