## Use your own relay

Sampler Hub can use your own [iroh relay](https://github.com/n0-computer/iroh/tree/main/iroh-relay) instead of the public ones run by n0. Add it to `config.toml` in the hub data directory and restart the hub:

```toml
relays = ["https://relay.example.com/"]
```

- Devices paired afterwards get the relay in the pairing link.
- Devices already paired keep finding the hub and switch to the new relay on their own, no need to pair again.
- The next Opera release shows the relay in the Sampler Hub settings.

The [custom relay guide](https://maestroai.company/docs/advanced/custom-relay/) explains how to host one.

## Shorter pairing links

Pairing links and QR codes are shorter, and the relay address no longer shows in clear.

The next Opera release is needed to scan the new QR codes. Until then, pair with the code shown under the QR code.

## Fixes

- The Linux data directory is `~/.local/share/sampler-hub`, the docs said `samplerhub`.
