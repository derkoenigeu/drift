# Drift — MySQL & MariaDB Schema and Data Sync

> **See the diff. Fix the drift.**

[![Release](https://img.shields.io/github/v/release/your-org/drift?label=latest)](../../releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-macOS%20%7C%20Windows%20%7C%20Linux-lightgrey)](../../releases)

Drift is an open-source desktop app and CLI for comparing and synchronizing MySQL and MariaDB databases. It detects **schema drift** (missing tables, columns, indexes, views, routines, triggers) and **data drift** (changed, added, or deleted rows) between any two database servers — and applies the fix with a single click.

Connect via direct TCP, SSH tunnel, Kubernetes port-forward, or Docker — no VPN, no agent, no cloud account required.

---

## Features

| | |
|---|---|
| **Schema sync** | Tables, columns, indexes, views, stored procedures, functions, triggers |
| **Data sync** | Row-level diff via primary keys — generates precise INSERT / UPDATE / DELETE |
| **Connection types** | Direct TCP · SSH tunnel · Kubernetes port-forward · Docker container |
| **Encrypted vault** | AES-256-GCM + scrypt credential store — no plaintext passwords on disk |
| **Table filters** | Include / exclude tables by glob; ignore specific columns in data diff |
| **Dry-run mode** | Preview every SQL statement before touching the target |
| **Cross-version** | MySQL 8 ↔ MariaDB 11 — normalizes type and default differences automatically |
| **Desktop GUI** | Electron + Vue 3 · macOS, Windows, Linux |
| **CLI** | Full feature parity via `db-mirror` command |
| **SQL dump** | Export any database as a `.sql` file |

---

## Download

Get the latest installer from the [Releases](../../releases) page:

| Platform | Installer |
|----------|-----------|
| **macOS** | `DB-Mirror-x.x.x.dmg` |
| **Windows** | `DB-Mirror Setup x.x.x.exe` |
| **Linux** | `DB-Mirror-x.x.x.AppImage` · `DB-Mirror_x.x.x_amd64.deb` |

---

## How it works

1. **Add connections** — save source and target database credentials to the encrypted vault
2. **Diff** — Drift introspects both servers and computes the exact SQL needed to bring target in line with source
3. **Review** — inspect every schema and data statement before applying
4. **Apply** — execute the plan; errors are reported per-statement with `continueOnError` support

The diff is fully serializable — you can save a plan as JSON, review it, and apply it later or on a different machine.

---

## Connection types

### Direct TCP
Standard host / port / user / password. Optionally with TLS.

### SSH tunnel
Drift opens a local port-forward through an SSH jump host automatically. No VPN needed.

```
SSH host:  bastion.example.com
SSH user:  deploy
SSH key:   ~/.ssh/id_ed25519
DB host:   10.0.0.5   (reachable from bastion)
DB port:   3306
```

### Kubernetes
Drift calls `kubectl port-forward` internally. Credentials can be resolved from a Kubernetes Secret automatically — no copy-paste from `kubectl get secret`.

```
Context:    prod-cluster
Namespace:  default
Target:     service/mysql   or   pod/mysql-0
Port:       3306
User from:  secret/mysql-secret → MYSQL_USER
Pass from:  secret/mysql-secret → MYSQL_PASSWORD
```

### Docker
Connects to a running MySQL / MariaDB container on the local Docker daemon — no published port needed.

---

## CLI quick start

```bash
# Prerequisites: Node.js ≥ 20, pnpm
git clone https://github.com/your-org/drift.git
cd drift && pnpm install && pnpm -r build

# Create a vault and add two profiles
node packages/cli/bin/db-mirror.js vault init
node packages/cli/bin/db-mirror.js vault add-profile \
  --name prod --host db.prod.example.com --user app --password '***' --database myapp
node packages/cli/bin/db-mirror.js vault add-profile \
  --name staging --host 127.0.0.1 --port 3307 --user root --password '***' --database myapp

# Preview schema + data diff
node packages/cli/bin/db-mirror.js diff prod staging --schema --data --dry-run

# Apply
node packages/cli/bin/db-mirror.js mirror prod staging --schema --data
```

Vault passphrase is read from the `DB_MIRROR_PASSPHRASE` environment variable, `--passphrase-file`, or an interactive prompt.

---

## Desktop app (development)

```bash
pnpm install
pnpm --filter @db-mirror/desktop dev      # Vite dev server + Electron
pnpm --filter @db-mirror/desktop build    # production build
pnpm --filter @db-mirror/desktop package  # build installer
```

---

## Architecture

```
drift/
├── packages/
│   ├── core/            Pure ESM library — no Electron dependency
│   │   ├── schema-diff/ introspect + structural diff (tables/views/routines/triggers)
│   │   ├── data-diff/   row-level diff via primary key hashing
│   │   ├── sync-plan/   serialisable SyncPlan (schema + data statements)
│   │   ├── vault/       AES-256-GCM encrypted credential store
│   │   ├── connection/  mysql2 pool factory — direct / SSH / K8s / Docker
│   │   ├── dump/        mysqldump subprocess wrapper
│   │   ├── filters/     glob include/exclude via micromatch
│   │   ├── k8s/         @kubernetes/client-node port-forward
│   │   └── ssh/         ssh2 tunnel
│   ├── cli/             Commander-based CLI (db-mirror)
│   └── desktop/         Electron main + Vue 3 renderer
```

---

## Supported databases

- **MySQL** 5.7, 8.0, 8.4
- **MariaDB** 10.6, 10.11, 11.x

Cross-version sync (MySQL 8 → MariaDB 11 and vice versa) is fully supported. Drift normalizes integer display widths, `json` / `longtext` type aliases, NULL default encoding, and string default quoting differences automatically.

---

## Security

- Credentials are stored in an AES-256-GCM encrypted vault (`~/.config/db-mirror/vault.db-mirror`)
- The vault key is derived from your passphrase using `scrypt` — never stored anywhere
- SSH connections use your existing keys; passwords are optional
- Kubernetes port-forwards bind to `127.0.0.1` with a random local port and are torn down after use
- Destructive applies require typing the target profile name as confirmation

---

## Releasing

Push a semver tag — GitHub Actions builds macOS, Windows, and Linux installers and publishes a release automatically:

```bash
git tag v1.2.3
git push origin v1.2.3
```

---

## License

MIT © zebra group GmbH

---

*Keywords: mysql sync tool · mariadb sync · database schema diff · database migration tool · schema comparison · data synchronization · mysql gui · mariadb gui · kubernetes database sync · ssh tunnel mysql · docker database sync · electron database app · open source database tool · mysql to mariadb migration*
