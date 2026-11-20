// ══════════════════════════════════════════════════════════════════════════════
//  deploy-endpoint.ts — COMPILE THIS CRATE'S OWN NATIVE BINARY, PACKAGE IT AS AN
//  ANONYMOUS PAYLOAD, AND REGISTER IT AS THE APP "provider" ON ANY ENDPOINT THAT
//  SPEAKS SANDBOX RUNTIME v2
//
//      yarn workspace @typecrate/provider deploy:endpoint https://<your-endpoint>
//      yarn workspace @typecrate/provider start:modal            # the vault's box (see start-modal.ts)
//      yarn workspace @typecrate/provider deploy:endpoint <url> --dry-run
//      yarn workspace @typecrate/provider deploy:endpoint <url> --teardown
//      yarn workspace @typecrate/provider deploy:endpoint <url> --out Temp/provider.tar.gz
// ══════════════════════════════════════════════════════════════════════════════
//
//  WHY THIS PACKAGE HAS ITS OWN VERB AT ALL. `src/bin/server.rs` produced the first
//  executable this crate ever owned (`[[bin]] typecrate-provider-server`,
//  Cargo.toml), so the provider can be started natively — it no longer has to be
//  compiled into `typecrate-services` to have a process. A cloud container is exactly
//  that shape: ONE executable in ONE stage directory, no Node, no services router, no
//  `/v1/harness`. The generic client under `services/daytona/sandbox/runtime-deploy.ts`
//  is what both this workload and nexus share — this file is a thin WORKLOAD
//  declaration on top of it: it names the crate, the bin, the payload members, the APP
//  NAME, the native base path and the config, and changes nothing else.
//
//  v2: ONE ORIGIN, ONE NAMED APP, NO SLOT. The runner is a multi-app registry now
//  (services/daytona/sandbox-specs.md §4, §7): the payload is registered through
//  `POST /apps` as `provider` and served at `<endpoint>/app/provider/…`, and a
//  replacement is the blue-green `PUT /apps/provider` — the previous revision keeps
//  serving until the new one is ready, and NO PEER APP IS TOUCHED. There is no v1
//  "slot", no `DELETE /`, and no root/default alias anywhere in this verb.
//
//  targetPrefix = /providers/private/v1 (spec §8.5). The provider's native routes are
//  `/providers/private/v1/{chat/completions,responses,models,stats}` (src/serve.rs), so
//  the app declares that base path and the gateway maps
//
//      /app/provider/models  →  127.0.0.1:<internal port>/providers/private/v1/models
//
//  The app name is passed EXPLICITLY: an omitted name means the documented `default`
//  app (runtime-deploy.ts `RuntimePayload.name`), which would put the provider on
//  `/app/default/` and collide with whatever else uses that alias.
//
//  READINESS IS DIRECT LOOPBACK `/health`. The bin serves `GET /health` at its ROOT
//  (src/bin/server.rs HEALTH_PATH) — the route is NOT part of the prefixed native
//  surface. The runner probes `http://127.0.0.1:<port><probePath>` (spec §8.7), so
//  `probePath: '/health'` reaches the bin directly. Going through the gateway would
//  map `/app/provider/health` to `/providers/private/v1/health`, which the crate does
//  NOT serve; this verb therefore never advertises that URL as the provider's health.
//
//  THE PAYLOAD (fixed contract, services/daytona/sandbox/runtime-server/src/contract.rs):
//
//      bundle   typecrate-provider.tar.gz — flat, gzip, exactly two members
//      ┌────────────────────────────┬────────┬───────────────────────────────────┐
//      │ member                     │ mode   │ why                               │
//      ├────────────────────────────┼────────┼───────────────────────────────────┤
//      │ typecrate-provider-server  │ 0755   │ the runner execs `./<member>`, so  │
//      │                            │        │ the exec bit is not cosmetic       │
//      │ agentic-provider.json      │ 0644   │ the credentials; mode 0644 because │
//      │                            │        │ nothing execs it                   │
//      └────────────────────────────┴────────┴───────────────────────────────────┘
//
//      config: { runCmd: './typecrate-provider-server',
//                artifactPath: 'typecrate-provider-server',
//                probePath: '/health', probeTimeoutSec: 120, stopSignal: 'SIGTERM',
//                env: { TYPECRATE_PROVIDER_VAULT_PATH: './agentic-provider.json',
//                       TYPECRATE_PROVIDER_HOST: '127.0.0.1',
//                       RUST_LOG: 'info' } }
//
//  NO `port` UNLESS THE OPERATOR NAMES ONE. v2 auto-allocates an internal port when
//  `config.port` is absent or 0 (spec §8.3/§17.3), and injects it as `PORT` into the
//  child; the bin honours `PORT` (src/bin/server.rs), so the provider needs no port
//  declaration at all. `--port <n>` is honoured for an operator who wants a fixed
//  internal port, but it is INTERNAL: clients use `/app/provider/…`, never a port.
//
//  `env.PORT` IS RESERVED and rejected LOCALLY (spec R-8.3/R-8.4). The runner injects
//  PORT from the internal port, a user value is a hard 400 `config_invalid`, and the
//  v1 "decoupled bind/probe warning" no longer exists — so this verb refuses the key at
//  parse time with the fix (`--port`), instead of shipping a payload that cannot boot.
//
//  THE BIN BINDS LOOPBACK. `TYPECRATE_PROVIDER_HOST=127.0.0.1` is set in `config.env`
//  (spec R-8.5: the deploy client supplies the app's own host variable), so the only
//  way in is the runner's gateway; the provider is never exposed on another interface.
//
//  CREDENTIALS TRAVEL IN THE BUNDLE, NOT IN `env`. The app record exposes the posted
//  config (filename/profile/env are registry metadata), the payload is anonymous in
//  both directions, and the container's control plane is only as protected as the
//  runner's admin auth — so a key placed in `config.env` would be published. The vault
//  document is therefore a bundle MEMBER and `env` carries only the relative PATH to it
//  (`./agentic-provider.json`, resolved against the runner's stage dir, which is the
//  process cwd: bin/server.rs HELP, `TYPECRATE_PROVIDER_VAULT_PATH` "relative to the
//  process's working directory").
//
//  TWO INDEPENDENT CREDENTIALS (spec §12.3). `--token` is the Daytona PREVIEW token
//  (`x-daytona-preview-token`, the provider edge). `--admin-token` / `--admin-token-file`
//  is the runner's admin Bearer (`Authorization: Bearer`), which gates mutations and app
//  log reads; it is NEVER printed, and the summary reports it by PRESENCE only. The file
//  form is preferred: the value never reaches a command line.
//
//  THE ENDPOINT IS AN ARGUMENT, NOT A CONSTANT. A box URL is a deployment address, not
//  package identity, so the URL comes from the operator (argument /
//  `TYPECRATE_PROVIDER_ENDPOINT` / the vault default in start-modal.ts). Nothing here
//  CREATES a container.
//
//  DRY RUN POSTURE. `--dry-run` ships nothing and touches no network — and it also does
//  not run cargo, so the only locally observable thing it needs is an existing artifact.
//  It resolves the binary from the candidate list the client derives from the repo-root
//  `.cargo/config.toml` `target-dir`, i.e. `.rust/target/release/<bin>`. Build it first
//  with `yarn workspace @typecrate/provider build:server`, or drop --dry-run and let the
//  client compile (its `--offline` retry is built in).
//
//  OUTPUT. Every human line is prefixed `[provider:deploy]` so it greps apart from the
//  client's `[runtime-deploy]` lines, and the LAST line is exactly one JSON object
//  (`buildSummary`) so `… | tail -1 | jq` works — including for a dry run, whose `status`
//  is `"planned"`, and for a teardown (`"removed": true|false`).
//
//  IMPORT IS A NO-OP. Everything meaningful is an exported pure function over explicit
//  input (parseArgs / buildPayloadConfig / buildPayloadEntries / resolveVaultPath /
//  resolveAdminToken / buildProviderPayload / buildDeployOptions / runDeployEndpoint),
//  and every side effect arrives through `ProviderDeps` (the client's `RuntimeDeps`
//  plus a `writeFile` seam). The entry guard at the bottom is the only thing that
//  touches process state, and it never fires under vitest.
// ══════════════════════════════════════════════════════════════════════════════

import { closeSync, existsSync, mkdirSync, openSync, readFileSync, readSync, statSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, join, resolve } from 'node:path';

import {
    CargoBuildError,
    DEFAULT_LOG_LINES,
    PayloadError,
    RESERVED_ENV_KEYS,
    RuntimeDeployError,
    appIdentity,
    buildSummary,
    cargoReleaseBin,
    createDefaultDeps as createClientDefaultDeps,
    deployPayload,
    encodeConfigHeader,
    packagePayload,
    readyBudgetMs,
    tailAppLogs,
    teardownApp,
} from '@services/daytona/sandbox';
import type { DeployOptions, FsAccess, PayloadEntry, RuntimeConfig, RuntimeDeps, RuntimePayload } from '@services/daytona/sandbox';

import { entrySignals, isCliEntryPoint, modulePathOf, scriptsDirOf } from './entry-guard';

// ══════════════════════════════════════════════════════════════════════════════
//  PINS — resolved from THIS module (never from cwd), as nexus's scripts do
// ══════════════════════════════════════════════════════════════════════════════

/** The verb's own file name: the entry guard's identity, and the usage line. */
export const ENTRY_SCRIPT_NAME = 'deploy-endpoint.ts';
/** Directory this module really sits in, so a run from the repo root resolves the same paths. */
const SCRIPTS_DIR = scriptsDirOf(import.meta.url);
/** Absolute path of this module — rule 3 of the entry guard matches its stack frames. */
const MODULE_FILE_PATH = modulePathOf(import.meta.url, SCRIPTS_DIR, ENTRY_SCRIPT_NAME);

/** Package root: the cwd cargo runs from, so the repo-root `.cargo/config.toml` is found by walking up. */
export const PACKAGE_ROOT = resolve(SCRIPTS_DIR, '..');
/** Repo root, three levels up — needed only to name the vault artifact the way the crate does. */
export const REPO_ROOT = resolve(PACKAGE_ROOT, '..', '..', '..');

/** Every line this script writes is prefixed, so it greps apart from the client's own lines. */
export const PREFIX = '[provider:deploy]';

/**
 * The registry key (spec §4.7). EXPLICIT on purpose: an omitted `RuntimePayload.name`
 * registers the `default` app at `/app/default/`, which is a shared alias this
 * provider must never claim.
 */
export const APP_NAME = 'provider';

/**
 * The app's native base path (spec §8.5). The crate's four routes already live under
 * `/providers/private/v1` (src/serve.rs), so the gateway mount
 * `/app/provider/<remainder>` becomes `/providers/private/v1/<remainder>` upstream.
 */
export const TARGET_PREFIX = '/providers/private/v1';

/** The app's public mount, derived from the name exactly as the registry derives it. */
export const APP_PATH = `/app/${APP_NAME}/`;

/** Cargo `-p` name (`Cargo.toml`, == npm name with `@` dropped and `/` → `-`). */
export const CARGO_PACKAGE = 'typecrate-provider';
/** Cargo target name (`[[bin]] name = …`, path `src/bin/server.rs`). */
export const CARGO_BIN = 'typecrate-provider-server';

/**
 * Bundle member names are the flat stage-dir paths, so they must equal `runCmd` /
 * `artifactPath` exactly. `SERVER_MEMBER` is also the bin name on purpose: the runner
 * reports the member list, and an operator reading a container log should be able to
 * match it to a cargo target without a translation table.
 */
export const SERVER_MEMBER = CARGO_BIN;
/** The credentials document, staged next to the binary. */
export const VAULT_MEMBER = 'agentic-provider.json';
/** The `x-runtime-filename` value. gzip ⇒ the runner extracts flat, whatever it is called. */
export const PAYLOAD_FILENAME = 'typecrate-provider.tar.gz';

/**
 * The crate's OWN native default (`serve::PROVIDER_PORT`, src/serve.rs; the bin's
 * `PORT` fallback). It is NOT applied to the payload: v2 auto-allocates an internal
 * port and injects it as `PORT`, which the bin honours. Kept exported because the docs
 * and the native `start:server` verb still name it.
 */
export const DEFAULT_PORT = 8800;

/**
 * The readiness probe path. The bin serves `/health` at its ROOT (src/bin/server.rs),
 * and the runner probes it DIRECTLY on loopback — so this is the one readiness rule
 * that also proves the provider process itself answered, without going through the
 * gateway (which would prepend TARGET_PREFIX and 404).
 */
export const DEFAULT_PROBE_PATH = '/health';

/**
 * The runner's probe window for this payload. The crate's default is 60 s; 120 s is
 * asked for because the first request after boot initialises the registry from the
 * vault document, and a cold container may pay that inside the probe window.
 */
export const DEFAULT_PROBE_TIMEOUT_SEC = 120;

/** The host-binding variable the provider's bin reads (src/bin/server.rs `TYPECRATE_PROVIDER_HOST`). */
export const PROVIDER_HOST_ENV = 'TYPECRATE_PROVIDER_HOST';
/** Bind loopback (spec R-8.5): the gateway is the only way in, never another interface. */
export const PROVIDER_HOST_LOOPBACK = '127.0.0.1';

/** The artifact the crate reads when neither vault env var is set (src/serve.rs DEFAULT_VAULT_ARTIFACT). */
export const DEFAULT_VAULT_PATH = join(REPO_ROOT, 'config', 'vault', 'generated', VAULT_MEMBER);
/** The exact string the operator has to run when that artifact is absent — same text the crate prints (src/serve.rs REGENERATION_COMMAND). */
export const VAULT_REGENERATION_COMMAND = 'yarn workspace @config/vault export -- --consumer agentic-provider';

/** Endpoint override, so a second box needs no flag (see start-modal.ts for the precedence). */
export const ENDPOINT_ENV = 'TYPECRATE_PROVIDER_ENDPOINT';
/** Runner-route token override. It authenticates the CONTAINER's routes, never the provider's own API. */
export const TOKEN_ENV = 'TYPECRATE_PROVIDER_ENDPOINT_TOKEN';

/**
 * The runner's §12.3 ADMIN bearer — INDEPENDENT of the preview token above. The two
 * names follow the house pattern (package-specific first, then the generic Daytona
 * one), so a sandbox created with `SANDBOX_ADMIN_TOKEN` needs no second variable for
 * this verb. The value is never printed; the summary reports it by presence only.
 */
export const ADMIN_TOKEN_ENV = 'TYPECRATE_PROVIDER_SANDBOX_ADMIN_TOKEN';
export const GENERIC_ADMIN_TOKEN_ENV = 'SANDBOX_ADMIN_TOKEN';
/** The PREFERRED form: a host file holding the token, so it never reaches a command line. */
export const ADMIN_TOKEN_FILE_ENV = 'TYPECRATE_PROVIDER_SANDBOX_ADMIN_TOKEN_FILE';
export const GENERIC_ADMIN_TOKEN_FILE_ENV = 'SANDBOX_ADMIN_TOKEN_FILE';

/** Operator-facing failure: a wrong argument or a missing input, with the fix in the message. */
export class ProviderDeployError extends Error {}

// ══════════════════════════════════════════════════════════════════════════════
//  USAGE
// ══════════════════════════════════════════════════════════════════════════════

export const USAGE = `usage: yarn workspace @typecrate/provider deploy:endpoint <url> [flags]
       (or: vite-node scripts/deploy-endpoint.ts <url> [flags])

  compiles src/bin/server.rs into a release ELF, packs it with the vault artifact as a
  two-member tar.gz, and registers it as the app "${APP_NAME}" (POST /apps, or the
  blue-green PUT /apps/${APP_NAME} when it already exists) on any endpoint that speaks
  the sandbox runtime v2 contract. Peers are never touched.

  clients use <endpoint>${APP_PATH}… ; the app declares targetPrefix "${TARGET_PREFIX}",
  so /app/${APP_NAME}/models maps to the bin's native /providers/private/v1/models.
  Readiness is the bin's own GET /health on the container's LOOPBACK port — never
  ${APP_PATH}health, which would map to ${TARGET_PREFIX}/health and 404.

  the endpoint (required — unless --out is given, which only packages):
    <url>                 positional, or --endpoint <url>
                          (env fallback: ${ENDPOINT_ENV})
    --token <t>           the Daytona PREVIEW token, sent as x-daytona-preview-token.
                          It authenticates the RUNNER's routes only; the deployed
                          provider itself is unauthenticated by contract.
                          (env fallback: ${TOKEN_ENV})
    --admin-token <t>     the runner's ADMIN bearer, sent as Authorization: Bearer on
                          mutations and app-log reads (spec §12.3). Independent of the
                          preview token, and never printed.
                          (env: ${ADMIN_TOKEN_ENV}, then ${GENERIC_ADMIN_TOKEN_ENV})
    --admin-token-file <path>
                          read the admin bearer from this host file (PREFERRED: the
                          value never reaches a command line).
                          (env: ${ADMIN_TOKEN_FILE_ENV}, then ${GENERIC_ADMIN_TOKEN_FILE_ENV})

  the payload:
    --port <n>            OPTIONAL internal loopback port; 0 (or omitting it) lets the
                          runner auto-allocate one (v2 default). Never public: clients
                          use ${APP_PATH}. Do NOT pass PORT in --env — it is reserved
                          (the runner injects it, and a user value is a 400).
    --probe-path <path>   loopback readiness probe (default ${DEFAULT_PROBE_PATH}: the bin's own
                          health route, direct on the container)
    --env K=V             extra config.env entry; repeatable, and it OVERRIDES the three
                          defaults (TYPECRATE_PROVIDER_VAULT_PATH, ${PROVIDER_HOST_ENV}, RUST_LOG).
                          Never a secret. PORT/APP_* are reserved and rejected here.
    --vault <path>        credentials file to ship (default
                          config/vault/generated/${VAULT_MEMBER.replace('.json', '')}.json);
                          a missing file is an error with its regeneration command
    --out <path>          ALSO write the assembled tar.gz here (host-side only, so it
                          works with --dry-run; a RELATIVE path is repo-root relative
                          (NOT cwd, which is the package dir under yarn workspace), so
                          --out Temp/x.tar.gz lands in the repo-root Temp/ AGENTS.md
                          wants; with no <url> nothing is deployed — the bundle is the
                          deliverable)
    --no-build            ship the already-built artifact instead of running cargo
    --artifact <path>     ship exactly this file (cargo never runs)
    --allow-unconfigured  append --allow-unconfigured to runCmd, i.e. boot the bin even
                          when the vault document is refused (it otherwise exits 1
                          BEFORE binding — src/bin/server.rs)

  app policy and verbs:
    (default)             an existing ready|failed app is replaced BLUE-GREEN with
                          PUT /apps/${APP_NAME}; it keeps serving ${APP_PATH} until the new
                          revision is ready, and every peer app is untouched
    --no-teardown         never replace: an existing app is an error
    --teardown            DELETE /apps/${APP_NAME} only — unregister this app, ship nothing
                          (peers and the runner keep serving)
    --no-wait             return after the 202 instead of polling GET /apps/${APP_NAME}
    --logs <n>            lines of GET /apps/${APP_NAME}/logs attached when a run fails
                          (default ${DEFAULT_LOG_LINES}; a tail is attempted on EVERY failure path)
    --dry-run             print the assembled payload + config; no probe, no POST/PUT,
                          no DELETE, no cargo (the artifact must already exist)
    -h, --help            this text

  after a successful run the app's base URL is <endpoint>${APP_PATH} — composed from the
  registry mount, never guessed — and the internal port in the summary is the registry's
  own loopback port, never a public one.`;

// ══════════════════════════════════════════════════════════════════════════════
//  FLAGS — pure parser: it returns errors instead of printing or exiting
// ══════════════════════════════════════════════════════════════════════════════

/** Every flag value after parsing; `main` decides how to report the errors it returns. */
export type ProviderFlags = {
    help: boolean;
    dryRun: boolean;
    /** `--teardown`: unregister the app and ship nothing. */
    teardownOnly: boolean;
    /** true = replace an existing app blue-green (the default); `--no-teardown` flips it. */
    replace: boolean;
    /** Empty when neither the argument, --endpoint nor the env supplied one. */
    endpoint: string;
    token: string;
    /** The runner's admin bearer (spec §12.3). Mutually exclusive with `adminTokenFile`. */
    adminToken: string;
    /** A host file holding the admin bearer — the preferred form. */
    adminTokenFile: string;
    /** undefined = auto-allocate (v2); `--port 0` is the explicit spelling of the same thing. */
    port?: number;
    /** The loopback readiness probe path; the bin's root `/health` by default. */
    probePath: string;
    env: Record<string, string>;
    vault: string;
    out: string;
    noBuild: boolean;
    artifact: string;
    wait: boolean;
    logsLines: number;
    /** Appends the bin's own flag to `runCmd`; the credential gate is otherwise fatal. */
    allowUnconfigured: boolean;
};

export type ParsedProviderArgs = { flags: ProviderFlags; errors: string[] };

/**
 * Reserved `config.env` keys, refused HERE so the operator sees the fix next to the
 * flag they typed. The runner rejects all of them with `400 config_invalid` (spec
 * R-8.3), and `PORT` gets its own wording because `--port` is the real knob: v2 has no
 * decoupled bind/probe (R-8.4), so a user PORT is never a warning.
 */
export const reservedEnvErrors = (env: Readonly<Record<string, string>>): string[] =>
    Object.keys(env)
        .filter((key) => (RESERVED_ENV_KEYS as readonly string[]).includes(key))
        .map((key) =>
            key === 'PORT'
                ? `--env PORT=${env[key]} is RESERVED: the runner injects PORT from the app's internal port and rejects a user value with 400 config_invalid — pass --port <n> (0 or omitted = auto-allocate) instead`
                : `--env ${key}=… is RESERVED: the runner owns ${key} and rejects a user value with 400 config_invalid — drop it`,
        );

/**
 * Strict flat-flag parser in the reference scripts' shape: the FIRST bare token is the
 * URL, `--env` repeats and accumulates (splitting on the first `=` so values may
 * contain `=`), a value flag followed by a `--`-leading token is a missing value, and
 * an unknown flag is an error. It never throws, never prints, never exits.
 */
/**
 * `options.requireEndpoint` defaults to true: THIS verb needs an endpoint. The wrapper
 * that MINTS one (`start:daytona`, which creates the sandbox) passes false, otherwise its
 * own default invocation — `start:daytona` with no flags at all — would be rejected by a
 * requirement that only makes sense for the verb that is given a URL.
 */
export const parseArgs = (argv: readonly string[], env: Readonly<Record<string, string | undefined>> = process.env, options: { requireEndpoint?: boolean } = {}): ParsedProviderArgs => {
    const flags: ProviderFlags = {
        help: false,
        dryRun: false,
        teardownOnly: false,
        replace: true,
        endpoint: env[ENDPOINT_ENV] ?? '',
        token: env[TOKEN_ENV] ?? '',
        // `||` so an empty package-specific variable falls through to the generic name.
        adminToken: env[ADMIN_TOKEN_ENV] || env[GENERIC_ADMIN_TOKEN_ENV] || '',
        adminTokenFile: env[ADMIN_TOKEN_FILE_ENV] || env[GENERIC_ADMIN_TOKEN_FILE_ENV] || '',
        // undefined = auto (§8.3); the v1 fixed-port default is deliberately gone.
        port: undefined,
        probePath: DEFAULT_PROBE_PATH,
        env: {},
        vault: DEFAULT_VAULT_PATH,
        out: '',
        noBuild: false,
        artifact: '',
        wait: true,
        logsLines: DEFAULT_LOG_LINES,
        allowUnconfigured: false,
    };
    const errors: string[] = [];
    /** The positional URL — the first token that is not a flag or a flag's value. */
    let positional = '';
    /**
     * The endpoint named on the COMMAND LINE only. `flags.endpoint` starts from
     * TYPECRATE_PROVIDER_ENDPOINT, which is a DEFAULT, not an assertion: letting it take
     * part in the "two different endpoints" check below would turn `start:modal
     * https://other--box.modal.run` (the whole point of the precedence rule) into an error.
     */
    let explicitEndpoint = '';
    const valueFlags = ['--endpoint', '--token', '--admin-token', '--admin-token-file', '--port', '--probe-path', '--env', '--vault', '--out', '--artifact', '--logs'];
    for (let index = 0; index < argv.length; index += 1) {
        const flag = argv[index]!;
        if (valueFlags.includes(flag)) {
            const value = argv[++index];
            if (value === undefined || value.startsWith('--')) {
                errors.push(`${flag} needs a value`);
                continue;
            }
            if (flag === '--endpoint') {
                flags.endpoint = value;
                explicitEndpoint = value;
            }
            else if (flag === '--token') flags.token = value;
            else if (flag === '--admin-token') flags.adminToken = value;
            else if (flag === '--admin-token-file') flags.adminTokenFile = value;
            else if (flag === '--vault') flags.vault = value;
            else if (flag === '--out') flags.out = value;
            else if (flag === '--artifact') flags.artifact = value;
            else if (flag === '--probe-path') {
                // Checked here rather than left to the runner's 400 so the operator sees
                // the rule next to the flag they typed.
                if (!value.startsWith('/')) errors.push(`--probe-path must start with "/" (it is an absolute route on the app's loopback listener), got "${value}"`);
                else flags.probePath = value;
            } else if (flag === '--port') {
                const port = Number(value);
                if (!Number.isInteger(port) || port < 0 || port > 65535) errors.push(`--port ${JSON.stringify(value)} is out of range (internal TCP port 0-65535, 0 = auto)`);
                // 0 is the explicit spelling of auto (§8.3) and must not reach the
                // payload as a literal `config.port`.
                else if (port !== 0) flags.port = port;
            } else if (flag === '--logs') {
                const lines = Number(value);
                if (!Number.isInteger(lines) || lines < 1 || lines > 1000) errors.push(`--logs ${JSON.stringify(value)} is out of range (1-1000 lines)`);
                else flags.logsLines = lines;
            } else {
                const eq = value.indexOf('=');
                if (eq <= 0) errors.push(`--env expects KEY=VALUE, got "${value}"`);
                else {
                    const key = value.slice(0, eq);
                    const reserved = reservedEnvErrors({ [key]: value.slice(eq + 1) });
                    // A reserved key is reported as itself, not as a generic parse
                    // failure: the fix is a different flag, not a different spelling.
                    if (reserved.length) errors.push(...reserved);
                    else flags.env[key] = value.slice(eq + 1);
                }
            }
            continue;
        }
        if (flag === '--dry-run') flags.dryRun = true;
        else if (flag === '--teardown') flags.teardownOnly = true;
        else if (flag === '--no-teardown') flags.replace = false;
        else if (flag === '--no-wait') flags.wait = false;
        else if (flag === '--no-build') flags.noBuild = true;
        else if (flag === '--allow-unconfigured') flags.allowUnconfigured = true;
        else if (flag === '-h' || flag === '--help') flags.help = true;
        else if (flag.startsWith('-')) errors.push(`unknown flag "${flag}"`);
        else if (positional) errors.push(`unexpected extra argument "${flag}" — the endpoint is the only positional (got "${positional}" already)`);
        else positional = flag;
    }
    // --help wins outright: a broken command line still gets the usage screen.
    if (flags.help) return { flags, errors };
    // Two admin credentials with no rule between them would make the Bearer depend on
    // which one the caller happened to read last; the file form is the preferred one,
    // so the operator picks it explicitly instead of getting it silently.
    if (flags.adminToken && flags.adminTokenFile) errors.push('--admin-token and --admin-token-file are mutually exclusive — pass ONE (the file form is preferred: the value never reaches a command line)');
    // `--out` with no endpoint is the package-only verb: an operator (or CI) that wants
    // the deployable bundle and nothing else. Any other run needs an endpoint.
    if (options.requireEndpoint !== false && !positional && !flags.endpoint && !flags.out) errors.push(`no endpoint — pass the runner URL as the argument, or --endpoint <url>, or ${ENDPOINT_ENV}=… (or use --out <path> to only build the bundle)`);
    // --teardown ships no bytes, so a shipping knob would be silently ignored. The admin
    // credential is NOT a shipping knob: DELETE /apps/<name> still needs it.
    if (flags.teardownOnly) {
        const shipping = [
            flags.out && '--out',
            flags.noBuild && '--no-build',
            flags.artifact && '--artifact',
            flags.probePath !== DEFAULT_PROBE_PATH && '--probe-path',
            flags.port !== undefined && '--port',
            flags.allowUnconfigured && '--allow-unconfigured',
            !flags.wait && '--no-wait',
            Object.keys(flags.env).length > 0 && '--env',
        ].filter(Boolean) as string[];
        if (shipping.length) errors.push(`--teardown ships nothing, so ${shipping.join(' / ')} do not apply (drop them, or drop --teardown)`);
    }
    if (positional && explicitEndpoint && positional !== explicitEndpoint) errors.push(`two different endpoints given: "${positional}" and --endpoint "${explicitEndpoint}"`);
    // The positional is the most explicit form there is, so it also overrides the
    // TYPECRATE_PROVIDER_ENDPOINT default without a word of complaint.
    if (positional) flags.endpoint = positional;
    return { flags, errors };
};

// ══════════════════════════════════════════════════════════════════════════════
//  THE PAYLOAD — declared here, assembled by the shared client
// ══════════════════════════════════════════════════════════════════════════════

/**
 * The payload's own `env` defaults, in the fixed order the contract lists them.
 * A relative `./` path is deliberate: the runner's cwd is the stage directory, so the
 * document is found whatever absolute prefix the extraction happened to use
 * (src/bin/server.rs HELP describes exactly that resolution).
 */
export const buildPayloadEnv = (flags: Pick<ProviderFlags, 'env'>): Record<string, string> => ({
    TYPECRATE_PROVIDER_VAULT_PATH: `./${VAULT_MEMBER}`,
    // spec R-8.5: the deploy client supplies the app's own host-binding variable. The
    // bin binds 0.0.0.0 by default (a container with a client on loopback was v1's
    // assumption); in v2 the gateway is the only way in, so loopback is the correct
    // interface and no other interface ever exposes the provider.
    [PROVIDER_HOST_ENV]: PROVIDER_HOST_LOOPBACK,
    RUST_LOG: 'info',
    // Operator values LAST: `--env` is how RUST_LOG gets raised to debug for one deploy,
    // and a value that could not override the default would be a dead flag. Reserved
    // keys (PORT, APP_*) were already refused by the parser.
    ...flags.env,
});

/**
 * `runCmd` — the only line that names the entry point. An ARCHIVE must carry it
 * (contract.rs refuses to guess), and the exec bit plus a flat member name is what
 * makes `./<member>` work from the stage dir.
 */
export const buildRunCmd = (flags: Pick<ProviderFlags, 'allowUnconfigured'>): string => `./${SERVER_MEMBER}${flags.allowUnconfigured ? ' --allow-unconfigured' : ''}`;

/**
 * The config the container receives, key for key. `port` is OMITTED unless the
 * operator asked for one: v2 auto-allocates and injects `PORT`, and the bin honours it.
 * `probePath` defaults to the bin's own root `/health`, probed directly on loopback.
 * Everything is forwarded to `normalizeRuntimeConfig` downstream, so an invalid value
 * fails here instead of as a 400 after the upload.
 */
export const buildPayloadConfig = (flags: Pick<ProviderFlags, 'port' | 'probePath' | 'env' | 'allowUnconfigured'>): RuntimeConfig => ({
    runCmd: buildRunCmd(flags),
    ...(flags.port === undefined ? {} : { port: flags.port }),
    artifactPath: SERVER_MEMBER,
    probeTimeoutSec: DEFAULT_PROBE_TIMEOUT_SEC,
    stopSignal: 'SIGTERM',
    probePath: flags.probePath,
    env: buildPayloadEnv(flags),
});

/**
 * The two members, with their modes. 0755 for the binary is load-bearing (the runner
 * execs it) and 0644 for the document is the mode an operator expects to edit — the
 * tar header carries it verbatim, so it is asserted member by member in the test suite.
 */
export const buildPayloadEntries = (artifactPath: string, vaultPath: string): PayloadEntry[] => [
    { name: SERVER_MEMBER, path: artifactPath, mode: 0o755 },
    { name: VAULT_MEMBER, path: vaultPath, mode: 0o644 },
];

/** File access default (the client keeps its own copy private, so this module owns one). */
const nodeFs: FsAccess = {
    exists: (path) => existsSync(path),
    read: (path) => readFileSync(path),
    // openSync/readSync, not readFileSync: an ELF can be tens of MiB and this seam exists
    // so a 4-byte magic check never slurps it (the client's own default does the same).
    readHead: (path, bytes) => {
        const buffer = Buffer.alloc(bytes);
        let fd: number | undefined;
        try {
            fd = openSync(path, 'r');
            return buffer.subarray(0, readSync(fd, buffer, 0, bytes, 0));
        } catch {
            // Unreadable (permissions, a build that raced us): "no magic" is what the
            // caller can act on, not a stack.
            return Buffer.alloc(0);
        } finally {
            if (fd !== undefined) closeSync(fd);
        }
    },
    stat: (path) => {
        try {
            const stats = statSync(path);
            return { size: stats.size, isFile: stats.isFile() };
        } catch {
            return null;
        }
    },
};

/** Extra seams beyond the client's: the bundle writer. Everything else is the client's own. */
export type ProviderDeps = RuntimeDeps & {
    /** real: mkdir -p of the parent, then writeFileSync. */
    writeFile?: (path: string, buffer: Buffer) => void;
};

/**
 * Where `--out <path>` lands. A RELATIVE value resolves against REPO_ROOT, not
 * `process.cwd()`: `yarn workspace @typecrate/provider deploy:endpoint` runs with the PACKAGE
 * directory as its cwd, so a cwd-relative `--out Temp/provider.tar.gz` — the spelling this
 * file documents — would create `packages/typecrate/provider/Temp/`, i.e. scratch inside the
 * package tree, which root AGENTS.md ("Scratch Space — use repo-root `Temp/`") forbids.
 *
 * An ABSOLUTE value is used verbatim, so `/tmp/x.tar.gz` and a CI artifact directory keep
 * working. There is no DEFAULT: an unspecified `--out` writes nothing at all (a deploy is not
 * a deploy-plus-a-local-bundle), so the only way to land a file is to ask for one, and asking
 * relatively now means repo-root-relative.
 */
export const resolveOutPath = (value: string, repoRoot: string = REPO_ROOT): string => (isAbsolute(value) ? value : join(repoRoot, value));


/** The real bundle writer: parent directories included, because `--out Temp/x/y.tar.gz` is the natural spelling. */
export const writeFileDefault = (path: string, buffer: Buffer): void => {
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, buffer);
};

/** The default dep set: the client's real fetch, plus the real filesystem and writer. */
export const createDefaultDeps = (): ProviderDeps => ({ ...createClientDefaultDeps(), fs: nodeFs, writeFile: writeFileDefault });

/**
 * The vault artifact, resolved and PROVED to exist before anything is compiled. Its
 * absence is the most common failure on a fresh clone, so it gets the crate's own
 * regeneration command rather than a stack trace — the same posture as the crate's 503
 * body (src/serve.rs REGENERATION_COMMAND).
 */
export const resolveVaultPath = (flags: Pick<ProviderFlags, 'vault'>, deps: ProviderDeps): string => {
    const fs = deps.fs ?? nodeFs;
    const path = resolve(flags.vault);
    const stats = fs.stat(path);
    if (!stats || !stats.isFile || stats.size === 0)
        throw new ProviderDeployError(
            `the credentials file is not a readable non-empty file: ${path}\n` +
                `  it is git-ignored and generated, so a fresh checkout does not have it. Regenerate it with:\n` +
                `  ${VAULT_REGENERATION_COMMAND}\n` +
                `  (or point --vault at a document you already have — its contents are shipped inside the bundle, never into config.env)`,
        );
    return path;
};

/**
 * The admin token FILE, anchored exactly once. `resolve()` here (not in the caller) is
 * what keeps the two consumers on ONE file: this module reads the value, and
 * start:daytona hands the same path to createSandbox, which uploads it. A relative
 * `--admin-token-file` therefore means "from the process cwd" for both, never two
 * different files depending on who resolved it.
 */
export const resolveAdminTokenFile = (flags: Pick<ProviderFlags, 'adminTokenFile'>): string | undefined => (flags.adminTokenFile ? resolve(flags.adminTokenFile) : undefined);

/**
 * The runner's admin Bearer, resolved from the flag/env pair. The FILE form is read
 * here with the injected filesystem seam, so a test never touches a real secret file;
 * its value is never logged and never returned in any summary/plan structure.
 */
export const resolveAdminToken = (flags: Pick<ProviderFlags, 'adminToken' | 'adminTokenFile'>, deps: ProviderDeps): string => {
    if (flags.adminToken && flags.adminTokenFile) throw new ProviderDeployError('the admin credential is configured twice (--admin-token and --admin-token-file) — pass ONE');
    if (flags.adminToken) return flags.adminToken;
    const path = resolveAdminTokenFile(flags);
    if (path === undefined) return '';
    const fs = deps.fs ?? nodeFs;
    const stats = fs.stat(path);
    if (!stats || !stats.isFile || stats.size === 0)
        throw new ProviderDeployError(
            `the admin token file is not a readable non-empty file: ${path}\n` +
                `  the runner's admin Bearer (spec §12.3) is independent of the preview token; give the file the token, chmod 600 it, or pass --admin-token instead`,
        );
    const token = fs.read(path).toString('utf8').trim();
    // A blank line or a stray newline-only file must not become "no auth": the caller
    // asked for admin auth, and silently dropping it would deploy against an
    // unauthenticated control plane while believing otherwise.
    if (!token) throw new ProviderDeployError(`the admin token file ${path} holds no token after trimming — write the token itself (no quotes, no labels) or pass --admin-token`);
    return token;
};

/** What this run assembled — returned as data so a test (and start-daytona.ts) can assert it. */
export type ProviderPayload = {
    /** The bytes to POST, plus the name, targetPrefix and member table the client computed. */
    payload: RuntimePayload;
    /** The artifact that went in, and how it was located (cargo NDJSON, a candidate, or --artifact). */
    artifact: { path: string; sizeBytes: number; source: 'explicit' | 'cargo-json' | 'candidate' };
    vaultPath: string;
    members: { name: string; size: number; mode: number }[];
    bytes: number;
};

/**
 * cargo → vault → tar.gz, in that order: compiling first means a build failure is
 * reported before the artifact's absence is, and the ordering is deterministic for the
 * operator. A dry run never compiles (`noBuild`), which is what keeps `--dry-run`
 * runnable on a host with no registry access at all.
 */
export const buildProviderPayload = async (flags: ProviderFlags, deps: ProviderDeps): Promise<ProviderPayload> => {
    const artifact = await cargoReleaseBin(
        {
            cwd: PACKAGE_ROOT,
            package: CARGO_PACKAGE,
            bin: CARGO_BIN,
            artifact: flags.artifact || undefined,
            // A dry run reads an existing artifact; --no-build says the same thing
            // explicitly for a real run.
            noBuild: flags.noBuild || flags.dryRun,
            log: deps.log,
            env: deps.env,
        },
        deps,
    );
    const vaultPath = resolveVaultPath(flags, deps);
    const packaged = packagePayload({ filename: PAYLOAD_FILENAME, entries: buildPayloadEntries(artifact.path, vaultPath) }, deps);
    return {
        // The NAME and TARGET PREFIX are registry metadata (spec §4.7/§8.5), top-level
        // on the wire (headers for a binary upload) — never config keys.
        payload: { buffer: packaged.buffer, filename: packaged.filename, config: buildPayloadConfig(flags), name: APP_NAME, targetPrefix: TARGET_PREFIX },
        artifact: { path: artifact.path, sizeBytes: artifact.sizeBytes, source: artifact.source },
        vaultPath,
        members: packaged.members,
        bytes: packaged.bytes,
    };
};

// ══════════════════════════════════════════════════════════════════════════════
//  THE VERB — orchestration over the shared client, every side effect injected
// ══════════════════════════════════════════════════════════════════════════════

/** The client options this run implies. Exported so the test pins the flag mapping directly. */
export const buildDeployOptions = (flags: ProviderFlags): DeployOptions => ({
    token: flags.token,
    // The admin Bearer travels on mutations AND on the app-log read; it is independent
    // of the preview token and never printed (the client reports presence only).
    adminToken: flags.adminToken,
    // v2 replaces in place with PUT /apps/<name>; the v1 `teardownFirst` spelling is an
    // alias the client still accepts, but `replace` says what actually happens.
    replace: flags.replace,
    noWait: !flags.wait,
    logsLines: flags.logsLines,
    dryRun: flags.dryRun,
    // The bundle's artifact member is the thing the container execs, so the client's
    // format guard applies.
    expectElf: true,
    // The summary is printed by THIS module as the last line, after the operator notes.
    printSummary: false,
});

/**
 * The dry-run plan: what would ship, and the exact config it would ship with. Exported
 * because start-daytona.ts prints the SAME payload plan under its own create block —
 * two copies of that table would drift the moment a member changes.
 */
export const printPayloadPlan = (flags: ProviderFlags, built: ProviderPayload, config: RuntimeConfig, notes: string[], log: (line: string) => void, endpointText?: string): void => {
    const line = (text: string) => log(`${PREFIX} ${text}`);
    line('DRY RUN — no probe, no PUT, no DELETE, no POST, no cargo, no network');
    line(`endpoint:  ${endpointText ?? (flags.endpoint || '(none — --out only, nothing to deploy)')}`);
    // The mount is COMPOSED from the endpoint the caller already knows (spec §4.2): it is
    // never a per-port preview link, and `<endpoint>` is only the placeholder for a run
    // that has not got one yet (start:daytona's create-then-deploy plan).
    line(`app:       "${APP_NAME}" — clients use ${flags.endpoint || '<endpoint>'}${APP_PATH}… ; native prefix ${TARGET_PREFIX}`);
    line(`artifact:  ${built.artifact.path}  (${built.artifact.source}, ${built.artifact.sizeBytes} bytes, Linux ELF)`);
    line(`vault:     ${built.vaultPath}`);
    line(`bundle:    ${built.payload.filename}  (${built.bytes} bytes gzip, ${built.members.length} members, flat)`);
    for (const member of built.members) line(`             ${member.mode.toString(8).padStart(4, '0')}  ${member.name}  ${member.size} bytes`);
    line(`config (${Object.keys(config).length} keys, sent as the x-runtime-config header):`);
    line(`           ${JSON.stringify(config)}`);
    line(`           base64url: ${encodeConfigHeader(config)}`);
    line(`readiness: HTTP 2xx on ${config.probePath} via LOOPBACK within ${config.probeTimeoutSec}s — client budget ${readyBudgetMs(config)} ms`);
    line(`           (direct to the bin: ${APP_PATH}health would map to ${TARGET_PREFIX}/health and 404, so it is NOT the probe)`);
    line(`app policy: ${flags.replace ? `an existing app is replaced blue-green with PUT /apps/${APP_NAME} — peers untouched` : 'an existing app is an error (--no-teardown)'}`);
    line(`auth:      preview ${flags.token ? `*** (length ${flags.token.length})` : '(none)'} · admin ${flags.adminToken || flags.adminTokenFile ? 'Bearer configured (value never printed)' : '(none)'}`);
    for (const note of notes) line(`note:      ${note}`);
};

/**
 * Runs the whole verb and returns the process exit code (0 = serving / removed / plan
 * printed / bundle written). Never calls process.exit, so the wrappers and the tests can
 * observe it.
 */
export const runDeployEndpoint = async (flags: ProviderFlags, deps: ProviderDeps = createDefaultDeps()): Promise<number> => {
    const log = deps.log ?? ((text: string) => console.log(text));
    const error = deps.error ?? ((text: string) => console.error(text));
    const line = (text: string) => log(`${PREFIX} ${text}`);
    const writeFile = deps.writeFile ?? writeFileDefault;
    // Resolved ONCE, before any network call: a bad admin-token file is an operator
    // error, not a mid-deploy surprise, and the value only ever reaches the client.
    const adminToken = resolveAdminToken(flags, deps);

    // ── --teardown: ONE app, and nothing else. No cargo, no packaging, no peers. ──
    if (flags.teardownOnly) {
        const freed = await teardownApp(flags.endpoint, APP_NAME, deps, { token: flags.token, adminToken });
        line(`✓ app "${APP_NAME}" ${freed.removed ? 'removed' : 'was not registered (nothing to remove)'} at ${flags.endpoint} — peers untouched, the runner keeps serving`);
        log(
            buildSummary({
                mode: 'teardown',
                endpoint: flags.endpoint,
                name: APP_NAME,
                status: 'ready',
                notes: [`DELETE /apps/${APP_NAME} — the runner keeps serving every other app`],
                removed: freed.removed,
                ...(flags.token ? { token: flags.token } : {}),
                ...(adminToken ? { adminToken } : {}),
                ...(freed.replaced ? { replaced: appIdentity(freed.replaced) } : {}),
            }),
        );
        return 0;
    }

    // ── assemble ──
    const built = await buildProviderPayload(flags, deps);
    const config = built.payload.config as RuntimeConfig;

    // `--out` is honored even on a dry run: writing a local file is not a container side
    // effect, and producing the bundle offline is half of what the flag is for.
    if (flags.out) {
        writeFile(resolveOutPath(flags.out), built.payload.buffer);
        line(`bundle written: ${resolveOutPath(flags.out)} (${built.bytes} bytes)`);
    }

    // No endpoint + --out is the package-only verb: the bundle is the deliverable.
    // `flags.out` is guaranteed non-empty here — parseArgs rejects "no endpoint and no
    // --out", and this module only ever reaches the network past this point.
    if (!flags.endpoint) {
        line('no endpoint given — the bundle was the deliverable, nothing was deployed');
        printPayloadPlan(flags, built, config, [], log);
        // Same rule as every other path: the LAST line is one bare JSON summary, so
        // `tail -1 | jq` works on a package-only run too. 'planned' is the client's own
        // word for "assembled, nothing sent", and the bundle's whereabouts travel in
        // notes because buildSummary has no field for them.
        log(
            buildSummary({
                mode: 'deploy',
                // '' is the honest value: buildSummary always names the endpoint, and a
                // package-only run has none.
                endpoint: '',
                name: APP_NAME,
                status: 'planned',
                publicUrl: null,
                path: APP_PATH,
                ...(flags.port === undefined ? {} : { port: flags.port }),
                profile: 'anonymous',
                filename: built.payload.filename,
                notes: [
                    `bundle written to ${resolveOutPath(flags.out)} (${built.bytes} bytes, ${built.members.length} members)`,
                    `no endpoint — POST it later with \`deploy:endpoint <url> --no-build\` as app "${APP_NAME}" at ${APP_PATH}`,
                ],
                ...(flags.token ? { token: flags.token } : {}),
                ...(adminToken ? { adminToken } : {}),
            }),
        );
        return 0;
    }

    // The deploy options carry the RESOLVED admin token (a token file never reaches
    // this structure as a path, and its value is never logged).
    const result = await deployPayload(flags.endpoint, built.payload, deps, buildDeployOptions({ ...flags, adminToken }));

    if (flags.dryRun) printPayloadPlan(flags, built, config, result.notes, log);
    else if (!result.ok) {
        error(`${PREFIX} ✗ ${result.status}: ${result.error ?? 'the app never became ready'}`);
        for (const note of result.notes) error(`${PREFIX} note: ${note}`);
        // The app's own log is the only view of WHY (bind failure, refused vault
        // document, ENOEXEC). `deployPayload` attaches it whenever it could read it;
        // this verb re-reads the shared route once when it did not, so the failure path
        // still ends with a tail (or with the endpoint's own reason for having none).
        if (result.logs) {
            error(`${PREFIX} ── GET /apps/${APP_NAME}/logs (${result.logs.split('\n').length} lines) ──`);
            for (const entry of result.logs.split('\n')) error(`${PREFIX} ${entry}`);
        } else if (result.status === 'failed' || result.status === 'timeout') {
            // A failed/timeout run HAS an app record to read, so a missing tail would
            // hide the only view of why. (For `busy`/`rejected`/`unauthorized` there is
            // no failed revision to explain, and the status line above is the answer.)
            const tail = await tailAppLogs(flags.endpoint, APP_NAME, deps, { token: flags.token, adminToken, lines: flags.logsLines });
            if (tail.ok && tail.text) {
                error(`${PREFIX} ── GET /apps/${APP_NAME}/logs (${tail.text.split('\n').length} lines) ──`);
                for (const entry of tail.text.split('\n')) error(`${PREFIX} ${entry}`);
            } else {
                error(`${PREFIX} no app log tail: GET /apps/${APP_NAME}/logs → ${tail.status === 0 ? `no HTTP reply (${tail.text})` : `HTTP ${tail.status}${tail.text ? `: ${tail.text.slice(0, 200)}` : ''}`}`);
            }
        }
        error(`${PREFIX} the app "${APP_NAME}" may still be registered as failed: rerun this verb to replace it blue-green, or unregister it with deploy:endpoint ${flags.endpoint} --teardown`);
    } else {
        line(`✓ ${result.status} — app ${result.name} revision ${result.revision ?? '?'}, internal port ${result.port}`);
        line(`  provider (clients): ${result.publicUrl}  (mount ${result.path} → native ${TARGET_PREFIX}/…)`);
        line(`  provider health:    the bin's GET ${DEFAULT_PROBE_PATH} on the container's LOOPBACK port — NOT exposed as ${result.path}health`);
        line(`  management:         GET ${flags.endpoint}/apps · PUT|DELETE ${flags.endpoint}/apps/${APP_NAME} · GET ${flags.endpoint}/apps/${APP_NAME}/logs`);
        line(`  unregister it:      yarn workspace @typecrate/provider deploy:endpoint ${flags.endpoint} --teardown`);
        for (const note of result.notes) line(`note: ${note}`);
    }

    // LAST line, bare: the prefixes are for humans, and a caller must be able to
    // `tail -1 | jq` this run.
    log(result.summary);
    // A dry run "did its job" (the client counts 'planned' as ok); so does a bundle write.
    return result.ok ? 0 : 1;
};

/** Error → one line, so a failure never surfaces as a bare [object Object]. */
const describe = (failure: unknown): string => (failure instanceof Error ? failure.message : String(failure));

/**
 * Parses, validates, runs, maps the result onto an exit code, and turns every NAMED
 * failure (operator-facing, with its own fix) into one line. An unexpected failure keeps
 * its stack, so a real defect is not dressed up as operator error.
 */
export const main = async (argv: readonly string[] = process.argv.slice(2), deps: ProviderDeps = createDefaultDeps()): Promise<number> => {
    const log = deps.log ?? ((text: string) => console.log(text));
    const error = deps.error ?? ((text: string) => console.error(text));
    const { flags, errors } = parseArgs(argv, deps.env ?? process.env);
    if (flags.help) {
        log(USAGE);
        return 0;
    }
    if (errors.length) {
        for (const problem of errors) error(`${PREFIX} ✗ ${problem}`);
        error('');
        error(USAGE);
        return 1;
    }
    try {
        return await runDeployEndpoint(flags, deps);
    } catch (failure) {
        const known = failure instanceof ProviderDeployError || failure instanceof RuntimeDeployError || failure instanceof PayloadError || failure instanceof CargoBuildError;
        if (known) {
            error(`${PREFIX} ✗ ${describe(failure)}`);
            // The two cargo-shaped failures both end in "no artifact": naming the verb
            // that fixes them is the difference between a retry and a stack read.
            if (failure instanceof CargoBuildError) error(`${PREFIX} hint: build it with \`yarn workspace @typecrate/provider build:server\` (a --dry-run never compiles; cargo's own --offline retry already ran)`);
            return 1;
        }
        error(`${PREFIX} ✗ unexpected failure: ${failure instanceof Error ? (failure.stack ?? failure.message) : String(failure)}`);
        return 1;
    }
};

// ══════════════════════════════════════════════════════════════════════════════
//  ENTRY POINT — importing this module must never start anything (see entry-guard.ts)
// ══════════════════════════════════════════════════════════════════════════════

if (isCliEntryPoint(entrySignals(MODULE_FILE_PATH))) {
    main().then(
        (code) => {
            process.exitCode = code;
        },
        (failure) => {
            console.error(`${PREFIX} ✗ unexpected failure: ${describe(failure)}`);
            process.exitCode = 1;
        },
    );
}
