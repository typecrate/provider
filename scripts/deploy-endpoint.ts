// ══════════════════════════════════════════════════════════════════════════════
//  deploy-endpoint.ts — COMPILE THIS CRATE'S OWN NATIVE BINARY, PACKAGE IT AS AN
//  ANONYMOUS PAYLOAD, AND HAND IT TO ANY ENDPOINT THAT SPEAKS RUNTIME CONTRACT v1
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
//  `/v1/harness`. `packages/typecrate/nexus/scripts/` already drives that flow, but it
//  hardcodes the hub (its profile, its binary, port 8085, `/health` probing, and it
//  shells out to its own `deploy-daytona.ts`), so it cannot host a second workload.
//  The generic client under `services/daytona/sandbox/runtime-deploy.ts` is what both
//  workloads share — this file is a thin WORKLOAD declaration on top of it: it names
//  the crate, the bin, the payload members and the config, and changes nothing else.
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
//      config: { port: 8800, runCmd: './typecrate-provider-server',
//                artifactPath: 'typecrate-provider-server',
//                probeTimeoutSec: 120, stopSignal: 'SIGTERM',
//                env: { TYPECRATE_PROVIDER_VAULT_PATH: './agentic-provider.json',
//                       RUST_LOG: 'info' } }
//
//  NO `probePath` BY DESIGN. The anonymous profile treats an ABSENT `probePath` as
//  "ready when the port accepts a TCP connection" (contract.rs), which is the
//  only readiness rule that does not assume the container knows something about the
//  payload. `--probe-path /health` opts back into the HTTP 2xx probe — and `/health`
//  exists here (bin/server.rs HEALTH_PATH), unlike most payloads; it is opt-in because
//  it makes the deploy depend on a path the box has to forward.
//
//  CREDENTIALS TRAVEL IN THE BUNDLE, NOT IN `env`. `GET /status` echoes the posted
//  config VERBATIM to a caller that presented no secret (contract.rs, and the
//  client's own `configNotes` warns about it), and the container has no auth by default
//  — so a key placed in `config.env` would be published to anyone who can reach the
//  URL. The vault document is therefore a bundle MEMBER and `env` carries only the
//  relative PATH to it (`./agentic-provider.json`, resolved against the runner's stage
//  dir, which is the process cwd: bin/server.rs HELP, `TYPECRATE_PROVIDER_VAULT_PATH`
//  "relative to the process's working directory"). The payload is anonymous in both
//  directions: the container does not know what it runs, and it does not authenticate
//  the caller — the provider's own upstream credentials are the payload's business.
//
//  THE ENDPOINT IS AN ARGUMENT, NOT A CONSTANT. A Modal box URL is a deployment
//  address, not package identity, and this repo's box is already occupied by another
//  workload, so the URL comes from the operator (argument / `TYPECRATE_PROVIDER_ENDPOINT`
//  / the vault default in start-modal.ts). Nothing here CREATES a container: box,
//  Daytona preview link, Modal sandbox tunnel and a loopback runner are the same client
//  (runtime-deploy.ts header), and the slot policy — busy slot → `DELETE /`, or
//  `DELETE /sandbox` on a box, which replaces the CONTAINER — is the client's, decided
//  from the endpoint's OWN /health, never from a flag.
//
//  DRY RUN POSTURE. `--dry-run` ships nothing and touches no network (the client's own
//  posture, runtime-deploy.ts:1777-1787) — and it also does not run cargo, so the only
//  locally observable thing it needs is an existing artifact. It resolves the binary from
//  the candidate list the client derives from the repo-root `.cargo/config.toml`
//  `target-dir` (runtime-deploy.ts:874-899), i.e. `.rust/target/release/<bin>`. Build it
//  first with `yarn workspace @typecrate/provider build:server`, or drop --dry-run and
//  let the client compile (its `--offline` retry is built in, which is what a host with
//  no registry access needs).
//
//  OUTPUT. Every human line is prefixed `[provider:deploy]` so it greps apart from the
//  client's `[runtime-deploy]` lines, and the LAST line is exactly one JSON object
//  (`buildSummary`, runtime-deploy.ts:1600-1631) so `… | tail -1 | jq` works — including
//  for a dry run, whose `status` is `"planned"`.
//
//  IMPORT IS A NO-OP. Everything meaningful is an exported pure function over explicit
//  input (parseArgs / buildPayloadConfig / buildPayloadEntries / resolveVaultPath /
//  buildProviderPayload / runDeployEndpoint), and every side effect arrives through
//  `ProviderDeps` (the client's `RuntimeDeps` plus a `writeFile` seam). The entry guard
//  at the bottom is the only thing that touches process state, and it never fires under
//  vitest — deploy-endpoint.test.ts drives the whole verb with zero network and zero
//  cargo. The guard lives in entry-guard.ts because start-modal.ts and start-daytona.ts
//  import `main` from here and must not re-trigger it.
// ══════════════════════════════════════════════════════════════════════════════

import { closeSync, existsSync, mkdirSync, openSync, readFileSync, readSync, statSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, join, resolve } from 'node:path';

import {
    CargoBuildError,
    DEFAULT_LOG_LINES,
    PayloadError,
    RuntimeDeployError,
    buildSummary,
    cargoReleaseBin,
    createDefaultDeps as createClientDefaultDeps,
    deployPayload,
    encodeConfigHeader,
    packagePayload,
    readyBudgetMs,
    teardown,
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
 * The provider's listener. MUST equal `serve::PROVIDER_PORT` (src/serve.rs:169) and the
 * bin's own default (src/bin/server.rs, `PORT` → 8800); it is restated here because the
 * PAYLOAD declares the port the runner probes, and a container runner injects `PORT`
 * from that declaration (the Rust runtime-server `childEnv`,
 * modal-sandbox-container.py:1895-1899 `_child_env`), so
 * the two numbers cannot be allowed to drift.
 */
export const DEFAULT_PORT = 8800;

/**
 * The runner's probe window for this payload. The crate's default is 60 s
 * (RUNNER_DEFAULTS); 120 s is asked for because the first request after boot initialises
 * the registry from the vault document, and a cold container may pay that inside the
 * probe window.
 */
export const DEFAULT_PROBE_TIMEOUT_SEC = 120;

/** The artifact the crate reads when neither vault env var is set (src/serve.rs DEFAULT_VAULT_ARTIFACT). */
export const DEFAULT_VAULT_PATH = join(REPO_ROOT, 'config', 'vault', 'generated', VAULT_MEMBER);
/** The exact string the operator has to run when that artifact is absent — same text the crate prints (src/serve.rs REGENERATION_COMMAND). */
export const VAULT_REGENERATION_COMMAND = 'yarn workspace @config/vault export -- --consumer agentic-provider';

/** Endpoint override, so a second box needs no flag (see start-modal.ts for the precedence). */
export const ENDPOINT_ENV = 'TYPECRATE_PROVIDER_ENDPOINT';
/** Runner-route token override. It authenticates the CONTAINER's routes, never the provider's own API. */
export const TOKEN_ENV = 'TYPECRATE_PROVIDER_ENDPOINT_TOKEN';

/** Operator-facing failure: a wrong argument or a missing input, with the fix in the message. */
export class ProviderDeployError extends Error {}

// ══════════════════════════════════════════════════════════════════════════════
//  USAGE
// ══════════════════════════════════════════════════════════════════════════════

export const USAGE = `usage: yarn workspace @typecrate/provider deploy:endpoint <url> [flags]
       (or: vite-node scripts/deploy-endpoint.ts <url> [flags])

  compiles src/bin/server.rs into a release ELF, packs it with the vault artifact as
  an ANONYMOUS two-member tar.gz, POSTs it to any endpoint that speaks the runtime
  contract v1 (Modal box, Modal sandbox tunnel, Daytona preview link, loopback runner),
  and waits until the CONTAINER says the payload is serving.

  the endpoint (required — unless --out is given, which only packages):
    <url>                 positional, or --endpoint <url>
                          (env fallback: ${ENDPOINT_ENV})
    --token <t>           the endpoint's own secret, sent as x-daytona-preview-token.
                          It authenticates the RUNNER's routes only; the deployed
                          provider itself is unauthenticated by contract.
                          (env fallback: ${TOKEN_ENV})

  the payload:
    --port <n>            port the provider listens on inside the container
                          (default ${DEFAULT_PORT}; also sent as config.port, which the
                          runner injects into the child as PORT)
    --probe-path <path>   probe readiness over HTTP instead of a bare TCP connect
                          (e.g. --probe-path /health); absent = TCP, no path pinned
    --env K=V             extra config.env entry; repeatable, and it OVERRIDES the two
                          defaults (TYPECRATE_PROVIDER_VAULT_PATH, RUST_LOG).
                          Never a secret: GET /status echoes config to anonymous callers
    --vault <path>        credentials file to ship (default
                          config/vault/generated/${VAULT_MEMBER.replace('.json', '')}.json);
                          a missing file is an error with its regeneration command
    --out <path>          ALSO write the assembled tar.gz here (host-side only, so it
                          works with --dry-run; a RELATIVE path is repo-root relative (NOT
                          cwd, which is the package dir under yarn workspace), so
                          --out Temp/x.tar.gz lands in the repo-root Temp/ AGENTS.md wants;
                          with no <url> nothing is deployed — the bundle is the deliverable)
    --no-build            ship the already-built artifact instead of running cargo
    --artifact <path>     ship exactly this file (cargo never runs)
    --allow-unconfigured  append --allow-unconfigured to runCmd, i.e. boot the bin even
                          when the vault document is refused (it otherwise exits 1
                          BEFORE binding — src/bin/server.rs)

  slot policy and verbs:
    (default)             a busy slot is RESOLVED first: DELETE / anywhere, or
                          DELETE /sandbox on a Modal box (that replaces the CONTAINER)
    --no-teardown         never replace: a busy slot is an error
    --teardown            DELETE / only — free the slot, ship nothing (no cargo, no pack)
    --no-wait             return after the 202 instead of polling GET /status
    --logs <n>            lines of GET /logs attached when a run fails (default ${DEFAULT_LOG_LINES};
                          a tail is attached on EVERY failure path whether or not you pass this)
    --dry-run             print the assembled payload + config; no probe, no DELETE,
                          no POST, no cargo (the artifact must already exist)
    -h, --help            this text

  the provider's own URL after a successful run is read from the endpoint's
  GET /status → runtime.preview.url and printed as publicUrl — never guessed. On a Modal
  box that IS the box URL (a box forwards every non-contract path to the ready port); on
  Daytona it is that port's preview link, which carries its own token.`;

// ══════════════════════════════════════════════════════════════════════════════
//  FLAGS — pure parser: it returns errors instead of printing or exiting
// ══════════════════════════════════════════════════════════════════════════════

/** Every flag value after parsing; `main` decides how to report the errors it returns. */
export type ProviderFlags = {
    help: boolean;
    dryRun: boolean;
    /** `--teardown`: free the slot and ship nothing. */
    teardownOnly: boolean;
    /** true = resolve a busy slot (the default); `--no-teardown` flips it. */
    replace: boolean;
    /** Empty when neither the argument, --endpoint nor the env supplied one. */
    endpoint: string;
    token: string;
    port: number;
    /** Empty string = the contract's TCP-connect readiness (no `probePath` key). */
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
        port: DEFAULT_PORT,
        probePath: '',
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
    const valueFlags = ['--endpoint', '--token', '--port', '--probe-path', '--env', '--vault', '--out', '--artifact', '--logs'];
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
            else if (flag === '--vault') flags.vault = value;
            else if (flag === '--out') flags.out = value;
            else if (flag === '--artifact') flags.artifact = value;
            else if (flag === '--probe-path') {
                // Checked here rather than left to the runner's 400 so the operator sees
                // the rule next to the flag they typed.
                if (!value.startsWith('/')) errors.push(`--probe-path must start with "/" (or omit it for TCP-connect readiness), got "${value}"`);
                else flags.probePath = value;
            } else if (flag === '--port') {
                const port = Number(value);
                if (!Number.isInteger(port) || port < 1 || port > 65535) errors.push(`--port ${JSON.stringify(value)} is out of range (TCP port 1-65535)`);
                else flags.port = port;
            } else if (flag === '--logs') {
                const lines = Number(value);
                if (!Number.isInteger(lines) || lines < 1 || lines > 1000) errors.push(`--logs ${JSON.stringify(value)} is out of range (1-1000 lines)`);
                else flags.logsLines = lines;
            } else {
                const eq = value.indexOf('=');
                if (eq <= 0) errors.push(`--env expects KEY=VALUE, got "${value}"`);
                else flags.env[value.slice(0, eq)] = value.slice(eq + 1);
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
    // `--out` with no endpoint is the package-only verb: an operator (or CI) that wants
    // the deployable bundle and nothing else. Any other run needs an endpoint.
    if (options.requireEndpoint !== false && !positional && !flags.endpoint && !flags.out) errors.push(`no endpoint — pass the runner URL as the argument, or --endpoint <url>, or ${ENDPOINT_ENV}=… (or use --out <path> to only build the bundle)`);
    // --teardown ships no bytes, so a shipping knob would be silently ignored.
    if (flags.teardownOnly) {
        const shipping = [
            flags.out && '--out',
            flags.noBuild && '--no-build',
            flags.artifact && '--artifact',
            flags.probePath && '--probe-path',
            flags.port !== DEFAULT_PORT && '--port',
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
    RUST_LOG: 'info',
    // Operator values LAST: `--env` is how RUST_LOG gets raised to debug for one deploy,
    // and a value that could not override the default would be a dead flag.
    ...flags.env,
});

/**
 * `runCmd` — the only line that names the entry point. The anonymous profile refuses to
 * guess it for an ARCHIVE (contract.rs), and the exec bit plus a flat member
 * name is what makes `./<member>` work from the stage dir.
 */
export const buildRunCmd = (flags: Pick<ProviderFlags, 'allowUnconfigured'>): string => `./${SERVER_MEMBER}${flags.allowUnconfigured ? ' --allow-unconfigured' : ''}`;

/**
 * The config the container receives, key for key. `probePath` is OMITTED unless asked
 * for, because its absence is the meaningful choice (TCP readiness ⇒ the container needs
 * no knowledge of this workload). Everything is forwarded to `normalizeRuntimeConfig`
 * downstream, so an invalid value fails here instead of as a 400 after the upload.
 */
export const buildPayloadConfig = (flags: Pick<ProviderFlags, 'port' | 'probePath' | 'env' | 'allowUnconfigured'>): RuntimeConfig => ({
    runCmd: buildRunCmd(flags),
    port: flags.port,
    artifactPath: SERVER_MEMBER,
    probeTimeoutSec: DEFAULT_PROBE_TIMEOUT_SEC,
    stopSignal: 'SIGTERM',
    ...(flags.probePath ? { probePath: flags.probePath } : {}),
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

/**
 * `PORT` in `config.env` would fight the runner: it FORCES `PORT` to the declared
 * `config.port` in the child environment, and `config.env` wins over that injection
 * (the Rust runtime-server `childEnv`, modal-sandbox-container.py:1895-1899
 * `_child_env`). So `--env PORT=9100`
 * --port 8800 boots a server the probe is watching on the wrong port. Reported, not
 * fatal: an operator who sets both numbers equal has said something sensible.
 */
export const portCollisionNotes = (flags: Pick<ProviderFlags, 'port' | 'env'>): string[] => {
    const declared = flags.env.PORT;
    if (declared === undefined || Number(declared) === flags.port) return [];
    return [`config.env PORT=${declared} overrides the runner's injected PORT=${flags.port}: the bin binds ${declared} while the container probes ${flags.port} — pass --port ${declared} instead of --env PORT=${declared}`];
};

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

/** What this run assembled — returned as data so a test (and start-daytona.ts) can assert it. */
export type ProviderPayload = {
    /** The bytes to POST, plus the name and the member table the client computed. */
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
        payload: { buffer: packaged.buffer, filename: packaged.filename, config: buildPayloadConfig(flags) },
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
    teardownFirst: flags.replace,
    noWait: !flags.wait,
    logsLines: flags.logsLines,
    dryRun: flags.dryRun,
    // The bundle's artifact member is the thing the container execs, so the client's
    // format guard applies (runtime-deploy.ts:796-836).
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
    line('DRY RUN — no probe, no DELETE, no POST, no cargo, no network');
    line(`endpoint:  ${endpointText ?? (flags.endpoint || '(none — --out only, nothing to deploy)')}`);
    line(`artifact:  ${built.artifact.path}  (${built.artifact.source}, ${built.artifact.sizeBytes} bytes, Linux ELF)`);
    line(`vault:     ${built.vaultPath}`);
    line(`bundle:    ${built.payload.filename}  (${built.bytes} bytes gzip, ${built.members.length} members, flat)`);
    for (const member of built.members) line(`             ${member.mode.toString(8).padStart(4, '0')}  ${member.name}  ${member.size} bytes`);
    line(`config (${Object.keys(config).length} keys, sent as the x-runtime-config header):`);
    line(`           ${JSON.stringify(config)}`);
    line(`           base64url: ${encodeConfigHeader(config)}`);
    line(`readiness: ${config.probePath ? `HTTP 2xx on ${config.probePath}` : 'TCP connect'} within ${config.probeTimeoutSec}s — client budget ${readyBudgetMs(config)} ms`);
    line(`slot:      ${flags.replace ? 'a busy slot is resolved first (DELETE /, or DELETE /sandbox on a Modal box)' : 'a busy slot is an error (--no-teardown)'}`);
    line(`auth:      ${flags.token ? `*** (length ${flags.token.length})` : '(no header — anonymous endpoint)'}`);
    for (const note of notes) line(`note:      ${note}`);
};

/**
 * Runs the whole verb and returns the process exit code (0 = serving / freed / plan
 * printed / bundle written). Never calls process.exit, so the wrappers and the tests can
 * observe it.
 */
export const runDeployEndpoint = async (flags: ProviderFlags, deps: ProviderDeps = createDefaultDeps()): Promise<number> => {
    const log = deps.log ?? ((text: string) => console.log(text));
    const error = deps.error ?? ((text: string) => console.error(text));
    const line = (text: string) => log(`${PREFIX} ${text}`);
    const writeFile = deps.writeFile ?? writeFileDefault;

    // ── --teardown: the slot, and nothing else. No cargo, no packaging. ──
    if (flags.teardownOnly) {
        const freed = await teardown(flags.endpoint, deps, { token: flags.token });
        line(`✓ slot free at ${flags.endpoint}${freed.replaced ? ` — ${JSON.stringify(freed.replaced)} is gone (the container stays up)` : ' (it was already empty)'}`);
        // `slot` is not a summary FIELD (buildSummary's shape is the client's), so the
        // post-teardown slot travels in notes, where a consumer of the JSON already reads it.
        log(
            buildSummary({
                mode: 'teardown',
                endpoint: flags.endpoint,
                status: 'ready',
                notes: [`slot now reads "${freed.slot}" — the container keeps serving and is ready for the next POST`],
                ...(flags.token ? { token: flags.token } : {}),
                ...(freed.replaced ? { replaced: freed.replaced } : {}),
            }),
        );
        return 0;
    }

    // ── assemble ──
    const built = await buildProviderPayload(flags, deps);
    const config = built.payload.config as RuntimeConfig;
    const notes = portCollisionNotes(flags);

    // `--out` is honored even on a dry run: writing a local file is not a container side
    // effect, and producing the bundle offline is half of what the flag is for.
    if (flags.out) {
        writeFile(resolveOutPath(flags.out), built.payload.buffer);
        line(`bundle written: ${resolveOutPath(flags.out)} (${built.bytes} bytes)`);
    }

    // No endpoint + --out is the package-only verb: the bundle is the deliverable.
    // `flags.out` is guaranteed non-empty here — parseArgs rejects "no endpoint and no
    // --out" (line ~345), and this module only ever reaches the network past this point.
    if (!flags.endpoint) {
        line('no endpoint given — the bundle was the deliverable, nothing was deployed');
        printPayloadPlan(flags, built, config, notes, log);
        // Same rule as every other path: the LAST line is one bare JSON summary, so
        // `tail -1 | jq` works on a package-only run too. 'planned' is the client's own
        // word for "assembled, nothing sent" (runtime-deploy.ts:1777-1787), and the
        // bundle's whereabouts travel in notes because buildSummary has no field for them.
        log(
            buildSummary({
                mode: 'deploy',
                // '' is the honest value: buildSummary always names the endpoint, and a
                // package-only run has none.
                endpoint: '',
                status: 'planned',
                port: flags.port,
                profile: 'anonymous',
                filename: built.payload.filename,
                notes: [
                    `bundle written to ${resolveOutPath(flags.out)} (${built.bytes} bytes, ${built.members.length} members)`,
                    'no endpoint — POST it later with `deploy:endpoint <url> --no-build`',
                ],
                ...(flags.token ? { token: flags.token } : {}),
            }),
        );
        return 0;
    }

    const result = await deployPayload(flags.endpoint, built.payload, deps, buildDeployOptions(flags));

    if (flags.dryRun) printPayloadPlan(flags, built, config, [...notes, ...result.notes], log);
    else if (!result.ok) {
        error(`${PREFIX} ✗ ${result.status}: ${result.error ?? 'the deploy did not become ready'}`);
        for (const note of result.notes) error(`${PREFIX} note: ${note}`);
        // The container's own log tail is the only view of WHY (bind failure, refused
        // vault document, ENOEXEC). The client attaches it; printing it is this module's job.
        if (result.logs) {
            error(`${PREFIX} ── GET /logs (${result.logs.split('\n').length} lines) ──`);
            for (const entry of result.logs.split('\n')) error(`${PREFIX} ${entry}`);
        }
        error(`${PREFIX} the slot may still hold this payload: retry with --teardown, or free it with deploy:endpoint ${flags.endpoint} --teardown`);
    } else {
        line(`✓ ${result.status} — slot ${result.slot}, port ${result.port}`);
        line(`  provider (clients): ${result.publicUrl ?? `not reported — ${result.notes[result.notes.length - 1] ?? 'the endpoint named no preview URL'}`}`);
        line(`  bin-only health:    GET ${result.publicUrl ?? flags.endpoint}/health  (the four contract routes are /providers/private/v1/*)`);
        line(`  free the slot:      yarn workspace @typecrate/provider deploy:endpoint ${flags.endpoint} --teardown`);
        for (const note of [...notes, ...result.notes]) line(`note: ${note}`);
    }

    // LAST line, bare: the prefixes are for humans, and a caller must be able to
    // `tail -1 | jq` this run (runtime-deploy.ts:1771-1773 states the same rule).
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
