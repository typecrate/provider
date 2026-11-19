// ─────────────────────────────────────────────────────────────────────────────
//  deploy-endpoint.test.ts — the provider's own deploy verb, end to end, with ZERO
//  network, ZERO cargo spawn and ZERO real filesystem.
//
//  Everything side-effectful in deploy-endpoint.ts arrives through `ProviderDeps`
//  (the shared client's RuntimeDeps — probe / runCommand / fs / env / log / error —
//  plus the writeFile seam), so these tests drive the REAL control flow against
//  scripted endpoint answers and assert exact values:
//    · parser      — every default, the positional endpoint, the env fallbacks and
//                    their precedence, the --env accumulation (values may contain `=`),
//                    each range check, each contradiction (--teardown + a shipping knob,
//                    two endpoints), and `--out` with no endpoint = package-only
//    · payload     — the FIXED config contract, key for key: no `probePath` unless asked
//                    for, `./typecrate-provider-server` as runCmd, the two env defaults
//                    and --env overriding them, and the two members WITH their modes —
//                    read back OUT of the real gzip'd tar with the client's own reader
//    · cargo       — the client's exact argv (no `--bin`: the package target builds the
//                    bin), that a dry run spawns NOTHING, and that it resolves the
//                    artifact from the repo-root .cargo/config.toml target-dir instead
//    · vault       — a missing or empty artifact fails with the crate's regeneration
//                    command and WITHOUT a probe: no deploy is attempted with a payload
//                    that cannot authenticate itself
//    · flow        — a dry run touches no network but prints the members + the config and
//                    ends with the machine summary; a real run preflights /health +
//                    /status, resolves a busy slot with DELETE /, POSTs with NO
//                    x-runtime-profile and the config header byte-identical to
//                    encodeConfigHeader, waits, and prints the summary LAST; --out writes
//                    bytes without deploying and anchors a RELATIVE value on the REPO root,
//                    never on the cwd; --teardown ships nothing
//    · entry       — the guard's three rules, including that start-modal.ts importing
//                    this verb must NOT fire this verb's guard
//
//  Nothing asserts a range where an exact value is knowable: the member tables, the
//  header sets, the probe sequences and the byte counts below are compared to literals.
// ─────────────────────────────────────────────────────────────────────────────

import { join, resolve } from 'node:path';
import { describe, expect, it, vi } from 'vitest';

import { DEFAULT_LOG_LINES, encodeConfigHeader, readTarBundle } from '@services/daytona/sandbox';
import type { FsAccess, Probe, ProbeResult } from '@services/daytona/sandbox';

import {
    DEFAULT_PORT,
    DEFAULT_PROBE_TIMEOUT_SEC,
    DEFAULT_VAULT_PATH,
    ENDPOINT_ENV,
    PACKAGE_ROOT,
    PAYLOAD_FILENAME,
    PREFIX,
    REPO_ROOT,
    SERVER_MEMBER,
    TOKEN_ENV,
    VAULT_MEMBER,
    VAULT_REGENERATION_COMMAND,
    USAGE,
    type ProviderDeps,
    type ProviderFlags,
    buildDeployOptions,
    buildPayloadConfig,
    buildPayloadEntries,
    buildPayloadEnv,
    buildProviderPayload,
    buildRunCmd,
    main,
    parseArgs,
    portCollisionNotes,
    resolveOutPath,
    resolveVaultPath,
    runDeployEndpoint,
} from './deploy-endpoint';
import { isCliEntryPoint } from './entry-guard';

// ══════════════════════════════════════════════════════════════════════════════
//  FAKES — the whole world this verb touches: a file table, a cargo recorder, a
//  script of endpoint answers, and a writer that keeps the bytes in memory
// ══════════════════════════════════════════════════════════════════════════════

/** A Linux ELF: the magic plus a body. Enough for every guard on the path. */
const ELF = Buffer.concat([Buffer.from([0x7f, 0x45, 0x4c, 0x46]), Buffer.from('provider-server-image', 'utf8')]);
const VAULT_DOC = '{"consumers":["agentic-provider"],"keys":["sk-test"]}';
const VAULT_BYTES = Buffer.byteLength(VAULT_DOC, 'utf8');

/** The repo-root cargo config, exactly as the real one pins the artifact location. */
const CARGO_CONFIG = '[build]\ntarget-dir = ".rust/target"\n';
/** The artifact path the client DERIVES from that config — what a dry run must find. */
const CARGO_TARGET_BIN = join(REPO_ROOT, '.rust', 'target', 'release', SERVER_MEMBER);
/** The vault artifact this package ships by default (the same path the crate bakes). */
const VAULT_PATH = DEFAULT_VAULT_PATH;

/** An in-memory filesystem: the only files a provider deploy ever reads. */
const fakeFs = (files: Record<string, string | Buffer>): FsAccess => {
    const table = new Map<string, Buffer>();
    for (const [path, data] of Object.entries(files)) table.set(resolve(path), Buffer.isBuffer(data) ? data : Buffer.from(data, 'utf8'));
    const bytesOf = (path: string): Buffer | undefined => table.get(resolve(path));
    return {
        exists: (path) => table.has(resolve(path)),
        read: (path) => {
            const data = bytesOf(path);
            if (!data) throw new Error(`fake fs: no such file ${path}`);
            return data;
        },
        readHead: (path, bytes) => (bytesOf(path) ?? Buffer.alloc(0)).subarray(0, bytes),
        stat: (path) => {
            const data = bytesOf(path);
            return data ? { size: data.length, isFile: true } : null;
        },
    };
};

/** The file set a healthy host has: the built ELF, the vault artifact, the cargo config. */
const healthyFs = (): FsAccess => fakeFs({ [CARGO_TARGET_BIN]: ELF, [VAULT_PATH]: VAULT_DOC, [join(REPO_ROOT, '.cargo', 'config.toml')]: CARGO_CONFIG });

/** cargo's NDJSON answer for one release bin (the shape `parseCargoArtifact` reads). */
const cargoJson = (path: string): string => `${JSON.stringify({ reason: 'compiler-artifact', target: { name: SERVER_MEMBER, kind: ['bin'] }, executable: path, profile: { release: true } })}\n`;

/** A cargo recorder: it answers success and keeps EVERY argv it was handed. */
const fakeCargo = (stdout = cargoJson(CARGO_TARGET_BIN)) => {
    const calls: Array<{ command: string; args: readonly string[]; cwd: string }> = [];
    const runCommand = (command: string, args: readonly string[], options: { cwd: string }): { status: number; stdout: string; stderr: string } => {
        calls.push({ command, args, cwd: options.cwd });
        return { status: 0, stdout, stderr: '' };
    };
    return { calls, runCommand };
};

const OK = (json: unknown): ProbeResult => ({ status: 200, ok: true, json, text: JSON.stringify(json) });
const ACCEPTED = (json: unknown): ProbeResult => ({ status: 202, ok: true, json, text: JSON.stringify(json) });
const REMOVED = (): ProbeResult => OK({ tornDown: true });

/** The runner's /health, and the three /states a deploy meets. */
const HEALTH = { ok: true, slot: 'empty', uptimeSec: 5, contract: 'daytona-runtime-v1', runnerPort: 7777, authenticated: false, exposedPorts: [7777] };
const STATUS_EMPTY = { slot: 'empty', runtime: null };
const STATUS_BUSY = { slot: 'ready', runtime: { profile: 'generic', port: 8080, ready: true, runtimeId: 'rt-old', preview: { url: 'https://box.modal.run' } } };
const STATUS_READY = { slot: 'ready', runtime: { profile: 'generic', port: DEFAULT_PORT, ready: true, runtimeId: 'rt-new', preview: { url: 'https://box.modal.run' } } };

/**
 * A scripted endpoint: each call must be in the script, in order per method+URL, and an
 * unscripted call THROWS — a verb that asks somewhere nobody promised is a bug, and a
 * hang is a worse test failure than an exception. `calls` is the request log read back.
 */
const scriptedProbe = (script: Array<[method: string, url: string, reply: ProbeResult]>) => {
    const queues = new Map<string, ProbeResult[]>();
    for (const [method, url, reply] of script) {
        const key = `${method} ${url}`;
        queues.set(key, [...(queues.get(key) ?? []), reply]);
    }
    const calls: Array<{ method: string; url: string; headers: Record<string, string>; bodyBytes: number }> = [];
    const probe: Probe = async (method, url, init) => {
        calls.push({ method, url, headers: init.headers, bodyBytes: init.body === undefined ? 0 : (init.body as Buffer).length });
        const pending = queues.get(`${method} ${url}`);
        if (!pending || pending.length === 0) throw new Error(`unscripted probe: ${method} ${url}`);
        return pending.shift()!;
    };
    return { probe, calls };
};

/** The sinks: every line the verb prints, in order. */
const fakeSinks = () => {
    const out: string[] = [];
    const err: string[] = [];
    return { out, err, log: (line: string) => out.push(line), error: (line: string) => err.push(line) };
};

/** Deps over a healthy host: in-memory fs, recorded cargo, no clock, no sleep, no disk. */
const hostDeps = (probe: Probe, overrides: Partial<ProviderDeps> = {}) => {
    const sinks = fakeSinks();
    const cargo = fakeCargo();
    const written: Array<{ path: string; buffer: Buffer }> = [];
    const deps: ProviderDeps = {
        probe,
        fs: healthyFs(),
        runCommand: cargo.runCommand,
        // An explicit empty env keeps an operator's ambient TYPECRATE_PROVIDER_ENDPOINT
        // out of the tests (main() reads deps.env when it is set).
        env: {},
        log: sinks.log,
        error: sinks.error,
        sleep: async () => undefined,
        writeFile: (path, buffer) => written.push({ path, buffer }),
        ...overrides,
    };
    return { deps, sinks, cargo, written };
};

/** Flags for the flow tests: the verb's own parser, so the flags under test are real. */
const flagsFor = (argv: readonly string[]): ProviderFlags => parseArgs(argv, {}).flags;

/**
 * Runs `run` with `process.cwd()` pointed somewhere OTHER than the repo root.
 *
 * WHY THIS EXISTS: the bug this file pins is that a RELATIVE `--out Temp/x.tar.gz` used
 * to resolve against the cwd, and vitest happens to run with the cwd AT the repo root —
 * so an expectation written as `resolve('Temp/x.tar.gz')` is the same string either way
 * and cannot fail. The real shape of the bug is cwd = the PACKAGE directory, which is
 * where `yarn workspace @typecrate/provider deploy:endpoint` starts (root AGENTS.md,
 * "Scratch Space — use repo-root `Temp/`"): there, cwd-relative resolution lands
 * `packages/typecrate/provider/Temp/`. Faking the cwd makes the two behaviours produce
 * different strings, which is the only way these tests can tell them apart.
 * `node:path`'s `resolve()` reads `process.cwd()` per call, so the spy reaches it, and
 * the restore is in a `finally` so a throwing test cannot leak the fake cwd into others.
 */
const withCwd = async <T>(cwd: string, run: () => T | Promise<T>): Promise<T> => {
    const spy = vi.spyOn(process, 'cwd').mockImplementation(() => cwd);
    try {
        return await run();
    } finally {
        spy.mockRestore();
    }
};

/** The last printed line: it MUST be the single machine-readable JSON object. */
const lastJson = (lines: string[]): Record<string, unknown> => JSON.parse(lines[lines.length - 1]!) as Record<string, unknown>;

/**
 * The canonical WIRE form of the fixed config. The client re-serialises config through
 * its own defaults before encoding the header (runtime-deploy.ts `buildConfigHeader`), so
 * the key ORDER on the wire is the client's, not the declaration order in
 * `buildPayloadConfig`. This string is that canonical text, pinned so a change in either
 * serialiser surfaces here; the SEMANTIC contract (key for key) is pinned independently by
 * the decoded-header assertion in the deploy test below.
 */
const CANONICAL_CONFIG_JSON = '{"runCmd":"./typecrate-provider-server","stopSignal":"SIGTERM","port":8800,"probeTimeoutSec":120,"artifactPath":"typecrate-provider-server","env":{"TYPECRATE_PROVIDER_VAULT_PATH":"./agentic-provider.json","RUST_LOG":"info"}}';

/** The exact dry-run note list the shared client produces for THIS config. */
const plannedNotes = (endpoint: string, bytes: number): string[] => [
    // The client's format guard runs before anything is sent, and its note leads the list.
    'format guard: "typecrate-provider-server" is a Linux ELF',
    'dry run — no probe, no DELETE, no POST, no cargo',
    `POST ${endpoint}/ — ${bytes} bytes as "typecrate-provider.tar.gz", profile anonymous`,
    // The header note carries the base64url AND the inline JSON of the same object.
    `x-runtime-config: ${encodeConfigHeader(JSON.parse(CANONICAL_CONFIG_JSON) as object)} (base64url JSON of ${CANONICAL_CONFIG_JSON})`,
    // No token in this run, so the client says the header was not sent at all.
    'auth: (not sent — anonymous endpoint)',
    // probeTimeoutSec 120 + the client's own 120 s slack, no build step: 240 000 ms.
    'readiness budget: 240000 ms derived from the payload\'s own timeouts',
    'slot policy: a busy slot is resolved first (DELETE /, or DELETE /sandbox on a Modal box)',
];

// ══════════════════════════════════════════════════════════════════════════════
//  PARSER
// ══════════════════════════════════════════════════════════════════════════════

describe('parseArgs', () => {
    it('starts from the provider payload contract, not from a neutral default', () => {
        const { flags, errors } = parseArgs(['https://box.modal.run']);
        expect(errors).toEqual([]);
        expect(flags).toEqual({
            help: false,
            dryRun: false,
            teardownOnly: false,
            replace: true,
            endpoint: 'https://box.modal.run',
            token: '',
            port: 8800,
            probePath: '',
            env: {},
            vault: join(REPO_ROOT, 'config', 'vault', 'generated', 'agentic-provider.json'),
            out: '',
            noBuild: false,
            artifact: '',
            wait: true,
            logsLines: 40,
            allowUnconfigured: false,
        });
        // The pins that must never drift from the crate are stated once, here.
        expect(DEFAULT_PORT).toBe(8800);
        expect(DEFAULT_PROBE_TIMEOUT_SEC).toBe(120);
        expect(DEFAULT_LOG_LINES).toBe(40);
        expect(DEFAULT_VAULT_PATH).toBe(join(REPO_ROOT, 'config', 'vault', 'generated', 'agentic-provider.json'));
        expect(PACKAGE_ROOT).toBe(join(REPO_ROOT, 'packages', 'typecrate', 'provider'));
    });

    it('reads the endpoint and the token from the documented environment', () => {
        const { flags, errors } = parseArgs([], { [ENDPOINT_ENV]: 'https://env.modal.run', [TOKEN_ENV]: 's3cret' });
        expect(errors).toEqual([]);
        expect(flags.endpoint).toBe('https://env.modal.run');
        expect(flags.token).toBe('s3cret');
        expect(ENDPOINT_ENV).toBe('TYPECRATE_PROVIDER_ENDPOINT');
        expect(TOKEN_ENV).toBe('TYPECRATE_PROVIDER_ENDPOINT_TOKEN');
    });

    it('lets the command line beat the environment', () => {
        const { flags } = parseArgs(['https://arg.modal.run'], { [ENDPOINT_ENV]: 'https://env.modal.run' });
        expect(flags.endpoint).toBe('https://arg.modal.run');
    });

    it('accumulates --env and keeps everything after the first = in the value', () => {
        const { flags, errors } = parseArgs(['https://x', '--env', 'RUST_LOG=debug', '--env', 'DSN=url://a=b']);
        expect(errors).toEqual([]);
        expect(flags.env).toEqual({ RUST_LOG: 'debug', DSN: 'url://a=b' });
    });

    it('reports each bad token instead of throwing', () => {
        expect(parseArgs(['https://x', '--nope']).errors).toEqual(['unknown flag "--nope"']);
        expect(parseArgs(['https://x', 'https://y']).errors).toEqual(['unexpected extra argument "https://y" — the endpoint is the only positional (got "https://x" already)']);
        expect(parseArgs(['https://x', '--port', '70000']).errors).toEqual(['--port "70000" is out of range (TCP port 1-65535)']);
        expect(parseArgs(['https://x', '--logs', '0']).errors).toEqual(['--logs "0" is out of range (1-1000 lines)']);
        expect(parseArgs(['https://x', '--probe-path', 'health']).errors).toEqual(['--probe-path must start with "/" (or omit it for TCP-connect readiness), got "health"']);
        expect(parseArgs(['https://x', '--env', 'NOEQUALS']).errors).toEqual(['--env expects KEY=VALUE, got "NOEQUALS"']);
        expect(parseArgs(['https://x', '--token']).errors).toEqual(['--token needs a value']);
    });

    it('refuses two endpoints that disagree, and accepts the same one twice', () => {
        expect(parseArgs(['https://a', '--endpoint', 'https://b']).errors).toEqual(['two different endpoints given: "https://a" and --endpoint "https://b"']);
        expect(parseArgs(['https://a', '--endpoint', 'https://a']).errors).toEqual([]);
    });

    it('lets the command line beat TYPECRATE_PROVIDER_ENDPOINT, which is only a default', () => {
        // The env var seeds flags.endpoint, so it MUST NOT take part in the disagreement
        // check: `start:modal https://second--box.modal.run` with the repo's box in the
        // environment is the documented way to aim at another box, not a contradiction.
        const positional = parseArgs(['https://second--box.modal.run'], { [ENDPOINT_ENV]: 'https://repo-box.modal.run' });
        expect(positional.errors).toEqual([]);
        expect(positional.flags.endpoint).toBe('https://second--box.modal.run');
        expect(parseArgs(['--endpoint', 'https://flag.run'], { [ENDPOINT_ENV]: 'https://repo-box.modal.run' }).flags.endpoint).toBe('https://flag.run');
        // An explicit pair that disagrees is still an error, env or no env.
        expect(parseArgs(['https://a.run', '--endpoint', 'https://b.run'], { [ENDPOINT_ENV]: 'https://a.run' }).errors).toEqual(['two different endpoints given: "https://a.run" and --endpoint "https://b.run"']);
    });

    it('refuses a --teardown that also carries shipping knobs', () => {
        expect(parseArgs(['https://x', '--teardown', '--port', '9100', '--env', 'A=1']).errors).toEqual(['--teardown ships nothing, so --port / --env do not apply (drop them, or drop --teardown)']);
        expect(parseArgs(['https://x', '--teardown']).errors).toEqual([]);
    });

    it('treats --out with no endpoint as the package-only verb', () => {
        const { flags, errors } = parseArgs(['--out', 'Temp/out.tar.gz']);
        expect(errors).toEqual([]);
        expect(flags.endpoint).toBe('');
        expect(flags.out).toBe('Temp/out.tar.gz');
    });

    it('names the fix when no endpoint was given at all', () => {
        expect(parseArgs([]).errors).toEqual([`no endpoint — pass the runner URL as the argument, or --endpoint <url>, or ${ENDPOINT_ENV}=… (or use --out <path> to only build the bundle)`]);
    });

    it('lets --help win over a broken command line', () => {
        const { flags, errors } = parseArgs(['--nope', '--help']);
        expect(flags.help).toBe(true);
        // main() prints USAGE and returns 0 without ever looking at these.
        expect(errors).toEqual(['unknown flag "--nope"']);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  THE PAYLOAD — the fixed contract, asserted as one literal per shape
// ══════════════════════════════════════════════════════════════════════════════

describe('buildPayloadConfig', () => {
    it('is exactly the anonymous payload the container contract asks for', () => {
        const config = buildPayloadConfig(flagsFor(['https://x']));
        expect(config).toEqual({
            runCmd: './typecrate-provider-server',
            port: 8800,
            artifactPath: 'typecrate-provider-server',
            probeTimeoutSec: 120,
            stopSignal: 'SIGTERM',
            env: { TYPECRATE_PROVIDER_VAULT_PATH: './agentic-provider.json', RUST_LOG: 'info' },
        });
        // `probePath` is ABSENT, not empty: its absence IS the TCP-readiness choice.
        expect(Object.keys(config)).not.toContain('probePath');
    });

    it('adds probePath only when the operator opts into HTTP readiness', () => {
        expect(buildPayloadConfig(flagsFor(['https://x', '--probe-path', '/health']))).toEqual({
            runCmd: './typecrate-provider-server',
            port: 8800,
            artifactPath: 'typecrate-provider-server',
            probeTimeoutSec: 120,
            stopSignal: 'SIGTERM',
            probePath: '/health',
            env: { TYPECRATE_PROVIDER_VAULT_PATH: './agentic-provider.json', RUST_LOG: 'info' },
        });
    });

    it('lets --env override the two defaults, and only those keys', () => {
        expect(buildPayloadEnv(flagsFor(['https://x', '--env', 'RUST_LOG=debug', '--env', 'TYPECRATE_PROVIDER_HOST=127.0.0.1']))).toEqual({
            TYPECRATE_PROVIDER_VAULT_PATH: './agentic-provider.json',
            RUST_LOG: 'debug',
            TYPECRATE_PROVIDER_HOST: '127.0.0.1',
        });
    });

    it('appends the bin flag to runCmd only when --allow-unconfigured is asked for', () => {
        expect(buildRunCmd(flagsFor(['https://x']))).toBe('./typecrate-provider-server');
        expect(buildRunCmd(flagsFor(['https://x', '--allow-unconfigured']))).toBe('./typecrate-provider-server --allow-unconfigured');
    });

    it('reports a config.env PORT that would fight the runner-injected one', () => {
        expect(portCollisionNotes(flagsFor(['https://x', '--env', 'PORT=9100']))).toEqual(["config.env PORT=9100 overrides the runner's injected PORT=8800: the bin binds 9100 while the container probes 8800 — pass --port 9100 instead of --env PORT=9100"]);
        expect(portCollisionNotes(flagsFor(['https://x', '--env', 'PORT=8800']))).toEqual([]);
        expect(portCollisionNotes(flagsFor(['https://x']))).toEqual([]);
    });
});

describe('buildPayloadEntries', () => {
    it('is two flat members: an exec bit for the bin, 0644 for the document', () => {
        expect(buildPayloadEntries('/abs/typecrate-provider-server', '/abs/agentic-provider.json')).toEqual([
            { name: 'typecrate-provider-server', path: '/abs/typecrate-provider-server', mode: 0o755 },
            { name: 'agentic-provider.json', path: '/abs/agentic-provider.json', mode: 0o644 },
        ]);
        // The names are the contract: runCmd and artifactPath are written against them.
        expect(SERVER_MEMBER).toBe('typecrate-provider-server');
        expect(VAULT_MEMBER).toBe('agentic-provider.json');
        expect(PAYLOAD_FILENAME).toBe('typecrate-provider.tar.gz');
    });
});

describe('resolveOutPath', () => {
    // The fake repo root below is deliberately NOT a real path: the function is pure
    // string work, and a literal fake keeps the expectation an exact literal too.
    it('anchors a RELATIVE value on the repo root it is given — never on the process cwd', async () => {
        // The cwd spy is what makes this a regression test rather than a tautology: with
        // the pre-fix `resolve(value)` this returns '/fake/cwd/Temp/out.tar.gz'.
        await withCwd('/fake/cwd', () => {
            expect(resolveOutPath('Temp/out.tar.gz', '/fake/repo')).toBe('/fake/repo/Temp/out.tar.gz');
        });
    });

    it('returns an ABSOLUTE value verbatim — the repo root is never prepended to someone else\'s path', async () => {
        // CI artifact dirs and `/tmp` stay reachable verbatim; a `.`-prefixed value is a
        // RELATIVE spelling, so it normalises into the repo root instead.
        await withCwd('/fake/cwd', () => {
            expect(resolveOutPath('/tmp/out.tar.gz', '/fake/repo')).toBe('/tmp/out.tar.gz');
            expect(resolveOutPath('./Temp/out.tar.gz', '/fake/repo')).toBe('/fake/repo/Temp/out.tar.gz');
        });
    });

    it("uses THIS repo root by default, so the verb cannot land scratch inside the package", async () => {
        // The shipped default, checked from the one cwd that actually breaks in practice:
        // `yarn workspace @typecrate/provider deploy:endpoint` starts in the PACKAGE
        // directory, and root AGENTS.md forbids `packages/typecrate/provider/Temp/`.
        await withCwd(PACKAGE_ROOT, () => {
            expect(resolveOutPath('Temp/out.tar.gz')).toBe(join(REPO_ROOT, 'Temp', 'out.tar.gz'));
            expect(resolveOutPath('Temp/out.tar.gz')).not.toBe(join(PACKAGE_ROOT, 'Temp', 'out.tar.gz'));
        });
    });
});

describe('resolveVaultPath', () => {
    it('accepts the artifact a healthy host has', () => {
        const deps: ProviderDeps = { probe: async () => OK({}), fs: healthyFs() };
        expect(resolveVaultPath(flagsFor(['https://x']), deps)).toBe(VAULT_PATH);
    });

    it('refuses to deploy a payload that cannot authenticate itself, and names the regeneration command', () => {
        const deps: ProviderDeps = { probe: async () => OK({}), fs: fakeFs({ [CARGO_TARGET_BIN]: ELF }) };
        let message = '';
        try {
            resolveVaultPath(flagsFor(['https://x']), deps);
        } catch (failure) {
            message = (failure as Error).message;
        }
        expect(message).toBe(
            `the credentials file is not a readable non-empty file: ${VAULT_PATH}\n` +
                '  it is git-ignored and generated, so a fresh checkout does not have it. Regenerate it with:\n' +
                `  ${VAULT_REGENERATION_COMMAND}\n` +
                '  (or point --vault at a document you already have — its contents are shipped inside the bundle, never into config.env)',
        );
        expect(VAULT_REGENERATION_COMMAND).toBe('yarn workspace @config/vault export -- --consumer agentic-provider');
    });

    it('refuses an EMPTY artifact the same way — a 0-byte document is a refused vault, not an empty one', () => {
        const deps: ProviderDeps = { probe: async () => OK({}), fs: fakeFs({ [VAULT_PATH]: '' }) };
        expect(() => resolveVaultPath(flagsFor(['https://x']), deps)).toThrow(/not a readable non-empty file/);
    });

    it('honours --vault', () => {
        const other = join(REPO_ROOT, 'Temp', 'other-vault.json');
        const deps: ProviderDeps = { probe: async () => OK({}), fs: fakeFs({ [other]: VAULT_DOC }) };
        expect(resolveVaultPath(flagsFor(['https://x', '--vault', other]), deps)).toBe(other);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  ASSEMBLY — cargo, then the vault, then the tar.gz the container will exec
// ══════════════════════════════════════════════════════════════════════════════

describe('buildProviderPayload', () => {
    it("runs the client's pinned cargo recipe from the package root, and reads its NDJSON path", async () => {
        const { probe } = scriptedProbe([]);
        const { deps, cargo } = hostDeps(probe);
        const built = await buildProviderPayload(flagsFor(['https://box.modal.run']), deps);
        expect(cargo.calls).toEqual([
            {
                command: 'cargo',
                // The client's own argv: `--bin` is NOT in it (the package target builds
                // the bin), and --locked is, because Cargo.lock is committed.
                args: ['build', '--release', '--locked', '-p', 'typecrate-provider', '--message-format=json'],
                cwd: PACKAGE_ROOT,
            },
        ]);
        expect(built.artifact).toEqual({ path: CARGO_TARGET_BIN, sizeBytes: ELF.length, source: 'cargo-json' });
    });

    it('never spawns cargo on a dry run, and resolves the artifact from the repo-root target-dir instead', async () => {
        const { probe } = scriptedProbe([]);
        const { deps, cargo } = hostDeps(probe);
        const built = await buildProviderPayload(flagsFor(['https://box.modal.run', '--dry-run']), deps);
        expect(cargo.calls).toEqual([]);
        expect(built.artifact).toEqual({ path: CARGO_TARGET_BIN, sizeBytes: ELF.length, source: 'candidate' });
    });

    it('never spawns cargo with --artifact, and ships exactly that file', async () => {
        const explicit = join(REPO_ROOT, '.rust', 'target', 'release', 'elsewhere-server');
        const { probe } = scriptedProbe([]);
        const { deps, cargo } = hostDeps(probe, { fs: fakeFs({ [explicit]: ELF, [VAULT_PATH]: VAULT_DOC }) });
        const built = await buildProviderPayload(flagsFor(['https://x', '--artifact', explicit]), deps);
        expect(cargo.calls).toEqual([]);
        expect(built.artifact).toEqual({ path: explicit, sizeBytes: ELF.length, source: 'explicit' });
    });

    it('writes exactly the two members, with their modes, inside one gzip — read back with the reader\'s own parser', async () => {
        const { probe } = scriptedProbe([]);
        const { deps } = hostDeps(probe);
        const built = await buildProviderPayload(flagsFor(['https://x', '--dry-run']), deps);
        expect(built.payload.filename).toBe('typecrate-provider.tar.gz');
        expect(built.members).toEqual([
            { name: 'typecrate-provider-server', size: ELF.length, mode: 0o755 },
            { name: 'agentic-provider.json', size: VAULT_BYTES, mode: 0o644 },
        ]);
        // Read back from the bytes, not trusted from the inputs: names, sizes and the exec
        // bit are what the container sees in its stage dir.
        expect(readTarBundle(built.payload.buffer).map((member) => ({ name: member.name, size: member.size, mode: member.mode }))).toEqual([
            { name: 'typecrate-provider-server', size: ELF.length, mode: 0o755 },
            { name: 'agentic-provider.json', size: VAULT_BYTES, mode: 0o644 },
        ]);
        expect(built.bytes).toBe(built.payload.buffer.length);
        // The gzip mtime is pinned, so one payload hashes to one digest — a re-deploy is
        // checkable, and this is the cheapest place to prove it.
        const again = await buildProviderPayload(flagsFor(['https://x', '--dry-run']), hostDeps(probe).deps);
        expect(again.payload.buffer.equals(built.payload.buffer)).toBe(true);
    });

    it('carries the config alongside the bytes', async () => {
        const { probe } = scriptedProbe([]);
        const { deps } = hostDeps(probe);
        const built = await buildProviderPayload(flagsFor(['https://x', '--dry-run', '--port', '9000']), deps);
        expect(built.payload.config).toEqual({
            runCmd: './typecrate-provider-server',
            port: 9000,
            artifactPath: 'typecrate-provider-server',
            probeTimeoutSec: 120,
            stopSignal: 'SIGTERM',
            env: { TYPECRATE_PROVIDER_VAULT_PATH: './agentic-provider.json', RUST_LOG: 'info' },
        });
    });

    it('refuses to assemble when the vault document is missing — before any probe', async () => {
        const { probe, calls } = scriptedProbe([]);
        const { deps, sinks } = hostDeps(probe, { fs: fakeFs({ [CARGO_TARGET_BIN]: ELF, [join(REPO_ROOT, '.cargo', 'config.toml')]: CARGO_CONFIG }) });
        expect(await main(['https://box.modal.run', '--dry-run'], deps)).toBe(1);
        expect(sinks.err.join('\n')).toContain(VAULT_REGENERATION_COMMAND);
        expect(calls).toEqual([]);
    });

    it('names the build verb when the artifact is not there', async () => {
        const { probe } = scriptedProbe([]);
        const { deps, sinks } = hostDeps(probe, { fs: fakeFs({ [VAULT_PATH]: VAULT_DOC }) });
        expect(await main(['https://box.modal.run', '--dry-run'], deps)).toBe(1);
        const printed = sinks.err.join('\n');
        expect(printed).toContain('no release binary "typecrate-provider-server" was found');
        expect(printed).toContain('yarn workspace @typecrate/provider build:server');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  DEPLOY OPTIONS — the flag mapping, pinned separately because a drift here changes
//  what the CONTAINER does (a busy slot replaced or refused, wait or not)
// ══════════════════════════════════════════════════════════════════════════════

describe('buildDeployOptions', () => {
    it('maps the verbs onto the client options', () => {
        expect(buildDeployOptions(flagsFor(['https://x', '--token', 't0k3n', '--logs', '200']))).toEqual({
            token: 't0k3n',
            teardownFirst: true,
            noWait: false,
            logsLines: 200,
            dryRun: false,
            expectElf: true,
            // The summary is printed by this module, LAST, after the operator notes.
            printSummary: false,
        });
        expect(buildDeployOptions(flagsFor(['https://x', '--no-teardown', '--no-wait', '--dry-run']))).toEqual({
            token: '',
            teardownFirst: false,
            noWait: true,
            logsLines: DEFAULT_LOG_LINES,
            dryRun: true,
            expectElf: true,
            printSummary: false,
        });
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  THE VERB
// ══════════════════════════════════════════════════════════════════════════════

describe('runDeployEndpoint — dry run', () => {
    it('touches NOTHING and prints the assembled payload, the config and the summary', async () => {
        const url = 'https://box.modal.run';
        const { probe, calls } = scriptedProbe([]);
        const { deps, sinks, cargo } = hostDeps(probe);
        // The byte count is measured from the same fake host the verb reads, so the note
        // is asserted against a real number rather than a guessed one. The warm-up call is
        // itself a DRY RUN, otherwise it would compile (record a cargo call) and poison the
        // "a dry run spawns nothing" assertion below.
        const built = await buildProviderPayload(flagsFor([url, '--dry-run']), deps);

        expect(await runDeployEndpoint(flagsFor([url, '--dry-run']), deps)).toBe(0);
        expect(calls).toEqual([]);
        expect(cargo.calls).toEqual([]);

        const printed = sinks.out.join('\n');
        expect(printed).toContain('DRY RUN — no probe, no DELETE, no POST, no cargo, no network');
        expect(printed).toContain(`0755  typecrate-provider-server  ${ELF.length} bytes`);
        expect(printed).toContain(`0644  agentic-provider.json  ${VAULT_BYTES} bytes`);
        expect(printed).toContain('{"runCmd":"./typecrate-provider-server","port":8800,"artifactPath":"typecrate-provider-server","probeTimeoutSec":120,"stopSignal":"SIGTERM","env":{"TYPECRATE_PROVIDER_VAULT_PATH":"./agentic-provider.json","RUST_LOG":"info"}}');
        expect(printed).toContain(`bundle:    typecrate-provider.tar.gz  (${built.bytes} bytes gzip, 2 members, flat)`);

        expect(lastJson(sinks.out)).toEqual({
            mode: 'deploy',
            endpoint: url,
            status: 'planned',
            publicUrl: null,
            port: 8800,
            profile: 'anonymous',
            filename: 'typecrate-provider.tar.gz',
            notes: plannedNotes(url, built.bytes),
        });
    });

    it('still writes the bundle with --out, because a host-side write is not a deploy', async () => {
        const { probe } = scriptedProbe([]);
        const { deps, sinks, written } = hostDeps(probe);
        // cwd = the PACKAGE directory: the exact place `yarn workspace @typecrate/provider
        // deploy:endpoint` starts, and the reason a relative `--out` used to drop scratch
        // into the package tree. The expected path below is the LITERAL repo-root answer,
        // so a regression to cwd resolution fails here (it would yield
        // `${PACKAGE_ROOT}/Temp/out.tar.gz`) instead of matching a recomputed resolve().
        await withCwd(PACKAGE_ROOT, async () => {
            expect(await runDeployEndpoint(flagsFor(['https://box.modal.run', '--dry-run', '--out', 'Temp/out.tar.gz']), deps)).toBe(0);
        });
        expect(written.length).toBe(1);
        expect(written[0]!.path).toBe(join(REPO_ROOT, 'Temp', 'out.tar.gz'));
        // The path the verb PRINTS is the path it WROTE: one resolution, used twice, so a
        // note can never promise a file that was not created. (`line()` prefixes PREFIX.)
        expect(sinks.out).toContain(`${PREFIX} bundle written: ${join(REPO_ROOT, 'Temp', 'out.tar.gz')} (${written[0]!.buffer.length} bytes)`);
        // gzip magic: a bundle the runner extracts flat, not a single staged file.
        expect([...(written[0]!.buffer.subarray(0, 2))]).toEqual([0x1f, 0x8b]);
    });

    it('packages without an endpoint at all when --out is the deliverable', async () => {
        const { probe, calls } = scriptedProbe([]);
        const { deps, sinks, written, cargo } = hostDeps(probe);
        const built = await buildProviderPayload(flagsFor(['--out', 'Temp/out.tar.gz', '--dry-run']), deps);
        // Same cwd trap as the test above, and this path is the one the README runbook
        // spells (`--out Temp/provider.tar.gz`), so the summary note must name the
        // repo-root file the write actually created — not a cwd-relative string that would
        // point a human at a file that does not exist.
        await withCwd(PACKAGE_ROOT, async () => {
            expect(await runDeployEndpoint(flagsFor(['--out', 'Temp/out.tar.gz']), deps)).toBe(0);
        });
        expect(calls).toEqual([]);
        // A real (non-dry) package-only run DOES compile — that is the client's pinned
        // recipe — and still never touches the network: no endpoint means nothing to probe.
        expect(cargo.calls.map((call) => `${call.command} ${call.args.join(' ')} (cwd ${call.cwd})`)).toEqual([
            `cargo build --release --locked -p typecrate-provider --message-format=json (cwd ${PACKAGE_ROOT})`,
        ]);
        expect(written.length).toBe(1);
        expect(written[0]!.path).toBe(join(REPO_ROOT, 'Temp', 'out.tar.gz'));
        expect(sinks.out.join('\n')).toContain('no endpoint given — the bundle was the deliverable, nothing was deployed');
        // The LAST line is one bare JSON summary on EVERY path, package-only included:
        // `tail -1 | jq` is the machine contract this verb promises.
        expect(lastJson(sinks.out)).toEqual({
            mode: 'deploy',
            endpoint: '',
            status: 'planned',
            publicUrl: null,
            port: 8800,
            profile: 'anonymous',
            filename: 'typecrate-provider.tar.gz',
            notes: [
                `bundle written to ${join(REPO_ROOT, 'Temp', 'out.tar.gz')} (${built.bytes} bytes, 2 members)`,
                'no endpoint — POST it later with `deploy:endpoint <url> --no-build`',
            ],
        });
    });

    it('reports a PORT that would fight the runner-injected one, in the plan and in the summary', async () => {
        const { probe } = scriptedProbe([]);
        const { deps, sinks } = hostDeps(probe);
        expect(await runDeployEndpoint(flagsFor(['https://box.modal.run', '--dry-run', '--env', 'PORT=9100']), deps)).toBe(0);
        const note = "config.env PORT=9100 overrides the runner's injected PORT=8800: the bin binds 9100 while the container probes 8800 — pass --port 9100 instead of --env PORT=9100";
        expect(sinks.out.join('\n')).toContain(`note:      ${note}`);
    });
});

describe('runDeployEndpoint — a real deploy', () => {
    it('preflights, ships the ANONYMOUS payload, and waits for the container to say ready', async () => {
        const url = 'https://box.modal.run';
        const { probe, calls } = scriptedProbe([
            ['GET', `${url}/health`, OK(HEALTH)],
            ['GET', `${url}/status`, OK(STATUS_EMPTY)],
            ['POST', `${url}/`, ACCEPTED({ slot: 'accepted' })],
            ['GET', `${url}/status`, OK(STATUS_READY)],
        ]);
        const { deps, sinks, written } = hostDeps(probe);
        const built = await buildProviderPayload(flagsFor([url]), deps);
        expect(await runDeployEndpoint(flagsFor([url]), deps)).toBe(0);
        expect(written).toEqual([]);

        expect(calls.map((call) => `${call.method} ${call.url}`)).toEqual(['GET https://box.modal.run/health', 'GET https://box.modal.run/status', 'POST https://box.modal.run/', 'GET https://box.modal.run/status']);

        const post = calls[2]!;
        // NO x-runtime-profile anywhere: that absence is the whole anonymity contract.
        // The config header is compared DECODED, not as a string — the client re-serialises
        // it through its own defaults (buildConfigHeader), so key ORDER is the client's
        // business while the decoded object is the provider's contract.
        const { 'x-runtime-config': header, ...restHeaders } = post.headers;
        expect(restHeaders).toEqual({
            'content-type': 'application/octet-stream',
            'x-runtime-filename': 'typecrate-provider.tar.gz',
        });
        expect(header).toMatch(/^[A-Za-z0-9_-]+$/);
        // The header decodes to the exact fixed contract, key for key.
        expect(JSON.parse(Buffer.from(header!, 'base64url').toString('utf8'))).toEqual({
            runCmd: './typecrate-provider-server',
            port: 8800,
            artifactPath: 'typecrate-provider-server',
            probeTimeoutSec: 120,
            stopSignal: 'SIGTERM',
            env: { TYPECRATE_PROVIDER_VAULT_PATH: './agentic-provider.json', RUST_LOG: 'info' },
        });
        expect(post.bodyBytes).toBe(built.bytes);

        expect(lastJson(sinks.out)).toEqual({
            mode: 'deploy',
            endpoint: url,
            status: 'ready',
            publicUrl: 'https://box.modal.run',
            port: 8800,
            profile: 'anonymous',
            filename: 'typecrate-provider.tar.gz',
            notes: ['format guard: "typecrate-provider-server" is a Linux ELF', 'the endpoint itself serves the payload (it forwards every non-contract path)'],
        });
        // The artifact that shipped is named on stdout by the client's own note, and the
        // verb names where the credentials went: an operator must be able to see WHAT
        // shipped without re-reading the flags.
        expect(sinks.out.join('\n')).toContain(`[runtime-deploy] cargo reported the artifact: ${CARGO_TARGET_BIN}`);
        expect(sinks.out.join('\n')).toContain('provider (clients): https://box.modal.run');
    });

    it('sends the token as the preview header when the endpoint has a secret', async () => {
        const url = 'https://box.modal.run';
        const { probe, calls } = scriptedProbe([
            ['GET', `${url}/health`, OK(HEALTH)],
            ['GET', `${url}/status`, OK(STATUS_EMPTY)],
            ['POST', `${url}/`, ACCEPTED({ slot: 'accepted' })],
            ['GET', `${url}/status`, OK(STATUS_READY)],
        ]);
        const { deps, sinks } = hostDeps(probe);
        expect(await runDeployEndpoint(flagsFor([url, '--token', 's3cret']), deps)).toBe(0);
        for (const call of calls) expect(call.headers['x-daytona-preview-token']).toBe('s3cret');
        expect(lastJson(sinks.out)).toEqual({
            mode: 'deploy',
            endpoint: url,
            status: 'ready',
            publicUrl: 'https://box.modal.run',
            port: 8800,
            profile: 'anonymous',
            filename: 'typecrate-provider.tar.gz',
            notes: ['format guard: "typecrate-provider-server" is a Linux ELF', 'the endpoint itself serves the payload (it forwards every non-contract path)'],
            token: 's3cret',
            tokenScope: 'management',
        });
    });

    it('resolves a BUSY slot with DELETE / before shipping (replace-on-arrival)', async () => {
        const url = 'https://runner.internal';
        const { probe, calls } = scriptedProbe([
            ['GET', `${url}/health`, OK(HEALTH)],
            ['GET', `${url}/status`, OK(STATUS_BUSY)], // preflight: someone else holds the slot
            ['GET', `${url}/status`, OK(STATUS_BUSY)], // teardown names the victim first
            ['DELETE', `${url}/`, REMOVED()],
            ['GET', `${url}/status`, OK(STATUS_EMPTY)], // and proves the slot is free
            ['POST', `${url}/`, ACCEPTED({ slot: 'accepted' })],
            ['GET', `${url}/status`, OK(STATUS_READY)],
        ]);
        const { deps, sinks } = hostDeps(probe);
        expect(await runDeployEndpoint(flagsFor([url]), deps)).toBe(0);
        expect(calls.map((call) => `${call.method} ${call.url}`)).toEqual([
            'GET https://runner.internal/health',
            'GET https://runner.internal/status',
            'GET https://runner.internal/status',
            'DELETE https://runner.internal/',
            'GET https://runner.internal/status',
            'POST https://runner.internal/',
            'GET https://runner.internal/status',
        ]);
        // What was destroyed is named in the summary, not guessed after the fact.
        expect(lastJson(sinks.out)).toMatchObject({ replaced: { slot: 'ready', profile: 'generic', port: 8080, runtimeId: 'rt-old' } });
        expect(sinks.out.join('\n')).toContain('slot: freed a ready runtime');
    });

    it('refuses to touch a busy slot with --no-teardown, and says what to do', async () => {
        const url = 'https://runner.internal';
        const { probe } = scriptedProbe([
            ['GET', `${url}/health`, OK(HEALTH)],
            ['GET', `${url}/status`, OK(STATUS_BUSY)],
        ]);
        const { deps, sinks } = hostDeps(probe);
        expect(await main([url, '--no-teardown'], deps)).toBe(1);
        expect(sinks.err.join('\n')).toContain('the slot holds a ready runtime and replacing it is disabled — free it first with teardown');
    });

    it('reports a failed readiness with the container log tail, and exits 1', async () => {
        const url = 'https://box.modal.run';
        const { probe } = scriptedProbe([
            ['GET', `${url}/health`, OK(HEALTH)],
            ['GET', `${url}/status`, OK(STATUS_EMPTY)],
            ['POST', `${url}/`, ACCEPTED({ slot: 'accepted' })],
            ['GET', `${url}/status`, OK({ slot: 'failed', runtime: { profile: 'generic', port: 8800, ready: false, runtimeId: 'rt-x' } })],
            ['GET', `${url}/logs?lines=40`, { status: 200, ok: true, json: null, text: 'refused vault artifact: no such file' }],
        ]);
        const { deps, sinks } = hostDeps(probe);
        expect(await runDeployEndpoint(flagsFor([url]), deps)).toBe(1);
        const printed = sinks.err.join('\n');
        expect(printed).toContain('✗ failed: the payload reported slot "failed" — its own log tail is attached');
        expect(printed).toContain('refused vault artifact: no such file');
        expect(printed).toContain('the slot may still hold this payload: retry with --teardown');
        expect(lastJson(sinks.out)).toMatchObject({ status: 'failed', publicUrl: null });
    });

    it('--teardown frees the slot and ships nothing (no cargo, no pack, no POST)', async () => {
        const url = 'https://box.modal.run';
        const { probe, calls } = scriptedProbe([
            ['GET', `${url}/status`, OK(STATUS_BUSY)],
            ['DELETE', `${url}/`, REMOVED()],
            ['GET', `${url}/status`, OK(STATUS_EMPTY)],
        ]);
        const { deps, sinks, cargo } = hostDeps(probe);
        expect(await runDeployEndpoint(flagsFor([url, '--teardown']), deps)).toBe(0);
        expect(calls.map((call) => call.method)).toEqual(['GET', 'DELETE', 'GET']);
        expect(cargo.calls).toEqual([]);
        expect(lastJson(sinks.out)).toEqual({
            mode: 'teardown',
            endpoint: url,
            status: 'ready',
            publicUrl: null,
            notes: ['slot now reads "empty" — the container keeps serving and is ready for the next POST'],
            replaced: { slot: 'ready', profile: 'generic', port: 8080, runtimeId: 'rt-old' },
        });
    });
});

describe('main', () => {
    it('prints USAGE and exits 0 on --help, touching no dep', async () => {
        const { probe, calls } = scriptedProbe([]);
        const { deps, sinks } = hostDeps(probe);
        expect(await main(['--help'], deps)).toBe(0);
        expect(calls).toEqual([]);
        // One line, verbatim, unprefixed: the usage screen is the contract, and a prefix
        // on it would break `… --help | head -1`.
        expect(sinks.out).toEqual([USAGE]);
    });

    it('maps a broken command line to exit 1 plus the usage screen', async () => {
        const { probe } = scriptedProbe([]);
        const { deps, sinks } = hostDeps(probe);
        expect(await main(['--nope'], deps)).toBe(1);
        // TWO problems from one token: the unknown flag, and the endpoint it left unset.
        // A blank line separates the errors from the usage screen.
        expect(sinks.err).toEqual([`${PREFIX} ✗ unknown flag "--nope"`, `${PREFIX} ✗ no endpoint — pass the runner URL as the argument, or --endpoint <url>, or ${ENDPOINT_ENV}=… (or use --out <path> to only build the bundle)`, '', USAGE]);
    });

    it('keeps an unexpected failure honest — the stack, not a dressed-up hint', async () => {
        const { probe } = scriptedProbe([]);
        const broken: FsAccess = {
            exists: () => true,
            read: () => {
                throw new Error('disk on fire');
            },
            readHead: () => Buffer.alloc(0),
            stat: () => ({ size: 1, isFile: true }),
        };
        const { deps, sinks } = hostDeps(probe, { fs: broken });
        expect(await main(['https://box.modal.run', '--dry-run'], deps)).toBe(1);
        expect(sinks.err[0]).toContain('✗ unexpected failure:');
        expect(sinks.err[0]).toContain('disk on fire');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  ENTRY GUARD — `yarn deploy:endpoint` must auto-run, and an import must not
//  (entry-guard.ts is the one implementation all three verbs share)
// ══════════════════════════════════════════════════════════════════════════════

describe('isCliEntryPoint', () => {
    const verb = '/repo/packages/typecrate/provider/scripts/deploy-endpoint.ts';
    const wrapper = '/repo/packages/typecrate/provider/scripts/start-modal.ts';

    it('fires for argv naming the script, and for the lifecycle line', () => {
        // Rule 1, absolute: vite-node keeps the file on the command line.
        expect(isCliEntryPoint({ modulePath: verb, argv: ['node', verb] })).toBe(true);
        // Rule 1, relative: the token must be a PATH SUFFIX of the module, so a sibling
        // package's script of the same name in the wrong directory cannot fire it.
        expect(isCliEntryPoint({ modulePath: verb, argv: ['node', 'vite-node', '--script', 'scripts/deploy-endpoint.ts'] })).toBe(true);
        // Rule 2: the yarn lifecycle line.
        expect(isCliEntryPoint({ modulePath: verb, env: { npm_lifecycle_script: 'vite-node scripts/deploy-endpoint.ts' } })).toBe(true);
        // …and it is per-script: the wrapper's lifecycle line must not fire the verb.
        expect(isCliEntryPoint({ modulePath: verb, env: { npm_lifecycle_script: 'vite-node scripts/start-modal.ts' } })).toBe(false);
        // A test importing the verb names `deploy-endpoint.test.ts`, which is NOT a suffix
        // of `deploy-endpoint.ts` — the near-miss the old name-based regex used to swallow.
        expect(isCliEntryPoint({ modulePath: verb, argv: ['node', '/repo/packages/typecrate/provider/scripts/deploy-endpoint.test.ts'] })).toBe(false);
    });

    it('never fires under vitest', () => {
        expect(isCliEntryPoint({ modulePath: verb, vitest: true, argv: ['node', verb] })).toBe(false);
    });

    it('fires only when NOTHING but the runner sits above the module', () => {
        // Rule 3: `vite-node scripts/deploy-endpoint.ts` rewrites argv and drops the file,
        // so the module-runner stack is the only evidence left.
        expect(isCliEntryPoint({ modulePath: verb, stack: `Error\n    at Object.<anonymous> (${verb}:1:1)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(true);
        // start-modal.ts importing the verb is exactly the case that must NOT auto-run.
        expect(isCliEntryPoint({ modulePath: verb, stack: `Error\n    at Object.<anonymous> (${verb}:1:1)\n    at mainModal (${wrapper}:3:3)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(false);
        // …while the wrapper's own guard does fire for the wrapper.
        expect(isCliEntryPoint({ modulePath: wrapper, stack: `Error\n    at Object.<anonymous> (${wrapper}:1:1)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(true);
        expect(isCliEntryPoint({ modulePath: '/repo/packages/typecrate/provider/scripts/start-daytona.ts', env: { npm_lifecycle_script: 'vite-node scripts/start-daytona.ts' } })).toBe(true);
    });

    it('does nothing at all with no signals', () => {
        expect(isCliEntryPoint({ modulePath: verb })).toBe(false);
    });
});
