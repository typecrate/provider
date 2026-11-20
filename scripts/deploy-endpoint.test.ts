// ─────────────────────────────────────────────────────────────────────────────
//  deploy-endpoint.test.ts — the provider's own deploy verb, end to end, with ZERO
//  network, ZERO cargo spawn and ZERO real filesystem, against sandbox runtime v2.
//
//  Everything side-effectful in deploy-endpoint.ts arrives through `ProviderDeps`
//  (the shared client's RuntimeDeps — probe / runCommand / fs / env / log / error —
//  plus the writeFile seam), so these tests drive the REAL control flow against
//  scripted endpoint answers and assert exact values:
//    · parser      — the auto port (undefined, `--port 0` too), the env/flag admin
//                    credential pair, the reserved `env.PORT` refusal, every range
//                    check, and `--out` with no endpoint = package-only
//    · payload     — the v2 fixed contract, key for key: app name "provider", top-level
//                    targetPrefix "/providers/private/v1", NO config.port unless asked,
//                    probePath "/health" (the bin's loopback route), and the env triple
//                    including TYPECRATE_PROVIDER_HOST=127.0.0.1 and never PORT
//    · registry    — POST /apps on create, PUT /apps/provider blue-green on replace,
//                    GET /apps/provider for readiness, DELETE /apps/provider for
//                    teardown, GET /apps/provider/logs for the failure tail, and the
//                    x-app-name / x-app-target-prefix upload headers
//    · auth        — the Daytona preview token and the runner ADMIN bearer travel
//                    together but stay separate; the admin value never reaches a log
//    · urls        — the provider base is endpoint + /app/provider/ (never a per-port
//                    link), and /app/provider/health is NOT advertised as health
// ─────────────────────────────────────────────────────────────────────────────

import { join, resolve } from 'node:path';
import { describe, expect, it, vi } from 'vitest';

import { DEFAULT_LOG_LINES, encodeConfigHeader, readTarBundle } from '@services/daytona/sandbox';
import type { FsAccess, Probe, ProbeResult } from '@services/daytona/sandbox';

import {
    ADMIN_TOKEN_ENV,
    ADMIN_TOKEN_FILE_ENV,
    APP_NAME,
    APP_PATH,
    DEFAULT_PORT,
    DEFAULT_PROBE_PATH,
    DEFAULT_PROBE_TIMEOUT_SEC,
    DEFAULT_VAULT_PATH,
    ENDPOINT_ENV,
    GENERIC_ADMIN_TOKEN_ENV,
    GENERIC_ADMIN_TOKEN_FILE_ENV,
    PACKAGE_ROOT,
    PAYLOAD_FILENAME,
    PREFIX,
    PROVIDER_HOST_ENV,
    PROVIDER_HOST_LOOPBACK,
    REPO_ROOT,
    SERVER_MEMBER,
    TARGET_PREFIX,
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
    reservedEnvErrors,
    resolveAdminToken,
    resolveAdminTokenFile,
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
const MISSING = (json: unknown = { error: 'app_not_found' }): ProbeResult => ({ status: 404, ok: false, json, text: JSON.stringify(json) });

/**
 * The v2 runner handshake. `authRequired: true` + the `admin-auth` capability is the
 * shape a sandbox created with SANDBOX_ADMIN_TOKEN reports.
 */
const HEALTH = { ok: true, contract: 'sandbox-runtime-v2', apiVersion: 2, capabilities: ['apps'], authRequired: false, uptimeSec: 5, runnerPort: 7777 };
const HEALTH_ADMIN = { ...HEALTH, capabilities: ['apps', 'admin-auth'], authRequired: true };

/** A registry AppView, as GET /apps/provider reports it. */
const APP_READY = {
    name: APP_NAME,
    revision: 1,
    path: APP_PATH,
    publicUrl: 'https://box.modal.run/app/provider/',
    internalPort: 20043,
    targetPrefix: TARGET_PREFIX,
    profile: 'anonymous',
    filename: PAYLOAD_FILENAME,
    status: 'ready',
    ready: true,
    error: null,
    lastReplaceError: null,
    pending: null,
    queue: null,
    lastProbe: null,
};
const APP_FAILED = { ...APP_READY, status: 'failed', ready: false, error: 'vault document refused', internalPort: 20043 };

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
 * and cannot fail. Faking the cwd makes the two behaviours produce different strings.
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
 * The canonical WIRE form of the default config. The client re-serialises config through
 * its own defaults before encoding the header, so the key ORDER on the wire is the
 * client's, not the declaration order in `buildPayloadConfig`. This string is that
 * canonical text, pinned so a change in either serialiser surfaces here; the SEMANTIC
 * contract is pinned independently by the decoded-header assertion below.
 */
const CANONICAL_CONFIG_JSON =
    '{"runCmd":"./typecrate-provider-server","stopSignal":"SIGTERM","probeTimeoutSec":120,"probePath":"/health","artifactPath":"typecrate-provider-server","env":{"TYPECRATE_PROVIDER_VAULT_PATH":"./agentic-provider.json","TYPECRATE_PROVIDER_HOST":"127.0.0.1","RUST_LOG":"info"}}';

/** The exact dry-run note list the shared client produces for THIS payload. */
const plannedNotes = (endpoint: string, bytes: number): string[] => [
    // The client's format guard runs before anything is sent, and its note leads the list.
    'format guard: "typecrate-provider-server" is a Linux ELF',
    'dry run — no probe, no PUT, no POST, no cargo',
    `POST ${endpoint}/apps — ${bytes} bytes as "typecrate-provider.tar.gz", app "provider", targetPrefix "/providers/private/v1", profile anonymous`,
    `x-runtime-config: ${encodeConfigHeader(JSON.parse(CANONICAL_CONFIG_JSON) as object)} (base64url JSON of ${CANONICAL_CONFIG_JSON})`,
    'auth: preview token not sent; admin bearer not sent',
    `public base URL: ${endpoint}/app/provider/ (endpoint + registry path /app/provider/)`,
    // probeTimeoutSec 120 + the client's own 120 s slack, no build step: 240 000 ms.
    "readiness budget: 240000 ms derived from the app's own timeouts",
    'replace policy: an existing app is replaced blue-green with PUT /apps/provider (it keeps serving /app/provider/ until the new revision is ready)',
];

/** The public base of the box these flow tests deploy to. */
const BOX = 'https://box.modal.run';

// ══════════════════════════════════════════════════════════════════════════════
//  PARSER
// ══════════════════════════════════════════════════════════════════════════════

describe('parseArgs', () => {
    it('starts from the v2 payload contract: no port, loopback readiness, no admin credential', () => {
        const { flags, errors } = parseArgs([BOX]);
        expect(errors).toEqual([]);
        expect(flags).toStrictEqual({
            help: false,
            dryRun: false,
            teardownOnly: false,
            replace: true,
            endpoint: BOX,
            token: '',
            adminToken: '',
            adminTokenFile: '',
            // v2: absent = the runner auto-allocates.
            port: undefined,
            probePath: '/health',
            env: {},
            vault: join(REPO_ROOT, 'config', 'vault', 'generated', 'agentic-provider.json'),
            out: '',
            noBuild: false,
            artifact: '',
            wait: true,
            logsLines: 40,
            allowUnconfigured: false,
        });
        // The pins that must never drift are stated once, here.
        expect(APP_NAME).toBe('provider');
        expect(APP_PATH).toBe('/app/provider/');
        expect(TARGET_PREFIX).toBe('/providers/private/v1');
        expect(DEFAULT_PROBE_PATH).toBe('/health');
        expect(DEFAULT_PROBE_TIMEOUT_SEC).toBe(120);
        expect(DEFAULT_LOG_LINES).toBe(40);
        // The crate's own native default — documented, never applied to the payload.
        expect(DEFAULT_PORT).toBe(8800);
        expect(PROVIDER_HOST_ENV).toBe('TYPECRATE_PROVIDER_HOST');
        expect(PROVIDER_HOST_LOOPBACK).toBe('127.0.0.1');
        expect(DEFAULT_VAULT_PATH).toBe(join(REPO_ROOT, 'config', 'vault', 'generated', 'agentic-provider.json'));
        expect(PACKAGE_ROOT).toBe(join(REPO_ROOT, 'packages', 'typecrate', 'provider'));
    });

    it('omits config.port by default, treats --port 0 as auto, and honours an explicit port', () => {
        expect(flagsFor([BOX]).port).toBeUndefined();
        expect(flagsFor([BOX, '--port', '0']).port).toBeUndefined();
        expect(flagsFor([BOX, '--port', '9000']).port).toBe(9000);
        expect(parseArgs([BOX, '--port', '65536']).errors).toEqual(['--port "65536" is out of range (internal TCP port 0-65535, 0 = auto)']);
    });

    it('reads the endpoint and the preview token from the documented environment', () => {
        const { flags, errors } = parseArgs([], { [ENDPOINT_ENV]: 'https://env.modal.run', [TOKEN_ENV]: 's3cret' });
        expect(errors).toEqual([]);
        expect(flags.endpoint).toBe('https://env.modal.run');
        expect(flags.token).toBe('s3cret');
        expect(ENDPOINT_ENV).toBe('TYPECRATE_PROVIDER_ENDPOINT');
        expect(TOKEN_ENV).toBe('TYPECRATE_PROVIDER_ENDPOINT_TOKEN');
    });

    it('reads the ADMIN bearer from its own package env, then the generic Daytona name', () => {
        expect(parseArgs([], { [ENDPOINT_ENV]: BOX, [ADMIN_TOKEN_ENV]: 'env-admin' }).flags.adminToken).toBe('env-admin');
        expect(parseArgs([], { [ENDPOINT_ENV]: BOX, [ADMIN_TOKEN_ENV]: '', [GENERIC_ADMIN_TOKEN_ENV]: 'generic-admin' }).flags.adminToken).toBe('generic-admin');
        expect(parseArgs([], { [ENDPOINT_ENV]: BOX, [ADMIN_TOKEN_FILE_ENV]: '/host/a.token' }).flags.adminTokenFile).toBe('/host/a.token');
        expect(parseArgs([], { [ENDPOINT_ENV]: BOX, [GENERIC_ADMIN_TOKEN_FILE_ENV]: '/host/b.token' }).flags.adminTokenFile).toBe('/host/b.token');
        // The two credentials never share a variable, and the preview token is not enough.
        expect(parseArgs([], { [ENDPOINT_ENV]: BOX, [TOKEN_ENV]: 'preview-only' }).flags.adminToken).toBe('');
        expect(ADMIN_TOKEN_ENV).toBe('TYPECRATE_PROVIDER_SANDBOX_ADMIN_TOKEN');
        expect(ADMIN_TOKEN_FILE_ENV).toBe('TYPECRATE_PROVIDER_SANDBOX_ADMIN_TOKEN_FILE');
        expect(GENERIC_ADMIN_TOKEN_ENV).toBe('SANDBOX_ADMIN_TOKEN');
        expect(GENERIC_ADMIN_TOKEN_FILE_ENV).toBe('SANDBOX_ADMIN_TOKEN_FILE');
    });

    it('refuses two admin credentials at once — the caller picks one', () => {
        expect(parseArgs([BOX, '--admin-token', 'a', '--admin-token-file', '/host/a.token']).errors).toEqual([
            '--admin-token and --admin-token-file are mutually exclusive — pass ONE (the file form is preferred: the value never reaches a command line)',
        ]);
        // The same contradiction through the environment is caught before any probe.
        expect(parseArgs([BOX], { [ADMIN_TOKEN_ENV]: 'a', [ADMIN_TOKEN_FILE_ENV]: '/host/a.token' }).errors).toEqual([
            '--admin-token and --admin-token-file are mutually exclusive — pass ONE (the file form is preferred: the value never reaches a command line)',
        ]);
    });

    it('refuses a RESERVED env key locally, with the real knob in the message', () => {
        // v2 R-8.3/R-8.4: a user PORT is a hard 400, so it must never be shipped.
        expect(parseArgs([BOX, '--env', 'PORT=9100']).errors).toEqual([
            '--env PORT=9100 is RESERVED: the runner injects PORT from the app\'s internal port and rejects a user value with 400 config_invalid — pass --port <n> (0 or omitted = auto-allocate) instead',
        ]);
        expect(parseArgs([BOX, '--env', 'APP_NAME=x']).errors).toEqual(['--env APP_NAME=… is RESERVED: the runner owns APP_NAME and rejects a user value with 400 config_invalid — drop it']);
        expect(parseArgs([BOX, '--env', 'APP_REGISTRY_URL=x']).errors).toEqual(['--env APP_REGISTRY_URL=… is RESERVED: the runner owns APP_REGISTRY_URL and rejects a user value with 400 config_invalid — drop it']);
        // …and the rejected key never lands in the payload.
        expect(parseArgs([BOX, '--env', 'PORT=9100']).flags.env).toEqual({});
        expect(reservedEnvErrors({ PORT: '1', RUST_LOG: 'debug' })).toEqual([
            '--env PORT=1 is RESERVED: the runner injects PORT from the app\'s internal port and rejects a user value with 400 config_invalid — pass --port <n> (0 or omitted = auto-allocate) instead',
        ]);
        expect(reservedEnvErrors({ RUST_LOG: 'debug' })).toEqual([]);
    });

    it('accumulates --env and keeps everything after the first = in the value', () => {
        const { flags, errors } = parseArgs([BOX, '--env', 'RUST_LOG=debug', '--env', 'DSN=url://a=b']);
        expect(errors).toEqual([]);
        expect(flags.env).toEqual({ RUST_LOG: 'debug', DSN: 'url://a=b' });
    });

    it('reports each bad token instead of throwing', () => {
        expect(parseArgs([BOX, '--nope']).errors).toEqual(['unknown flag "--nope"']);
        expect(parseArgs([BOX, 'https://y']).errors).toEqual(['unexpected extra argument "https://y" — the endpoint is the only positional (got "https://box.modal.run" already)']);
        expect(parseArgs([BOX, '--logs', '0']).errors).toEqual(['--logs "0" is out of range (1-1000 lines)']);
        expect(parseArgs([BOX, '--probe-path', 'health']).errors).toEqual(['--probe-path must start with "/" (it is an absolute route on the app\'s loopback listener), got "health"']);
        expect(parseArgs([BOX, '--env', 'NOEQUALS']).errors).toEqual(['--env expects KEY=VALUE, got "NOEQUALS"']);
        expect(parseArgs([BOX, '--token']).errors).toEqual(['--token needs a value']);
        expect(parseArgs([BOX, '--admin-token']).errors).toEqual(['--admin-token needs a value']);
        expect(parseArgs([BOX, '--admin-token-file']).errors).toEqual(['--admin-token-file needs a value']);
    });

    it('refuses two endpoints that disagree, and accepts the same one twice', () => {
        expect(parseArgs(['https://a', '--endpoint', 'https://b']).errors).toEqual(['two different endpoints given: "https://a" and --endpoint "https://b"']);
        expect(parseArgs(['https://a', '--endpoint', 'https://a']).errors).toEqual([]);
    });

    it('lets the command line beat TYPECRATE_PROVIDER_ENDPOINT, which is only a default', () => {
        const positional = parseArgs(['https://second--box.modal.run'], { [ENDPOINT_ENV]: 'https://repo-box.modal.run' });
        expect(positional.errors).toEqual([]);
        expect(positional.flags.endpoint).toBe('https://second--box.modal.run');
        expect(parseArgs(['--endpoint', 'https://flag.run'], { [ENDPOINT_ENV]: 'https://repo-box.modal.run' }).flags.endpoint).toBe('https://flag.run');
    });

    it('refuses a --teardown that also carries shipping knobs, but keeps the admin credential', () => {
        expect(parseArgs([BOX, '--teardown', '--port', '9100', '--env', 'A=1']).errors).toEqual(['--teardown ships nothing, so --port / --env do not apply (drop them, or drop --teardown)']);
        expect(parseArgs([BOX, '--teardown']).errors).toEqual([]);
        // DELETE /apps/provider is still an authenticated mutation, so the admin bearer
        // (and the preview token) must survive --teardown.
        expect(parseArgs([BOX, '--teardown', '--admin-token', 'at', '--token', 'pt']).errors).toEqual([]);
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
        expect(errors).toEqual(['unknown flag "--nope"']);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  THE PAYLOAD — the fixed v2 contract, asserted as one literal per shape
// ══════════════════════════════════════════════════════════════════════════════

describe('buildPayloadConfig', () => {
    it('is exactly the anonymous v2 payload the container contract asks for', () => {
        const config = buildPayloadConfig(flagsFor([BOX]));
        expect(config).toEqual({
            runCmd: './typecrate-provider-server',
            artifactPath: 'typecrate-provider-server',
            probeTimeoutSec: 120,
            stopSignal: 'SIGTERM',
            probePath: '/health',
            env: { TYPECRATE_PROVIDER_VAULT_PATH: './agentic-provider.json', TYPECRATE_PROVIDER_HOST: '127.0.0.1', RUST_LOG: 'info' },
        });
        // v2 auto-allocates: an absent port is the documented choice, not an empty value.
        expect(Object.keys(config)).not.toContain('port');
        // The v1 PORT escape hatch is gone, and a user value cannot even reach here.
        expect(Object.keys(config.env!)).not.toContain('PORT');
    });

    it('adds a config.port only when the operator names an internal port', () => {
        expect(buildPayloadConfig(flagsFor([BOX, '--port', '9000']))).toEqual({
            runCmd: './typecrate-provider-server',
            port: 9000,
            artifactPath: 'typecrate-provider-server',
            probeTimeoutSec: 120,
            stopSignal: 'SIGTERM',
            probePath: '/health',
            env: { TYPECRATE_PROVIDER_VAULT_PATH: './agentic-provider.json', TYPECRATE_PROVIDER_HOST: '127.0.0.1', RUST_LOG: 'info' },
        });
        expect(buildPayloadConfig(flagsFor([BOX, '--port', '0']))).not.toHaveProperty('port');
    });

    it('honours --probe-path, and keeps loopback readiness by default', () => {
        expect(buildPayloadConfig(flagsFor([BOX, '--probe-path', '/ready'])).probePath).toBe('/ready');
        expect(buildPayloadConfig(flagsFor([BOX])).probePath).toBe('/health');
    });

    it('lets --env override the defaults, and only those keys', () => {
        expect(buildPayloadEnv(flagsFor([BOX, '--env', 'RUST_LOG=debug', '--env', 'TYPECRATE_PROVIDER_HOST=127.0.0.1']))).toEqual({
            TYPECRATE_PROVIDER_VAULT_PATH: './agentic-provider.json',
            TYPECRATE_PROVIDER_HOST: '127.0.0.1',
            RUST_LOG: 'debug',
        });
    });

    it('appends the bin flag to runCmd only when --allow-unconfigured is asked for', () => {
        expect(buildRunCmd(flagsFor([BOX]))).toBe('./typecrate-provider-server');
        expect(buildRunCmd(flagsFor([BOX, '--allow-unconfigured']))).toBe('./typecrate-provider-server --allow-unconfigured');
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
    it('anchors a RELATIVE value on the repo root it is given — never on the process cwd', async () => {
        await withCwd('/fake/cwd', () => {
            expect(resolveOutPath('Temp/out.tar.gz', '/fake/repo')).toBe('/fake/repo/Temp/out.tar.gz');
        });
    });

    it('returns an ABSOLUTE value verbatim — the repo root is never prepended to someone else\'s path', async () => {
        await withCwd('/fake/cwd', () => {
            expect(resolveOutPath('/tmp/out.tar.gz', '/fake/repo')).toBe('/tmp/out.tar.gz');
            expect(resolveOutPath('./Temp/out.tar.gz', '/fake/repo')).toBe('/fake/repo/Temp/out.tar.gz');
        });
    });

    it("uses THIS repo root by default, so the verb cannot land scratch inside the package", async () => {
        await withCwd(PACKAGE_ROOT, () => {
            expect(resolveOutPath('Temp/out.tar.gz')).toBe(join(REPO_ROOT, 'Temp', 'out.tar.gz'));
            expect(resolveOutPath('Temp/out.tar.gz')).not.toBe(join(PACKAGE_ROOT, 'Temp', 'out.tar.gz'));
        });
    });
});

describe('resolveVaultPath', () => {
    it('accepts the artifact a healthy host has', () => {
        const deps: ProviderDeps = { probe: async () => OK({}), fs: healthyFs() };
        expect(resolveVaultPath(flagsFor([BOX]), deps)).toBe(VAULT_PATH);
    });

    it('refuses to deploy a payload that cannot authenticate itself, and names the regeneration command', () => {
        const deps: ProviderDeps = { probe: async () => OK({}), fs: fakeFs({ [CARGO_TARGET_BIN]: ELF }) };
        let message = '';
        try {
            resolveVaultPath(flagsFor([BOX]), deps);
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
        expect(() => resolveVaultPath(flagsFor([BOX]), deps)).toThrow(/not a readable non-empty file/);
    });

    it('honours --vault', () => {
        const other = join(REPO_ROOT, 'Temp', 'other-vault.json');
        const deps: ProviderDeps = { probe: async () => OK({}), fs: fakeFs({ [other]: VAULT_DOC }) };
        expect(resolveVaultPath(flagsFor([BOX, '--vault', other]), deps)).toBe(other);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  THE ADMIN CREDENTIAL — independent of the preview token, never printed
// ══════════════════════════════════════════════════════════════════════════════

describe('resolveAdminToken', () => {
    it('returns the flag/env value, and nothing at all when no credential is configured', () => {
        const deps: ProviderDeps = { probe: async () => OK({}), fs: healthyFs() };
        expect(resolveAdminToken(flagsFor([BOX]), deps)).toBe('');
        expect(resolveAdminToken(flagsFor([BOX, '--admin-token', 'admin-t0k3n']), deps)).toBe('admin-t0k3n');
    });

    it('reads the file form, TRIMMED, so a trailing newline is not part of the Bearer', () => {
        const file = join(REPO_ROOT, 'Temp', 'admin.token');
        const deps: ProviderDeps = { probe: async () => OK({}), fs: fakeFs({ [file]: 'admin-t0k3n\n' }) };
        expect(resolveAdminToken(flagsFor([BOX, '--admin-token-file', file]), deps)).toBe('admin-t0k3n');
    });

    it('refuses a missing or blank file instead of silently deploying without admin auth', () => {
        const missing = join(REPO_ROOT, 'Temp', 'missing.token');
        const deps: ProviderDeps = { probe: async () => OK({}), fs: healthyFs() };
        expect(() => resolveAdminToken(flagsFor([BOX, '--admin-token-file', missing]), deps)).toThrow(/the admin token file is not a readable non-empty file/);
        expect(() => resolveAdminToken(flagsFor([BOX, '--admin-token-file', missing]), deps)).toThrow(/independent of the preview token/);

        const blank = join(REPO_ROOT, 'Temp', 'blank.token');
        const blankFs: ProviderDeps = { probe: async () => OK({}), fs: fakeFs({ [blank]: '\n\t\n' }) };
        expect(() => resolveAdminToken(flagsFor([BOX, '--admin-token-file', blank]), blankFs)).toThrow(/holds no token after trimming/);
    });

    it('refuses a doubly configured credential at resolution time too, not only in the parser', () => {
        const deps: ProviderDeps = { probe: async () => OK({}), fs: healthyFs() };
        expect(() => resolveAdminToken({ adminToken: 'a', adminTokenFile: '/host/a.token' }, deps)).toThrow(/configured twice/);
    });

    it('anchors the file path ONCE, so the reader and createSandbox cannot disagree', async () => {
        expect(resolveAdminTokenFile(flagsFor([BOX]))).toBeUndefined();
        // An absolute value is verbatim; the credential object start:daytona passes to
        // createSandbox is therefore the same file resolveAdminToken read.
        expect(resolveAdminTokenFile(flagsFor([BOX, '--admin-token-file', '/host/admin.token']))).toBe('/host/admin.token');
        // A relative value is anchored on the process cwd HERE, once, instead of being
        // resolved again by whoever consumes the credential.
        await withCwd('/fake/cwd', () => {
            expect(resolveAdminTokenFile(flagsFor([BOX, '--admin-token-file', 'Temp/admin.token']))).toBe('/fake/cwd/Temp/admin.token');
        });
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  ASSEMBLY — cargo, then the vault, then the tar.gz the container will exec
// ══════════════════════════════════════════════════════════════════════════════

describe('buildProviderPayload', () => {
    it("runs the client's pinned cargo recipe from the package root, and reads its NDJSON path", async () => {
        const { probe } = scriptedProbe([]);
        const { deps, cargo } = hostDeps(probe);
        const built = await buildProviderPayload(flagsFor([BOX]), deps);
        expect(cargo.calls).toEqual([
            {
                command: 'cargo',
                args: ['build', '--release', '--locked', '-p', 'typecrate-provider', '--message-format=json'],
                cwd: PACKAGE_ROOT,
            },
        ]);
        expect(built.artifact).toEqual({ path: CARGO_TARGET_BIN, sizeBytes: ELF.length, source: 'cargo-json' });
    });

    it('never spawns cargo on a dry run, and carries the v2 app identity + targetPrefix', async () => {
        const { probe } = scriptedProbe([]);
        const { deps, cargo } = hostDeps(probe);
        const built = await buildProviderPayload(flagsFor([BOX, '--dry-run']), deps);
        expect(cargo.calls).toEqual([]);
        expect(built.artifact).toEqual({ path: CARGO_TARGET_BIN, sizeBytes: ELF.length, source: 'candidate' });
        // The registry key and the native base path are TOP-LEVEL payload metadata, not
        // config keys (spec §4.7/§8.5). A `default` name would put the provider on
        // /app/default/; an empty prefix would route /app/provider/models to /models.
        expect(built.payload.name).toBe('provider');
        expect(built.payload.targetPrefix).toBe('/providers/private/v1');
        expect(built.payload.config).not.toHaveProperty('targetPrefix');
        expect(built.payload.config).not.toHaveProperty('name');
    });

    it('never spawns cargo with --artifact, and ships exactly that file', async () => {
        const explicit = join(REPO_ROOT, '.rust', 'target', 'release', 'elsewhere-server');
        const { probe } = scriptedProbe([]);
        const { deps, cargo } = hostDeps(probe, { fs: fakeFs({ [explicit]: ELF, [VAULT_PATH]: VAULT_DOC }) });
        const built = await buildProviderPayload(flagsFor([BOX, '--artifact', explicit]), deps);
        expect(cargo.calls).toEqual([]);
        expect(built.artifact).toEqual({ path: explicit, sizeBytes: ELF.length, source: 'explicit' });
    });

    it('writes exactly the two members, with their modes, inside one gzip — read back with the reader\'s own parser', async () => {
        const { probe } = scriptedProbe([]);
        const { deps } = hostDeps(probe);
        const built = await buildProviderPayload(flagsFor([BOX, '--dry-run']), deps);
        expect(built.payload.filename).toBe('typecrate-provider.tar.gz');
        expect(built.members).toEqual([
            { name: 'typecrate-provider-server', size: ELF.length, mode: 0o755 },
            { name: 'agentic-provider.json', size: VAULT_BYTES, mode: 0o644 },
        ]);
        expect(readTarBundle(built.payload.buffer).map((member) => ({ name: member.name, size: member.size, mode: member.mode }))).toEqual([
            { name: 'typecrate-provider-server', size: ELF.length, mode: 0o755 },
            { name: 'agentic-provider.json', size: VAULT_BYTES, mode: 0o644 },
        ]);
        expect(built.bytes).toBe(built.payload.buffer.length);
        const again = await buildProviderPayload(flagsFor([BOX, '--dry-run']), hostDeps(probe).deps);
        expect(again.payload.buffer.equals(built.payload.buffer)).toBe(true);
    });

    it('carries the config alongside the bytes', async () => {
        const { probe } = scriptedProbe([]);
        const { deps } = hostDeps(probe);
        const built = await buildProviderPayload(flagsFor([BOX, '--dry-run', '--port', '9000']), deps);
        expect(built.payload.config).toEqual({
            runCmd: './typecrate-provider-server',
            port: 9000,
            artifactPath: 'typecrate-provider-server',
            probeTimeoutSec: 120,
            stopSignal: 'SIGTERM',
            probePath: '/health',
            env: { TYPECRATE_PROVIDER_VAULT_PATH: './agentic-provider.json', TYPECRATE_PROVIDER_HOST: '127.0.0.1', RUST_LOG: 'info' },
        });
    });

    it('refuses to assemble when the vault document is missing — before any probe', async () => {
        const { probe, calls } = scriptedProbe([]);
        const { deps, sinks } = hostDeps(probe, { fs: fakeFs({ [CARGO_TARGET_BIN]: ELF, [join(REPO_ROOT, '.cargo', 'config.toml')]: CARGO_CONFIG }) });
        expect(await main([BOX, '--dry-run'], deps)).toBe(1);
        expect(sinks.err.join('\n')).toContain(VAULT_REGENERATION_COMMAND);
        expect(calls).toEqual([]);
    });

    it('names the build verb when the artifact is not there', async () => {
        const { probe } = scriptedProbe([]);
        const { deps, sinks } = hostDeps(probe, { fs: fakeFs({ [VAULT_PATH]: VAULT_DOC }) });
        expect(await main([BOX, '--dry-run'], deps)).toBe(1);
        const printed = sinks.err.join('\n');
        expect(printed).toContain('no release binary "typecrate-provider-server" was found');
        expect(printed).toContain('yarn workspace @typecrate/provider build:server');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  DEPLOY OPTIONS — the flag mapping, pinned separately because a drift here changes
//  what the CONTAINER does (replace or refuse) and what the runner authenticates
// ══════════════════════════════════════════════════════════════════════════════

describe('buildDeployOptions', () => {
    it('maps the verbs onto the client options, admin bearer included', () => {
        expect(buildDeployOptions(flagsFor([BOX, '--token', 't0k3n', '--admin-token', 'at', '--logs', '200']))).toEqual({
            token: 't0k3n',
            adminToken: 'at',
            replace: true,
            noWait: false,
            logsLines: 200,
            dryRun: false,
            expectElf: true,
            printSummary: false,
        });
        expect(buildDeployOptions(flagsFor([BOX, '--no-teardown', '--no-wait', '--dry-run']))).toEqual({
            token: '',
            adminToken: '',
            replace: false,
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
    it('touches NOTHING and prints the assembled payload, the app identity and the summary', async () => {
        const { probe, calls } = scriptedProbe([]);
        const { deps, sinks, cargo } = hostDeps(probe);
        const built = await buildProviderPayload(flagsFor([BOX, '--dry-run']), deps);

        expect(await runDeployEndpoint(flagsFor([BOX, '--dry-run']), deps)).toBe(0);
        expect(calls).toEqual([]);
        expect(cargo.calls).toEqual([]);

        const printed = sinks.out.join('\n');
        expect(printed).toContain('DRY RUN — no probe, no PUT, no DELETE, no POST, no cargo, no network');
        expect(printed).toContain(`app:       "provider" — clients use https://box.modal.run/app/provider/… ; native prefix /providers/private/v1`);
        expect(printed).toContain(`0755  typecrate-provider-server  ${ELF.length} bytes`);
        expect(printed).toContain(`0644  agentic-provider.json  ${VAULT_BYTES} bytes`);
        expect(printed).toContain('{"runCmd":"./typecrate-provider-server","artifactPath":"typecrate-provider-server","probeTimeoutSec":120,"stopSignal":"SIGTERM","probePath":"/health","env":{"TYPECRATE_PROVIDER_VAULT_PATH":"./agentic-provider.json","TYPECRATE_PROVIDER_HOST":"127.0.0.1","RUST_LOG":"info"}}');
        expect(printed).toContain(`bundle:    typecrate-provider.tar.gz  (${built.bytes} bytes gzip, 2 members, flat)`);
        expect(printed).toContain('readiness: HTTP 2xx on /health via LOOPBACK within 120s — client budget 240000 ms');
        expect(printed).toContain('(direct to the bin: /app/provider/health would map to /providers/private/v1/health and 404, so it is NOT the probe)');
        expect(printed).toContain('app policy: an existing app is replaced blue-green with PUT /apps/provider — peers untouched');

        expect(lastJson(sinks.out)).toEqual({
            mode: 'deploy',
            endpoint: BOX,
            name: 'provider',
            status: 'planned',
            publicUrl: 'https://box.modal.run/app/provider/',
            reportedPublicUrl: null,
            path: '/app/provider/',
            port: 0,
            profile: 'anonymous',
            filename: 'typecrate-provider.tar.gz',
            notes: plannedNotes(BOX, built.bytes),
        });
    });

    it('masks BOTH credentials in the plan, and never prints the admin value', async () => {
        const { probe } = scriptedProbe([]);
        const { deps, sinks } = hostDeps(probe);
        expect(await runDeployEndpoint(flagsFor([BOX, '--dry-run', '--token', 's3cret', '--admin-token', 'admin-value-never-printed']), deps)).toBe(0);
        const printed = sinks.out.join('\n');
        expect(printed).toContain('auth:      preview *** (length 6) · admin Bearer configured (value never printed)');
        expect(printed).not.toContain('admin-value-never-printed');
        // The client's own summary reports the PREVIEW token (documented `token` +
        // `tokenScope: management`); the ADMIN credential is presence-only there too.
        expect(lastJson(sinks.out)).toMatchObject({ token: 's3cret', tokenScope: 'management', adminAuth: 'bearer' });
        expect(JSON.stringify(lastJson(sinks.out))).not.toContain('admin-value-never-printed');
    });

    it('still writes the bundle with --out, because a host-side write is not a deploy', async () => {
        const { probe } = scriptedProbe([]);
        const { deps, sinks, written } = hostDeps(probe);
        await withCwd(PACKAGE_ROOT, async () => {
            expect(await runDeployEndpoint(flagsFor([BOX, '--dry-run', '--out', 'Temp/out.tar.gz']), deps)).toBe(0);
        });
        expect(written.length).toBe(1);
        expect(written[0]!.path).toBe(join(REPO_ROOT, 'Temp', 'out.tar.gz'));
        expect(sinks.out).toContain(`${PREFIX} bundle written: ${join(REPO_ROOT, 'Temp', 'out.tar.gz')} (${written[0]!.buffer.length} bytes)`);
        expect([...(written[0]!.buffer.subarray(0, 2))]).toEqual([0x1f, 0x8b]);
    });

    it('packages without an endpoint at all when --out is the deliverable', async () => {
        const { probe, calls } = scriptedProbe([]);
        const { deps, sinks, written, cargo } = hostDeps(probe);
        const built = await buildProviderPayload(flagsFor(['--out', 'Temp/out.tar.gz', '--dry-run']), deps);
        await withCwd(PACKAGE_ROOT, async () => {
            expect(await runDeployEndpoint(flagsFor(['--out', 'Temp/out.tar.gz']), deps)).toBe(0);
        });
        expect(calls).toEqual([]);
        expect(cargo.calls.map((call) => `${call.command} ${call.args.join(' ')} (cwd ${call.cwd})`)).toEqual([
            `cargo build --release --locked -p typecrate-provider --message-format=json (cwd ${PACKAGE_ROOT})`,
        ]);
        expect(written.length).toBe(1);
        expect(written[0]!.path).toBe(join(REPO_ROOT, 'Temp', 'out.tar.gz'));
        expect(sinks.out.join('\n')).toContain('no endpoint given — the bundle was the deliverable, nothing was deployed');
        expect(lastJson(sinks.out)).toEqual({
            mode: 'deploy',
            endpoint: '',
            name: 'provider',
            status: 'planned',
            publicUrl: null,
            reportedPublicUrl: null,
            path: '/app/provider/',
            profile: 'anonymous',
            filename: 'typecrate-provider.tar.gz',
            notes: [
                `bundle written to ${join(REPO_ROOT, 'Temp', 'out.tar.gz')} (${built.bytes} bytes, 2 members)`,
                'no endpoint — POST it later with `deploy:endpoint <url> --no-build` as app "provider" at /app/provider/',
            ],
        });
    });

    it('refuses a reserved PORT before any probe or cargo, as a local validation error', async () => {
        const { probe, calls } = scriptedProbe([]);
        const { deps, sinks } = hostDeps(probe);
        expect(await main([BOX, '--env', 'PORT=9100'], deps)).toBe(1);
        expect(calls).toEqual([]);
        expect(sinks.err[0]).toBe(`${PREFIX} ✗ --env PORT=9100 is RESERVED: the runner injects PORT from the app's internal port and rejects a user value with 400 config_invalid — pass --port <n> (0 or omitted = auto-allocate) instead`);
    });
});

describe('runDeployEndpoint — a real deploy', () => {
    it('handshakes v2, creates the app "provider", and waits on GET /apps/provider', async () => {
        const { probe, calls } = scriptedProbe([
            ['GET', `${BOX}/health`, OK(HEALTH)],
            ['GET', `${BOX}/apps/provider`, MISSING()],
            ['POST', `${BOX}/apps`, ACCEPTED({ ...APP_READY, revision: 1, status: 'pending', ready: false, internalPort: null })],
            ['GET', `${BOX}/apps/provider`, OK(APP_READY)],
        ]);
        const { deps, sinks, written } = hostDeps(probe);
        const built = await buildProviderPayload(flagsFor([BOX]), deps);
        expect(await runDeployEndpoint(flagsFor([BOX]), deps)).toBe(0);
        expect(written).toEqual([]);

        expect(calls.map((call) => `${call.method} ${call.url}`)).toEqual(['GET https://box.modal.run/health', 'GET https://box.modal.run/apps/provider', 'POST https://box.modal.run/apps', 'GET https://box.modal.run/apps/provider']);

        const post = calls[2]!;
        // No x-runtime-profile anywhere: that absence is the anonymous contract. The
        // NAME and TARGET PREFIX are separate v2 headers, and the config header is
        // compared DECODED because the client re-serialises it.
        const { 'x-runtime-config': header, ...restHeaders } = post.headers;
        expect(restHeaders).toEqual({
            'content-type': 'application/octet-stream',
            'x-runtime-filename': 'typecrate-provider.tar.gz',
            'x-app-name': 'provider',
            'x-app-target-prefix': '/providers/private/v1',
        });
        expect(header).toBe(encodeConfigHeader(JSON.parse(CANONICAL_CONFIG_JSON) as object));
        expect(JSON.parse(Buffer.from(header!, 'base64url').toString('utf8'))).toEqual({
            runCmd: './typecrate-provider-server',
            artifactPath: 'typecrate-provider-server',
            probeTimeoutSec: 120,
            stopSignal: 'SIGTERM',
            probePath: '/health',
            env: { TYPECRATE_PROVIDER_VAULT_PATH: './agentic-provider.json', TYPECRATE_PROVIDER_HOST: '127.0.0.1', RUST_LOG: 'info' },
        });
        expect(post.bodyBytes).toBe(built.bytes);

        expect(lastJson(sinks.out)).toEqual({
            mode: 'deploy',
            endpoint: BOX,
            name: 'provider',
            status: 'ready',
            publicUrl: 'https://box.modal.run/app/provider/',
            reportedPublicUrl: 'https://box.modal.run/app/provider/',
            path: '/app/provider/',
            port: 20043,
            profile: 'anonymous',
            filename: 'typecrate-provider.tar.gz',
            revision: 1,
            notes: [
                'format guard: "typecrate-provider-server" is a Linux ELF',
                'publicUrl: https://box.modal.run/app/provider/ (composed from the runner endpoint and the registry path /app/provider/; the runner also reports https://box.modal.run/app/provider/)',
            ],
        });
        const printed = sinks.out.join('\n');
        expect(printed).toContain('[runtime-deploy] cargo reported the artifact: ' + CARGO_TARGET_BIN);
        expect(printed).toContain(`${PREFIX} ✓ ready — app provider revision 1, internal port 20043`);
        expect(printed).toContain(`${PREFIX}   provider (clients): https://box.modal.run/app/provider/  (mount /app/provider/ → native /providers/private/v1/…)`);
        // The loopback health route is named as INTERNAL, and the gateway alias is
        // explicitly denied: /app/provider/health would map to the prefixed 404.
        expect(printed).toContain(`${PREFIX}   provider health:    the bin's GET /health on the container's LOOPBACK port — NOT exposed as /app/provider/health`);
        expect(printed).not.toContain('GET https://box.modal.run/app/provider/health');
        expect(printed).toContain(`${PREFIX}   management:         GET https://box.modal.run/apps · PUT|DELETE https://box.modal.run/apps/provider · GET https://box.modal.run/apps/provider/logs`);
    });

    it('sends the preview token and the ADMIN bearer together, and keeps them separate', async () => {
        const { probe, calls } = scriptedProbe([
            ['GET', `${BOX}/health`, OK(HEALTH_ADMIN)],
            ['GET', `${BOX}/apps/provider`, MISSING()],
            ['POST', `${BOX}/apps`, ACCEPTED({ ...APP_READY, revision: 1, status: 'pending', ready: false, internalPort: null })],
            ['GET', `${BOX}/apps/provider`, OK(APP_READY)],
        ]);
        const { deps, sinks } = hostDeps(probe);
        expect(await runDeployEndpoint(flagsFor([BOX, '--token', 's3cret', '--admin-token', 'admin-bearer']), deps)).toBe(0);
        for (const call of calls) {
            expect(call.headers['x-daytona-preview-token']).toBe('s3cret');
            expect(call.headers.authorization).toBe('Bearer admin-bearer');
        }
        // Presence only in the summary: the admin VALUE is never echoed.
        const summary = lastJson(sinks.out);
        expect(summary).toMatchObject({ status: 'ready', token: 's3cret', tokenScope: 'management', adminAuth: 'bearer' });
        expect(sinks.out.join('\n')).not.toContain('admin-bearer');
        expect(sinks.out.join('\n')).toContain('the sandbox requires admin auth (capabilities include admin-auth): mutations carry the admin Bearer when one is configured');
    });

    it('replaces an existing app BLUE-GREEN with PUT /apps/provider, and preserves peers', async () => {
        const replacement = { ...APP_READY, revision: 2, pending: null, internalPort: 20044 };
        const { probe, calls } = scriptedProbe([
            ['GET', `${BOX}/health`, OK(HEALTH)],
            ['GET', `${BOX}/apps/provider`, OK(APP_READY)],
            ['PUT', `${BOX}/apps/provider`, ACCEPTED({ ...APP_READY, pending: { revision: 2, status: 'booting' } })],
            ['GET', `${BOX}/apps/provider`, OK(replacement)],
        ]);
        const { deps, sinks } = hostDeps(probe);
        expect(await runDeployEndpoint(flagsFor([BOX]), deps)).toBe(0);
        expect(calls.map((call) => `${call.method} ${call.url}`)).toEqual([
            'GET https://box.modal.run/health',
            'GET https://box.modal.run/apps/provider',
            'PUT https://box.modal.run/apps/provider',
            'GET https://box.modal.run/apps/provider',
        ]);
        // NEVER a root alias, never DELETE /, never a whole-box reset: the replacement is
        // one app-scoped PUT and the previous revision kept serving until revision 2.
        expect(calls.some((call) => call.method === 'DELETE')).toBe(false);
        expect(calls.some((call) => call.method === 'POST')).toBe(false);
        expect(lastJson(sinks.out)).toMatchObject({
            status: 'ready',
            revision: 2,
            port: 20044,
            replaced: { slot: 'ready', profile: 'anonymous', port: 20043, runtimeId: 'app:provider@1' },
            notes: [
                'format guard: "typecrate-provider-server" is a Linux ELF',
                'replace: app=provider revision=1 status=ready port=20043 profile=anonymous filename=typecrate-provider.tar.gz is being replaced blue-green — it keeps serving /app/provider/ until the next revision is ready',
                'publicUrl: https://box.modal.run/app/provider/ (composed from the runner endpoint and the registry path /app/provider/; the runner also reports https://box.modal.run/app/provider/)',
            ],
        });
    });

    it('refuses to touch an existing app with --no-teardown, and says what to do', async () => {
        const { probe } = scriptedProbe([
            ['GET', `${BOX}/health`, OK(HEALTH)],
            ['GET', `${BOX}/apps/provider`, OK(APP_READY)],
        ]);
        const { deps, sinks } = hostDeps(probe);
        expect(await main([BOX, '--no-teardown'], deps)).toBe(1);
        expect(sinks.err.join('\n')).toContain('app "provider" already exists (app=provider revision=1 status=ready port=20043 profile=anonymous filename=typecrate-provider.tar.gz) and replacement is disabled — delete it first (DELETE /apps/provider) or allow replacement');
        expect(lastJson(sinks.out)).toMatchObject({ status: 'busy', name: 'provider', path: '/app/provider/', publicUrl: 'https://box.modal.run/app/provider/' });
    });

    it('reports a failed app with its own log tail from GET /apps/provider/logs, and exits 1', async () => {
        const { probe, calls } = scriptedProbe([
            ['GET', `${BOX}/health`, OK(HEALTH)],
            ['GET', `${BOX}/apps/provider`, MISSING()],
            ['POST', `${BOX}/apps`, ACCEPTED({ ...APP_FAILED, status: 'pending', ready: false, error: null })],
            ['GET', `${BOX}/apps/provider`, OK(APP_FAILED)],
            ['GET', `${BOX}/apps/provider/logs?lines=40`, { status: 200, ok: true, json: null, text: 'booting ./typecrate-provider-server\nerror: vault document refused' }],
        ]);
        const { deps, sinks } = hostDeps(probe);
        expect(await runDeployEndpoint(flagsFor([BOX]), deps)).toBe(1);
        expect(calls.map((call) => `${call.method} ${call.url}`)).toEqual([
            'GET https://box.modal.run/health',
            'GET https://box.modal.run/apps/provider',
            'POST https://box.modal.run/apps',
            'GET https://box.modal.run/apps/provider',
            'GET https://box.modal.run/apps/provider/logs?lines=40',
        ]);
        const printed = sinks.err.join('\n');
        expect(printed).toContain('✗ failed: the app reported status "failed" (vault document refused) — its own log tail is attached');
        expect(printed).toContain('── GET /apps/provider/logs (2 lines) ──');
        expect(printed).toContain('booting ./typecrate-provider-server');
        expect(printed).toContain('the app "provider" may still be registered as failed: rerun this verb to replace it blue-green');
        expect(lastJson(sinks.out)).toMatchObject({
            status: 'failed',
            name: 'provider',
            publicUrl: 'https://box.modal.run/app/provider/',
            path: '/app/provider/',
            notes: [
                'format guard: "typecrate-provider-server" is a Linux ELF',
                'the app reported status "failed" (vault document refused) — its own log tail is attached',
                'log tail attached (2 lines)',
            ],
        });
    });

    it('re-reads the shared log route when the client could not attach a tail, so the reason is visible', async () => {
        const { probe, calls } = scriptedProbe([
            ['GET', `${BOX}/health`, OK(HEALTH)],
            ['GET', `${BOX}/apps/provider`, MISSING()],
            ['POST', `${BOX}/apps`, ACCEPTED({ ...APP_FAILED, status: 'pending', ready: false, error: null })],
            ['GET', `${BOX}/apps/provider`, OK(APP_FAILED)],
            // The client's own best-effort tail MISSES (a 503), and the verb retries once.
            ['GET', `${BOX}/apps/provider/logs?lines=120`, { status: 503, ok: false, json: null, text: 'proxy warming up' }],
            ['GET', `${BOX}/apps/provider/logs?lines=120`, { status: 503, ok: false, json: null, text: 'proxy warming up' }],
        ]);
        const { deps, sinks } = hostDeps(probe);
        expect(await runDeployEndpoint(flagsFor([BOX, '--logs', '120']), deps)).toBe(1);
        expect(calls.length).toBe(6);
        expect(sinks.err.join('\n')).toContain(`${PREFIX} no app log tail: GET /apps/provider/logs → HTTP 503: proxy warming up`);
    });

    it('--teardown unregisters ONLY the app and ships nothing (no cargo, no pack, no POST)', async () => {
        const { probe, calls } = scriptedProbe([
            ['GET', `${BOX}/health`, OK(HEALTH)],
            ['GET', `${BOX}/apps/provider`, OK(APP_READY)],
            ['DELETE', `${BOX}/apps/provider`, OK({ tornDown: true, name: 'provider', removed: true })],
            ['GET', `${BOX}/apps/provider`, MISSING()],
        ]);
        const { deps, sinks, cargo } = hostDeps(probe);
        expect(await runDeployEndpoint(flagsFor([BOX, '--teardown', '--admin-token', 'admin-bearer']), deps)).toBe(0);
        expect(calls.map((call) => `${call.method} ${call.url}`)).toEqual([
            'GET https://box.modal.run/health',
            'GET https://box.modal.run/apps/provider',
            'DELETE https://box.modal.run/apps/provider',
            'GET https://box.modal.run/apps/provider',
        ]);
        expect(calls[2]!.headers.authorization).toBe('Bearer admin-bearer');
        expect(cargo.calls).toEqual([]);
        expect(lastJson(sinks.out)).toEqual({
            mode: 'teardown',
            endpoint: BOX,
            name: 'provider',
            status: 'ready',
            publicUrl: null,
            reportedPublicUrl: null,
            path: null,
            notes: ['DELETE /apps/provider — the runner keeps serving every other app'],
            adminAuth: 'bearer',
            replaced: { slot: 'ready', profile: 'anonymous', port: 20043, runtimeId: 'app:provider@1' },
            removed: true,
        });
    });

    it('--teardown of an UNREGISTERED app is idempotent success (removed:false)', async () => {
        const { probe, calls } = scriptedProbe([
            ['GET', `${BOX}/health`, OK(HEALTH)],
            ['GET', `${BOX}/apps/provider`, MISSING()],
        ]);
        const { deps, sinks } = hostDeps(probe);
        expect(await runDeployEndpoint(flagsFor([BOX, '--teardown']), deps)).toBe(0);
        expect(calls.map((call) => call.method)).toEqual(['GET', 'GET']);
        expect(JSON.parse(sinks.out[sinks.out.length - 1]!)).toEqual({
            mode: 'teardown',
            endpoint: BOX,
            name: 'provider',
            status: 'ready',
            publicUrl: null,
            reportedPublicUrl: null,
            path: null,
            notes: ['DELETE /apps/provider — the runner keeps serving every other app'],
            removed: false,
        });
    });
});

describe('main', () => {
    it('prints USAGE and exits 0 on --help, touching no dep', async () => {
        const { probe, calls } = scriptedProbe([]);
        const { deps, sinks } = hostDeps(probe);
        expect(await main(['--help'], deps)).toBe(0);
        expect(calls).toEqual([]);
        expect(sinks.out).toEqual([USAGE]);
    });

    it('maps a broken command line to exit 1 plus the usage screen', async () => {
        const { probe } = scriptedProbe([]);
        const { deps, sinks } = hostDeps(probe);
        expect(await main(['--nope'], deps)).toBe(1);
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
        expect(await main([BOX, '--dry-run'], deps)).toBe(1);
        expect(sinks.err[0]).toContain('✗ unexpected failure:');
        expect(sinks.err[0]).toContain('disk on fire');
    });

    it('documents the v2 surface: the app name, targetPrefix, auto port and the denied health alias', () => {
        expect(USAGE).toContain('registers it as the app "provider"');
        expect(USAGE).toContain('targetPrefix "/providers/private/v1"');
        expect(USAGE).toContain('/app/provider/models maps to the bin\'s native /providers/private/v1/models');
        expect(USAGE).toContain('--port <n>            OPTIONAL internal loopback port');
        expect(USAGE).toContain('--admin-token <t>');
        expect(USAGE).toContain('--admin-token-file <path>');
        expect(USAGE).toContain('PUT /apps/provider');
        expect(USAGE).toContain('DELETE /apps/provider only');
        // Health is loopback-only: the usage must say so, not advertise the mount alias.
        expect(USAGE).toContain('never');
        expect(USAGE).toContain('/app/provider/health, which would map to /providers/private/v1/health and 404');
        expect(USAGE).not.toContain('runtime contract v1');
        expect(USAGE).not.toContain('DELETE / only');
        expect(USAGE).not.toContain('GET /status');
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
        expect(isCliEntryPoint({ modulePath: verb, argv: ['node', verb] })).toBe(true);
        expect(isCliEntryPoint({ modulePath: verb, argv: ['node', 'vite-node', '--script', 'scripts/deploy-endpoint.ts'] })).toBe(true);
        expect(isCliEntryPoint({ modulePath: verb, env: { npm_lifecycle_script: 'vite-node scripts/deploy-endpoint.ts' } })).toBe(true);
        expect(isCliEntryPoint({ modulePath: verb, env: { npm_lifecycle_script: 'vite-node scripts/start-modal.ts' } })).toBe(false);
        expect(isCliEntryPoint({ modulePath: verb, argv: ['node', '/repo/packages/typecrate/provider/scripts/deploy-endpoint.test.ts'] })).toBe(false);
    });

    it('never fires under vitest', () => {
        expect(isCliEntryPoint({ modulePath: verb, vitest: true, argv: ['node', verb] })).toBe(false);
    });

    it('fires only when NOTHING but the runner sits above the module', () => {
        expect(isCliEntryPoint({ modulePath: verb, stack: `Error\n    at Object.<anonymous> (${verb}:1:1)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(true);
        expect(isCliEntryPoint({ modulePath: verb, stack: `Error\n    at Object.<anonymous> (${verb}:1:1)\n    at mainModal (${wrapper}:3:3)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(false);
        expect(isCliEntryPoint({ modulePath: wrapper, stack: `Error\n    at Object.<anonymous> (${wrapper}:1:1)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(true);
        expect(isCliEntryPoint({ modulePath: '/repo/packages/typecrate/provider/scripts/start-daytona.ts', env: { npm_lifecycle_script: 'vite-node scripts/start-daytona.ts' } })).toBe(true);
    });

    it('does nothing at all with no signals', () => {
        expect(isCliEntryPoint({ modulePath: verb })).toBe(false);
    });
});
