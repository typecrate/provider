// ─────────────────────────────────────────────────────────────────────────────
//  start-daytona.test.ts — create + register + report, with NO Daytona API, NO cargo
//  spawn and NO network.
//
//  `start:daytona` is the only verb that BRINGS UP A MACHINE, so the tests below are
//  built around the money-shaped rules:
//    · parser      — the CREATE knobs are consumed here and every other token is
//                    forwarded to the deploy verb unchanged (one meaning per flag
//                    across both verbs); a value knob that gets no value, a
//                    non-integer, a 0 where 1 is the floor, and the two combinations
//                    that belong to the OTHER verb (--teardown, an endpoint) are errors
//    · create opts — `port` is DELIBERATELY ABSENT. `createSandbox({port})` selects the
//                    RUNNER's port and therefore the management endpoint, never the
//                    payload's; the provider's own port is auto-allocated by the v2
//                    runner. Unset knobs are omitted, not sent as undefined. The ADMIN
//                    credential is passed in its preferred form (file path first).
//    · dry run     — provably zero Daytona calls (the injected `create`/`ship` THROW),
//                    zero cargo, zero probes, and the plan still shows the payload
//    · real run    — the deploy goes to the MANAGEMENT endpoint with the token
//                    createSandbox returned, and the provider's base is that SAME origin
//                    plus /app/provider/ — there is NO second getPreviewLink call and no
//                    per-port token in v2
//    · lifecycle   — never deleted on success; deleted on a failed deploy, with
//                    --keep-on-fail as the opt-out and a failed delete reported in the
//                    notes as well as on stderr; the billing recipe is printed either way
//    · summary     — the LAST line is the create+deploy JSON with sandboxId and the
//                    one-origin app URL; `port` is the INTERNAL loopback port, never a
//                    public one
// ─────────────────────────────────────────────────────────────────────────────

import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

import type { CreateSandboxOptions, DeployResult, FsAccess, Probe } from '@services/daytona/sandbox';

import { APP_NAME, APP_PATH, DEFAULT_VAULT_PATH, PACKAGE_ROOT, PREFIX, buildProviderPayload, parseArgs } from './deploy-endpoint';
import {
    ENTRY_SCRIPT_NAME_DAYTONA,
    RUNNER_PORT,
    USAGE_DAYTONA,
    type CreatedSandbox,
    type DaytonaDeps,
    buildCreateOptions,
    buildDaytonaSummary,
    buildPlannedSummary,
    deleteRecipe,
    mainDaytona,
    parseArgsDaytona,
    runStartDaytona,
} from './start-daytona';

// ══════════════════════════════════════════════════════════════════════════════
//  FAKES — a file table, a Daytona that records, and a ship that records
// ══════════════════════════════════════════════════════════════════════════════

const ELF = Buffer.concat([Buffer.from([0x7f, 0x45, 0x4c, 0x46]), Buffer.from('provider-server-image', 'utf8')]);
const VAULT_DOC = '{"consumers":["agentic-provider"],"keys":["sk-test"]}';
const ADMIN_FILE = join(PACKAGE_ROOT, 'Temp', 'admin.token');
/** Absolute paths, derived from the verb's OWN constants so a rename breaks a test, not a deploy. */
const REPO = join(PACKAGE_ROOT, '..', '..', '..');
const BIN_PATH = join(REPO, '.rust', 'target', 'release', 'typecrate-provider-server');
const VAULT_PATH = DEFAULT_VAULT_PATH;

const hostFs = (): FsAccess => {
    const table = new Map<string, Buffer>([
        [BIN_PATH, ELF],
        [VAULT_PATH, Buffer.from(VAULT_DOC, 'utf8')],
        [join(REPO, '.cargo', 'config.toml'), Buffer.from('[build]\ntarget-dir = ".rust/target"\n', 'utf8')],
        [ADMIN_FILE, Buffer.from('admin-token-9e2c\n', 'utf8')],
    ]);
    const bytesOf = (path: string): Buffer | undefined => table.get(path);
    return {
        exists: (path) => table.has(path),
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

/** A dry run must never reach the network, so any probe is a loud failure. */
const neverProbed: Probe = async (method, url) => {
    throw new Error(`start:daytona touched the network: ${method} ${url}`);
};

/** The management endpoint and its token, exactly as `createSandbox` hands them back. */
const MANAGEMENT = 'https://7777-sb-1.preview.daytona.run';
const MANAGEMENT_TOKEN = 'mgmt-token-9e2c';
/** The app's base on the ONE origin: no second preview link exists in v2. */
const PROVIDER_URL = `${MANAGEMENT}${APP_PATH}`;

/** The sandbox handle: only `delete` is used (there is no per-port link to mint). */
const fakeSandbox = (options: { deleteFails?: string } = {}) => {
    const deletions: number[] = [];
    const sandbox = {
        delete: async () => {
            deletions.push(1);
            if (options.deleteFails) throw new Error(options.deleteFails);
            return { deleted: true };
        },
    };
    const created: CreatedSandbox = { endpoint: MANAGEMENT, token: MANAGEMENT_TOKEN, sandboxId: 'sb-1', name: 'noobscript-runtime-20261119-090000', port: RUNNER_PORT, public: false, sandbox };
    return { created, sandbox, deletions };
};

/** The client's own summary line, for a fake result the verb prints verbatim. */
const clientSummary = (status: string, endpoint: string, notes: string[]): string =>
    JSON.stringify({
        mode: 'deploy',
        endpoint,
        name: APP_NAME,
        status,
        publicUrl: PROVIDER_URL,
        reportedPublicUrl: status === 'ready' ? PROVIDER_URL : null,
        path: APP_PATH,
        port: 20043,
        profile: 'anonymous',
        filename: 'typecrate-provider.tar.gz',
        notes,
    });

/** A DeployResult as the shared v2 client returns it: the app base is one origin + path. */
const readyResult = (notes: string[] = []): DeployResult => ({
    ok: true,
    status: 'ready',
    slot: 'ready',
    app: null,
    runtime: null,
    publicUrl: PROVIDER_URL,
    reportedPublicUrl: PROVIDER_URL,
    path: APP_PATH,
    name: APP_NAME,
    notes,
    endpoint: MANAGEMENT,
    port: 20043,
    filename: 'typecrate-provider.tar.gz',
    profile: 'anonymous',
    revision: 1,
    replaced: null,
    recycled: null,
    summary: clientSummary('ready', MANAGEMENT, notes),
});

const failedResult = (): DeployResult => ({
    ...readyResult(['the app reported status "failed" — its own log tail is attached']),
    ok: false,
    status: 'failed',
    slot: 'failed',
    publicUrl: PROVIDER_URL,
    reportedPublicUrl: null,
    error: 'the app never became ready',
    logs: 'booting ./typecrate-provider-server\nerror: vault document refused',
});

/** The whole Daytona world: recorded create/ship calls, sinks, and a cargo recorder. */
const daytonaHost = (options: { createFails?: string; deleteFails?: string; result?: DeployResult } = {}) => {
    const out: string[] = [];
    const err: string[] = [];
    const cargo: string[] = [];
    const sandbox = fakeSandbox(options);
    const creates: CreateSandboxOptions[] = [];
    const ships: Array<{ endpoint: string; token: string; adminToken: string; filename: string; config: unknown; name?: string; targetPrefix?: string; bufferBytes: number }> = [];
    const deps: DaytonaDeps = {
        probe: neverProbed,
        fs: hostFs(),
        runCommand: async (command, args) => {
            cargo.push(`${command} ${args.join(' ')}`);
            return { status: 0, stdout: '', stderr: '' };
        },
        env: {},
        sleep: async () => undefined,
        log: (text) => out.push(text),
        error: (text) => err.push(text),
        create: async (createOptions) => {
            creates.push(createOptions);
            if (options.createFails) throw new Error(options.createFails);
            return sandbox.created;
        },
        ship: async (endpoint, payload, flags) => {
            ships.push({ endpoint, token: flags.token, adminToken: flags.adminToken, filename: payload.filename, config: payload.config, name: payload.name, targetPrefix: payload.targetPrefix, bufferBytes: payload.buffer.length });
            return options.result ?? readyResult();
        },
    };
    return { deps, out, err, cargo, creates, ships, sandbox };
};

/** Flags for the flow tests, parsed by the verb's own two-stage parser. */
const flagsFor = (argv: readonly string[]) => parseArgsDaytona(argv, {});

// ══════════════════════════════════════════════════════════════════════════════
//  THE CREATE KNOBS — parsed here, forwarded nowhere
// ══════════════════════════════════════════════════════════════════════════════

describe('parseArgsDaytona', () => {
    it('starts public, tokenless, unbudgeted, with no port and no endpoint at all', () => {
        const { daytona, deploy, errors } = flagsFor([]);
        expect(errors).toEqual([]);
        expect(daytona).toEqual({ public: true, keepOnFail: false });
        expect(deploy.endpoint).toBe('');
        // v2: the payload's internal port is auto-allocated unless named.
        expect(deploy.port).toBeUndefined();
        expect(deploy.dryRun).toBe(false);
        expect(deploy.wait).toBe(true);
    });

    it('consumes the create knobs and forwards every deploy knob verbatim', () => {
        const { daytona, deploy, errors } = flagsFor(['--private', '--cpu', '2', '--mem', '4', '--disk', '20', '--ttl', '0', '--name', 'provider-box', '--keep-on-fail', '--port', '9000', '--env', 'RUST_LOG=debug', '--dry-run']);
        expect(errors).toEqual([]);
        expect(daytona).toEqual({ public: false, keepOnFail: true, cpu: 2, mem: 4, disk: 20, ttlMinutes: 0, name: 'provider-box' });
        // `--port` is the PROVIDER's internal port, and it stays with the deploy verb.
        expect(deploy.port).toBe(9000);
        expect(deploy.env).toEqual({ RUST_LOG: 'debug' });
        expect(deploy.dryRun).toBe(true);
    });

    it('keeps the ADMIN credential on the deploy side, where create and HTTP both read it', () => {
        const viaToken = flagsFor(['--admin-token', 'at']).deploy;
        expect(viaToken.adminToken).toBe('at');
        expect(viaToken.adminTokenFile).toBe('');
        const viaFile = flagsFor(['--admin-token-file', '/host/admin.token']).deploy;
        expect(viaFile.adminTokenFile).toBe('/host/admin.token');
        expect(viaFile.adminToken).toBe('');
        // Neither is a CREATE knob: the create options are built from this same value.
        expect(flagsFor(['--admin-token', 'at']).daytona).toEqual({ public: true, keepOnFail: false });
    });

    it('lets the LAST of --public/--private win, like any other pair of flags', () => {
        expect(flagsFor(['--private', '--public']).daytona.public).toBe(true);
        expect(flagsFor(['--public', '--private']).daytona.public).toBe(false);
    });

    it('reports each bad value instead of creating the wrong machine', () => {
        const ttl = flagsFor(['--ttl', 'soon']);
        expect(ttl.errors).toEqual(['--ttl "soon" is not an integer >= 0']);
        expect(ttl.daytona).toEqual({ public: true, keepOnFail: false });

        const cpu = flagsFor(['--cpu', '0']);
        expect(cpu.errors).toEqual(['--cpu "0" is not an integer >= 1']);

        expect(flagsFor(['--name', '--private']).errors).toEqual(['--name needs a value']);
        expect(flagsFor(['--ttl']).errors).toEqual(['--ttl needs a value']);
    });

    it('refuses what belongs to deploy:endpoint, before Daytona is called', () => {
        expect(flagsFor(['--teardown']).errors).toEqual([`start:daytona always creates a sandbox — --teardown (unregister the app, ship nothing) is deploy:endpoint's verb`]);
        expect(flagsFor(['https://box.modal.run']).errors).toEqual(['start:daytona mints its own endpoint — the endpoint argument/--endpoint ("https://box.modal.run") belongs to deploy:endpoint']);
        expect(flagsFor(['--endpoint', 'https://box.modal.run']).errors).toEqual(['start:daytona mints its own endpoint — the endpoint argument/--endpoint ("https://box.modal.run") belongs to deploy:endpoint']);
        expect(flagsFor(['https://box.modal.run']).deploy.endpoint).toBe('');
    });

    it('keeps --out as a host-side write ALONGSIDE the deploy', () => {
        const { deploy, errors } = flagsFor(['--out', 'Temp/provider.tar.gz']);
        expect(errors).toEqual([]);
        expect(deploy.out).toBe('Temp/provider.tar.gz');
        expect(deploy.endpoint).toBe('');
    });
});

describe('buildCreateOptions', () => {
    it('names no port: the runner keeps 7777 and that is the management endpoint', () => {
        const options = buildCreateOptions({ public: true, keepOnFail: false });
        expect(options).toEqual({ public: true });
        expect('port' in options).toBe(false);
        expect('cpu' in options).toBe(false);
        expect('ttlMinutes' in options).toBe(false);
        // No credential configured ⇒ no admin key at all.
        expect('adminToken' in options).toBe(false);
        expect('adminTokenFile' in options).toBe(false);
    });

    it('maps every knob, including a ttl of 0', () => {
        const { daytona } = flagsFor(['--private', '--cpu', '2', '--mem', '4', '--disk', '20', '--ttl', '0', '--name', 'provider-box']);
        expect(buildCreateOptions(daytona)).toEqual({ public: false, cpu: 2, mem: 4, disk: 20, ttlMinutes: 0, name: 'provider-box' });
    });

    it('passes the admin credential to the fresh sandbox, FILE FORM FIRST', () => {
        const daytona = { public: false, keepOnFail: false };
        // The file form is preferred: only the PATH reaches createSandbox, and the
        // token itself is uploaded 0600 by the library.
        expect(buildCreateOptions(daytona, { adminToken: 'at', adminTokenFile: '/host/admin.token' })).toEqual({ public: false, adminTokenFile: '/host/admin.token' });
        // With no file, the resolved env-form value is passed explicitly.
        expect(buildCreateOptions(daytona, { adminToken: 'at' })).toEqual({ public: false, adminToken: 'at' });
    });

    it('pins the runner port this verb refuses to override', () => {
        expect(RUNNER_PORT).toBe(7777);
        expect(ENTRY_SCRIPT_NAME_DAYTONA).toBe('start-daytona.ts');
        expect(APP_NAME).toBe('provider');
        expect(APP_PATH).toBe('/app/provider/');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  THE MACHINE-READABLE LINES
// ══════════════════════════════════════════════════════════════════════════════

describe('summaries and recipes', () => {
    it('prints the billing recipe, and the private-sandbox caveat only when it applies', () => {
        expect(deleteRecipe('sb-1', true)).toEqual(['the sandbox IS BILLING until it is deleted (this verb never deletes on success):', '  yarn workspace @services/daytona sandbox down -- --sandbox sb-1']);
        expect(deleteRecipe('sb-1', false)).toEqual([
            'the sandbox IS BILLING until it is deleted (this verb never deletes on success):',
            '  yarn workspace @services/daytona sandbox down -- --sandbox sb-1',
            '  (a private sandbox also loses its preview token when it goes)',
        ]);
    });

    it('reports the create+deploy shape, with an INTERNAL port and the one-origin app URL', () => {
        expect(buildDaytonaSummary({ sandboxId: 'sb-1', name: 'box', management: MANAGEMENT, status: 'ready', port: 20043, public: false, providerUrl: PROVIDER_URL, providerToken: MANAGEMENT_TOKEN, notes: ['n'] })).toBe(
            `{"mode":"create+deploy","sandboxId":"sb-1","name":"box","endpoint":"${MANAGEMENT}","status":"ready","port":20043,"public":false,"providerUrl":"${PROVIDER_URL}","providerToken":"${MANAGEMENT_TOKEN}","tokenScope":"preview-proxy","notes":["n"]}`,
        );
        // No token ⇒ no token fields at all; no port ⇒ the key is ABSENT (it is internal
        // and "not assigned yet" is not a public port).
        expect(buildDaytonaSummary({ sandboxId: 'sb-2', name: 'box', management: MANAGEMENT, status: 'ready', public: true, providerUrl: PROVIDER_URL, notes: [] })).toBe(
            `{"mode":"create+deploy","sandboxId":"sb-2","name":"box","endpoint":"${MANAGEMENT}","status":"ready","public":true,"providerUrl":"${PROVIDER_URL}","notes":[]}`,
        );
    });

    it('plans the same shape it will later produce, with no port unless the operator named one', () => {
        const { daytona, deploy } = flagsFor(['--private', '--port', '9000']);
        expect(buildPlannedSummary(daytona, deploy, [])).toBe(
            '{"mode":"create+deploy","sandboxId":"<not created>","name":"<not created>","endpoint":"<Daytona preview link, runner port 7777>","status":"planned","port":9000,"public":false,"providerUrl":null,"notes":[]}',
        );
        expect(buildPlannedSummary({ public: true, keepOnFail: false }, flagsFor([]).deploy, [])).toBe(
            '{"mode":"create+deploy","sandboxId":"<not created>","name":"<not created>","endpoint":"<Daytona preview link, runner port 7777>","status":"planned","public":true,"providerUrl":null,"notes":[]}',
        );
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  THE VERB
// ══════════════════════════════════════════════════════════════════════════════

describe('runStartDaytona — dry run', () => {
    it('prints the whole plan while Daytona, cargo and the network are never touched', async () => {
        const { daytona, deploy } = flagsFor(['--dry-run']);
        // A throwing create/ship is stronger than a recorder: the exit code alone proves
        // they were never reached.
        const hostWorld = daytonaHost({ createFails: 'createSandbox must not run in a dry run' });
        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(0);
        expect(hostWorld.cargo).toEqual([]);
        expect(hostWorld.err).toEqual([]);

        const printed = hostWorld.out.join('\n');
        expect(printed).toContain('DRY RUN — no probe, no PUT, no DELETE, no POST, no cargo, no network');
        expect(printed).toContain('no createSandbox — Daytona was never called');
        expect(hostWorld.out).toContain(`${PREFIX} create:   {"public":true}  (the management endpoint is its 7777 preview link)`);
        expect(hostWorld.out).toContain(`${PREFIX} admin:    no admin Bearer`);
        expect(hostWorld.out).toContain(`${PREFIX} provider: <that endpoint>/app/provider/ after the deploy — ONE origin, no per-port preview link to mint`);
        // The v2 app identity/targetPrefix are in the plan, not only in the deploy verb.
        expect(printed).toContain('app:       "provider" — clients use <endpoint>/app/provider/… ; native prefix /providers/private/v1');
        // The bundle the plan describes is the real one, not a stand-in.
        expect(printed).toContain('0755  typecrate-provider-server');
        expect(printed).toContain('0644  agentic-provider.json');
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toEqual({
            mode: 'create+deploy',
            sandboxId: '<not created>',
            name: '<not created>',
            endpoint: '<Daytona preview link, runner port 7777>',
            status: 'planned',
            public: true,
            providerUrl: null,
            notes: ['no createSandbox — Daytona was never called'],
        });
    });

    it('carries an explicit internal port into the plan, and never into the create options', async () => {
        const { daytona, deploy } = flagsFor(['--dry-run', '--port', '9100']);
        const hostWorld = daytonaHost();
        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(0);
        expect(hostWorld.out).toContain(`${PREFIX} create:   {"public":true}  (the management endpoint is its 7777 preview link)`);
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toMatchObject({ port: 9100 });
        expect(hostWorld.creates).toEqual([]);
    });

    it('never prints the admin credential in the plan, only its presence', async () => {
        const { daytona, deploy } = flagsFor(['--dry-run', '--admin-token', 'admin-value-never-printed']);
        const hostWorld = daytonaHost();
        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(0);
        expect(hostWorld.out).toContain(`${PREFIX} admin:    admin Bearer configured (value never printed)`);
        expect(hostWorld.out.join('\n')).not.toContain('admin-value-never-printed');
    });
});

describe('runStartDaytona — a real deploy', () => {
    it('registers the app on the MANAGEMENT endpoint and reports the SAME origin + /app/provider/', async () => {
        const { daytona, deploy } = flagsFor(['--private']);
        const hostWorld = daytonaHost();
        const built = await buildProviderPayload(parseArgs(['https://x', '--dry-run'], {}).flags, hostWorld.deps);

        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(0);
        // The create call gets exactly the knobs the operator named — no port.
        expect(hostWorld.creates).toEqual([{ public: false }]);
        // The POST/PUT target is the runner's origin, with the token createSandbox returned.
        expect(hostWorld.ships).toEqual([
            {
                endpoint: MANAGEMENT,
                token: MANAGEMENT_TOKEN,
                adminToken: '',
                filename: 'typecrate-provider.tar.gz',
                config: built.payload.config,
                name: 'provider',
                targetPrefix: '/providers/private/v1',
                bufferBytes: built.payload.buffer.length,
            },
        ]);
        expect(hostWorld.ships[0]!.config).toEqual({
            runCmd: './typecrate-provider-server',
            artifactPath: 'typecrate-provider-server',
            probeTimeoutSec: 120,
            stopSignal: 'SIGTERM',
            probePath: '/health',
            env: { TYPECRATE_PROVIDER_VAULT_PATH: './agentic-provider.json', TYPECRATE_PROVIDER_HOST: '127.0.0.1', RUST_LOG: 'info' },
        });
        // Success never deletes: a serving provider is the thing that was asked for.
        expect(hostWorld.sandbox.deletions).toEqual([]);

        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toEqual({
            mode: 'create+deploy',
            sandboxId: 'sb-1',
            name: 'noobscript-runtime-20261119-090000',
            endpoint: MANAGEMENT,
            status: 'ready',
            port: 20043,
            public: false,
            providerUrl: PROVIDER_URL,
            providerToken: MANAGEMENT_TOKEN,
            tokenScope: 'preview-proxy',
            notes: [],
        });
        const printed = hostWorld.out.join('\n');
        expect(printed).toContain(`${PREFIX} ✓ ready — app "provider" revision 1, provider INTERNAL port 20043 (never public)`);
        expect(printed).toContain(`${PREFIX}   provider (clients): ${PROVIDER_URL}`);
        expect(printed).toContain(`${PREFIX}   mount:              /app/provider/ → native /providers/private/v1/… (targetPrefix)`);
        // The health route is loopback-only and the mount alias is DENIED, not advertised.
        expect(printed).toContain(`${PREFIX}   provider health:    the bin's GET /health on the container LOOPBACK (TYPECRATE_PROVIDER_HOST=127.0.0.1) — NOT /app/provider/health`);
        expect(printed).not.toContain('GET /app/provider/health');
        expect(printed).toContain(`${PREFIX}   management:         GET ${MANAGEMENT}/apps · PUT|DELETE ${MANAGEMENT}/apps/provider · GET ${MANAGEMENT}/apps/provider/logs`);
        expect(printed).toContain(`${PREFIX}   yarn workspace @services/daytona sandbox down -- --sandbox sb-1`);
        // The credential-in-the-bundle rule is printed where the operator can see it.
        expect(printed).toContain(`${PREFIX}   artifact:           ${BIN_PATH} (candidate) + ${VAULT_PATH} INSIDE the bundle`);
        // No per-port link/token story anywhere: one origin, one preview token.
        expect(printed).not.toContain('getPreviewLink');
        expect(printed).not.toContain('8800');
    });

    it('takes --token over the created sandbox token when the operator names one', async () => {
        const { daytona, deploy } = flagsFor(['--token', 'operator-token']);
        const hostWorld = daytonaHost();
        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(0);
        expect(hostWorld.ships[0]!.token).toBe('operator-token');
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toMatchObject({ providerToken: 'operator-token' });
    });

    it('flows the ADMIN credential into the sandbox options and every deploy/log/delete call', async () => {
        const { daytona, deploy } = flagsFor(['--private', '--admin-token-file', ADMIN_FILE]);
        const hostWorld = daytonaHost();
        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(0);
        // The fresh sandbox is configured with the FILE form (the token is uploaded 0600),
        // while the HTTP layer receives the resolved value for its Bearer header.
        expect(hostWorld.creates).toEqual([{ public: false, adminTokenFile: ADMIN_FILE }]);
        expect(hostWorld.ships[0]!.adminToken).toBe('admin-token-9e2c');
        // Presence only, in both the human line and the machine line.
        expect(hostWorld.out[0]).toBe(`${PREFIX} createSandbox {"public":false} — the runner boots on 7777; this deploy registers into that origin · admin Bearer configured (value never printed)`);
        expect(hostWorld.out.join('\n')).not.toContain('admin-token-9e2c');
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).not.toHaveProperty('adminAuth');
    });

    it('deletes a sandbox whose deploy FAILED, and still reports the client summary', async () => {
        const { daytona, deploy } = flagsFor([]);
        const hostWorld = daytonaHost({ result: failedResult() });
        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(1);
        expect(hostWorld.sandbox.deletions).toEqual([1]);
        expect(hostWorld.err).toEqual([
            `${PREFIX} ✗ failed: the app never became ready`,
            `${PREFIX} note: the app reported status "failed" — its own log tail is attached`,
            `${PREFIX} ── GET /apps/provider/logs (2 lines) ──`,
            `${PREFIX} booting ./typecrate-provider-server`,
            `${PREFIX} error: vault document refused`,
            `${PREFIX} ✓ sandbox sb-1 deleted — a failed app is not worth billing for (--keep-on-fail keeps it)`,
        ]);
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toEqual({
            mode: 'create+deploy',
            sandboxId: 'sb-1',
            name: 'noobscript-runtime-20261119-090000',
            endpoint: MANAGEMENT,
            status: 'failed',
            port: 20043,
            public: false,
            providerUrl: null,
            notes: ['the app reported status "failed" — its own log tail is attached'],
        });
    });

    it('leaves the machine alone with --keep-on-fail, and prints how to delete it', async () => {
        const { daytona, deploy } = flagsFor(['--keep-on-fail']);
        const hostWorld = daytonaHost({ result: failedResult() });
        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(1);
        expect(hostWorld.sandbox.deletions).toEqual([]);
        expect(hostWorld.err.slice(-4)).toEqual([
            `${PREFIX} --keep-on-fail: sandbox sb-1 is left alive for log reading`,
            `${PREFIX} the sandbox IS BILLING until it is deleted (this verb never deletes on success):`,
            `${PREFIX}   yarn workspace @services/daytona sandbox down -- --sandbox sb-1`,
            `${PREFIX}   (a private sandbox also loses its preview token when it goes)`,
        ]);
    });

    it('says out loud when a delete fails, because that is still money', async () => {
        const { daytona, deploy } = flagsFor([]);
        const hostWorld = daytonaHost({ result: failedResult(), deleteFails: 'daytona api 503' });
        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(1);
        expect(hostWorld.err).toContain(`${PREFIX} ✗ sandbox delete failed (daytona api 503) — delete it yourself:`);
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toMatchObject({
            status: 'failed',
            providerUrl: null,
            notes: ['the app reported status "failed" — its own log tail is attached', 'sandbox.delete() failed — the sandbox is still billing'],
        });
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  mainDaytona — parse first, pay later
// ══════════════════════════════════════════════════════════════════════════════════

describe('mainDaytona', () => {
    it('prints its own usage for --help, and calls Daytona for nothing', async () => {
        const hostWorld = daytonaHost({ createFails: 'Daytona was never supposed to be called' });
        expect(await mainDaytona(['--help'], hostWorld.deps, {})).toBe(0);
        expect(hostWorld.out).toEqual([USAGE_DAYTONA]);
        expect(hostWorld.creates).toEqual([]);
        expect(hostWorld.cargo).toEqual([]);
    });

    it('refuses a contradiction without spending money on it', async () => {
        const hostWorld = daytonaHost({ createFails: 'Daytona was never supposed to be called' });
        expect(await mainDaytona(['--teardown'], hostWorld.deps, {})).toBe(1);
        expect(hostWorld.err).toEqual([
            `${PREFIX} ✗ start:daytona always creates a sandbox — --teardown (unregister the app, ship nothing) is deploy:endpoint's verb`,
            '',
            USAGE_DAYTONA,
        ]);
        expect(hostWorld.creates).toEqual([]);
    });

    it('reaches Daytona only past the parser, and reports a create failure as one line', async () => {
        const hostWorld = daytonaHost({ createFails: 'DAYTONA_API_KEY is not set' });
        expect(await mainDaytona([], hostWorld.deps, {})).toBe(1);
        expect(hostWorld.creates).toEqual([{ public: true }]);
        expect(hostWorld.err).toEqual([
            `${PREFIX} ✗ createSandbox failed: DAYTONA_API_KEY is not set`,
            `${PREFIX} no sandbox was created, so nothing is billing (a half-built sandbox is deleted by createSandbox itself)`,
        ]);
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toEqual({
            mode: 'create+deploy',
            sandboxId: '<not created>',
            name: '<not created>',
            endpoint: '<Daytona preview link, runner port 7777>',
            status: 'failed',
            public: true,
            providerUrl: null,
            notes: ['createSandbox failed: DAYTONA_API_KEY is not set'],
        });
    });

    it('names the usage screen it documents, so --help and the README cannot drift apart', () => {
        expect(USAGE_DAYTONA.split('\n')[0]).toBe('usage: yarn workspace @typecrate/provider start:daytona [flags]');
        expect(USAGE_DAYTONA).toContain('ONE origin and no second preview link');
        expect(USAGE_DAYTONA).toContain('--ttl <minutes>     Daytona\'s kill fence; default NONE, so the sandbox bills until deleted');
        expect(USAGE_DAYTONA).toContain('--admin-token <t>   the runner\'s admin Bearer');
        expect(USAGE_DAYTONA).toContain('OPTIONAL internal loopback port');
        // v1 advice must be gone: no fixed payload port, no second link.
        expect(USAGE_DAYTONA).not.toContain('preview link is minted after the deploy');
        expect(USAGE_DAYTONA).not.toContain('default 8800');
    });
});
