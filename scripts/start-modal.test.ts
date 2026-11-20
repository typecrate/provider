// ─────────────────────────────────────────────────────────────────────────────
//  start-modal.test.ts — the Modal wrapper: WHICH box, and nothing else.
//
//  start-modal.ts owns exactly one decision (the endpoint, by precedence) and
//  delegates every byte of the deploy to scripts/deploy-endpoint.ts, so these tests
//  pin the decision as data AND drive `mainModal` end to end over an injected world
//  (in-memory fs, recorded cargo, scripted endpoint answers) — zero network, zero
//  cargo spawn, zero Modal calls:
//    · precedence — argument / --endpoint / TYPECRATE_PROVIDER_ENDPOINT /
//      @config/vault modalContainer, in that order, each with the vault map PRESENT
//      so a fall-through is never mistaken for the vault winning (the reference
//      package's bug: its start-modal always prepends the vault URL, so a positional
//      URL has to AGREE with the repo's box — start-modal.ts:12-25)
//    · the guard rails — `--help` and a broken command line must NOT inject an
//      endpoint (a typo would otherwise hide behind a deploy), and an empty vault map
//      must fall through to the verb's own "no endpoint" error
//    · provenance — the one line that names WHERE the endpoint came from, because
//      "which box am I about to displace?" must never be a guess
//    · the real default — the vault map this package imports is the one
//      config/vault generates, pinned here so a rename upstream breaks a test
//  The entry guard itself is tested in deploy-endpoint.test.ts (one implementation,
//  one suite); what is tested HERE is that importing THIS file started nothing.
// ─────────────────────────────────────────────────────────────────────────────

import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

import type { FsAccess, Probe } from '@services/daytona/sandbox';

import { DEFAULT_VAULT_PATH, ENDPOINT_ENV, PACKAGE_ROOT, PREFIX, USAGE, type ProviderDeps, buildProviderPayload, parseArgs } from './deploy-endpoint';
import { ENTRY_SCRIPT_NAME_MODAL, VAULT_BOXES, type EndpointSource, mainModal, resolveEndpoint, vaultBox } from './start-modal';

// ══════════════════════════════════════════════════════════════════════════════
//  FAKES — the same injected world the verb's own suite uses, kept local so this
//  file stays runnable on its own and no fixture is shared between suites
// ══════════════════════════════════════════════════════════════════════════════

/** A Linux ELF: the magic plus a body — enough for every guard on the path. */
const ELF = Buffer.concat([Buffer.from([0x7f, 0x45, 0x4c, 0x46]), Buffer.from('provider-server-image', 'utf8')]);
const VAULT_DOC = '{"consumers":["agentic-provider"],"keys":["sk-test"]}';

/**
 * The repo root, derived from the package root the VERB exports (not from cwd, which is
 * only accidentally the repo root when vitest runs from there), and the three absolute
 * paths a deploy reads. Built from the verb's own constants so a rename upstream breaks
 * a test rather than silently changing which file a deploy ships.
 */
const REPO = join(PACKAGE_ROOT, '..', '..', '..');
const pathsFor = (): { bin: string; vault: string; cargoConfig: string } => ({
    bin: join(REPO, '.rust', 'target', 'release', 'typecrate-provider-server'),
    vault: DEFAULT_VAULT_PATH,
    cargoConfig: join(REPO, '.cargo', 'config.toml'),
});

/** The fake host: the artifact, the vault document, and the cargo config that locates the artifact. */
const hostFs = (paths: { bin: string; vault: string; cargoConfig: string }): FsAccess => {
    const table = new Map<string, Buffer>([
        [paths.bin, ELF],
        [paths.vault, Buffer.from(VAULT_DOC, 'utf8')],
        [paths.cargoConfig, Buffer.from('[build]\ntarget-dir = ".rust/target"\n', 'utf8')],
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

/** The endpoint host: a dry run must never call it, so any call is a loud failure. */
const neverProbed: Probe = async (method, url) => {
    throw new Error(`start:modal touched the network it promised not to: ${method} ${url}`);
};

/** The one box map shape that matters: the vault's single entry, as config/vault writes it. */
const VAULT_MAP: Record<string, string> = { DevilAngel: 'https://devilz-angelz--container-sandbox.us-east.modal.run' };

/** Every line a run printed, as ONE string, for the exact-output assertions. */
const host = (fsPaths: { bin: string; vault: string; cargoConfig: string }, env: Record<string, string | undefined> = {}) => {
    const out: string[] = [];
    const err: string[] = [];
    const cargo: string[] = [];
    const deps: ProviderDeps = {
        probe: neverProbed,
        fs: hostFs(fsPaths),
        runCommand: async (command, args) => {
            cargo.push(`${command} ${args.join(' ')}`);
            return { status: 0, stdout: '', stderr: '' };
        },
        env,
        sleep: async () => undefined,
        log: (text) => out.push(text),
        error: (text) => err.push(text),
    };
    return { deps, out, err, cargo };
};

// ══════════════════════════════════════════════════════════════════════════════
//  THE PRECEDENCE RULE — as data, with the vault map present in EVERY case
// ══════════════════════════════════════════════════════════════════════════════

describe('resolveEndpoint', () => {
    it('takes the box the command line names, and does not touch the argv', () => {
        const bare = resolveEndpoint(['https://mine.run', '--dry-run'], {}, VAULT_MAP);
        expect(bare).toEqual({ argv: ['https://mine.run', '--dry-run'], source: 'argument' as EndpointSource, endpoint: 'https://mine.run', label: 'DevilAngel' });

        const flagged = resolveEndpoint(['--endpoint', 'https://mine.run', '--no-build'], {}, VAULT_MAP);
        expect(flagged).toEqual({ argv: ['--endpoint', 'https://mine.run', '--no-build'], source: 'argument', endpoint: 'https://mine.run', label: 'DevilAngel' });
    });

    it('lets the environment win over the vault, and never injects --endpoint', () => {
        const resolved = resolveEndpoint(['--dry-run'], { [ENDPOINT_ENV]: 'https://ci.run' }, VAULT_MAP);
        expect(resolved).toEqual({ argv: ['--dry-run'], source: 'environment', endpoint: 'https://ci.run', label: 'DevilAngel' });
        // The verb re-parses the SAME argv, so the env value has to reach it intact.
        expect(parseArgs(resolved.argv, { [ENDPOINT_ENV]: 'https://ci.run' }).flags.endpoint).toBe('https://ci.run');
    });

    it('falls back to the vault box ONLY when nothing else named one', () => {
        const resolved = resolveEndpoint(['--dry-run'], {}, VAULT_MAP);
        // The injection is exactly one flag pair, prepended: the verb's parser reads
        // `--endpoint <url>` and every operator flag keeps its meaning.
        expect(resolved).toEqual({ argv: ['--endpoint', 'https://devilz-angelz--container-sandbox.us-east.modal.run', '--dry-run'], source: 'vault', endpoint: 'https://devilz-angelz--container-sandbox.us-east.modal.run', label: 'DevilAngel' });
        expect(parseArgs(resolved.argv, {}).flags.endpoint).toBe('https://devilz-angelz--container-sandbox.us-east.modal.run');
    });

    it('refuses to invent an endpoint for a broken command line or for --help', () => {
        // A typo must surface as the verb's parse error, not as a deploy to the vault's box.
        expect(resolveEndpoint(['--nope'], {}, VAULT_MAP)).toEqual({ argv: ['--nope'], source: 'argument', endpoint: '', label: 'DevilAngel' });
        expect(resolveEndpoint(['--help'], {}, VAULT_MAP)).toEqual({ argv: ['--help'], source: 'argument', endpoint: '', label: 'DevilAngel' });
        // A missing value is the same story.
        expect(resolveEndpoint(['--port'], {}, VAULT_MAP)).toEqual({ argv: ['--port'], source: 'argument', endpoint: '', label: 'DevilAngel' });
    });

    it('has nothing to inject when the vault names no box, so the verb reports it', () => {
        const resolved = resolveEndpoint(['--dry-run'], {}, {});
        expect(resolved).toEqual({ argv: ['--dry-run'], source: 'argument', endpoint: '', label: '' });
        expect(parseArgs(resolved.argv, {}).errors).toEqual(['no endpoint — pass the runner URL as the argument, or --endpoint <url>, or TYPECRATE_PROVIDER_ENDPOINT=… (or use --out <path> to only build the bundle)']);
    });

    it('names the vault map this package imports, and its first entry', () => {
        // `@config/vault` is a real workspace import: pin the generated shape, because a
        // renamed box key would otherwise silently change which box `start:modal` targets.
        // If this test starts failing, the vault's Modal map changed (the sibling task that
        // gives each long-lived service its own deployment name): update the map this verb
        // reads AND `#deploy` in package.json in the same change.
        expect(VAULT_BOXES).toEqual({ DevilAngel: 'https://devilz-angelz--container-sandbox.us-east.modal.run' });
        expect(vaultBox(VAULT_MAP)).toEqual(['DevilAngel', 'https://devilz-angelz--container-sandbox.us-east.modal.run']);
        // Insertion order IS the rule (the map has one entry today; the first is the default).
        expect(vaultBox({ A: 'https://a.run', B: 'https://b.run' })).toEqual(['A', 'https://a.run']);
        expect(vaultBox({})).toEqual(['', '']);
        expect(ENTRY_SCRIPT_NAME_MODAL).toBe('start-modal.ts');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  mainModal — resolve, announce, delegate
// ══════════════════════════════════════════════════════════════════════════════

describe('mainModal', () => {
    it('deploys to the vault box when nothing else names one, and says where it came from', async () => {
        const hostWorld = host(pathsFor());
        const code = await mainModal(['--dry-run'], { env: {}, boxes: VAULT_MAP, deps: hostWorld.deps });
        expect(code).toBe(0);
        // A dry run compiles nothing and probes nothing, on either host.
        expect(hostWorld.cargo).toEqual([]);
        expect(hostWorld.err).toEqual([]);
        expect(hostWorld.out[0]).toBe(`${PREFIX} endpoint: https://devilz-angelz--container-sandbox.us-east.modal.run\n${PREFIX}   source: @config/vault modalContainer.DevilAngel — override it with --endpoint or TYPECRATE_PROVIDER_ENDPOINT to place the provider app on a different origin`);
        // The machine line is the LAST line, and it names the box + the app the run uses.
        // v2 auto-allocates the internal port, so the planned summary reports port 0
        // ("not assigned yet") and the one-origin app base.
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toMatchObject({
            mode: 'deploy',
            endpoint: 'https://devilz-angelz--container-sandbox.us-east.modal.run',
            name: 'provider',
            status: 'planned',
            publicUrl: 'https://devilz-angelz--container-sandbox.us-east.modal.run/app/provider/',
            path: '/app/provider/',
            port: 0,
            profile: 'anonymous',
        });
    });

    it('prefers an ambient TYPECRATE_PROVIDER_ENDPOINT over the vault, and names the env as the source', async () => {
        const env = { [ENDPOINT_ENV]: 'https://second-box.modal.run' };
        const hostWorld = host(pathsFor(), env);
        expect(await mainModal(['--dry-run'], { env, boxes: VAULT_MAP, deps: hostWorld.deps })).toBe(0);
        expect(hostWorld.out[0]).toBe(`${PREFIX} endpoint: https://second-box.modal.run\n${PREFIX}   source: $TYPECRATE_PROVIDER_ENDPOINT`);
        // The repo's box must appear NOWHERE in the run: a shared box is not displaced by accident.
        expect(hostWorld.out.join('\n')).not.toContain('devilz-angelz');
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toMatchObject({ endpoint: 'https://second-box.modal.run' });
    });

    it('prefers the command line over BOTH, which is what the reference package cannot do', async () => {
        const env = { [ENDPOINT_ENV]: 'https://env.run' };
        const hostWorld = host(pathsFor(), env);
        expect(await mainModal(['https://arg.run', '--dry-run'], { env, boxes: VAULT_MAP, deps: hostWorld.deps })).toBe(0);
        expect(hostWorld.out[0]).toBe(`${PREFIX} endpoint: https://arg.run\n${PREFIX}   source: the command line (--endpoint / the positional URL)`);
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toMatchObject({ endpoint: 'https://arg.run' });
    });

    it('prints the usage screen alone for --help, and deploys nothing', async () => {
        const hostWorld = host(pathsFor());
        expect(await mainModal(['--help'], { env: {}, boxes: VAULT_MAP, deps: hostWorld.deps })).toBe(0);
        // One line, and NOT a provenance line: `--help` resolves no endpoint, so the vault
        // box is never mentioned and no run is implied.
        expect(hostWorld.out).toEqual([USAGE]);
        expect(hostWorld.cargo).toEqual([]);
    });

    it('reports a broken command line as the verb does — exit 1, no endpoint invented', async () => {
        const hostWorld = host(pathsFor());
        expect(await mainModal(['--probe-path'], { env: {}, boxes: VAULT_MAP, deps: hostWorld.deps })).toBe(1);
        expect(hostWorld.err[0]).toBe(`${PREFIX} ✗ --probe-path needs a value`);
        // No injection happened, so the missing-endpoint error is the SECOND error.
        expect(hostWorld.err[1]).toBe(`${PREFIX} ✗ no endpoint — pass the runner URL as the argument, or --endpoint <url>, or TYPECRATE_PROVIDER_ENDPOINT=… (or use --out <path> to only build the bundle)`);
    });

    it('refuses to start anything when the file is only imported', async () => {
        // The entry guard fired at import time (this suite is vitest, so the guard's
        // vitest check wins) — proven by the absence of any output above, and pinned here
        // by the guard contract the verb's own suite tests in full.
        expect(await mainModal([], { env: {}, boxes: {}, deps: host(pathsFor()).deps })).toBe(1);
    });

    it('keeps the payload identical to the generic verb’s, because it is the same builder', async () => {
        const hostWorld = host(pathsFor());
        const built = await buildProviderPayload(parseArgs(['https://box.modal.run', '--dry-run']).flags, hostWorld.deps);
        expect(built.members.map((member) => `${member.mode.toString(8).padStart(4, '0')} ${member.name}`)).toEqual(['0755 typecrate-provider-server', '0644 agentic-provider.json']);
        expect(built.payload.config).toEqual({
            runCmd: './typecrate-provider-server',
            artifactPath: 'typecrate-provider-server',
            probeTimeoutSec: 120,
            stopSignal: 'SIGTERM',
            probePath: '/health',
            env: { TYPECRATE_PROVIDER_VAULT_PATH: './agentic-provider.json', TYPECRATE_PROVIDER_HOST: '127.0.0.1', RUST_LOG: 'info' },
        });
        // The v2 identity travels WITH the payload, on every verb (start:modal simply
        // forwards the argv; it must not lose the name or the native prefix).
        expect(built.payload.name).toBe('provider');
        expect(built.payload.targetPrefix).toBe('/providers/private/v1');
    });
});
