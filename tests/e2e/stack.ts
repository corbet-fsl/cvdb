// Local relay + api for the cvdb end-to-end tests (never a deployed Worker).
//
//   CV_ROOT=<CareerVector checkout> bun tests/e2e/stack.ts
//
// Reuses the service commands of CareerVector's `ui/e2e/stack.ts` (ports, flags, vars), starts
// only the relay and the api, prints one `READY {json}` line and runs until stdin closes or a
// signal arrives; every process is killed and the state directory removed on exit.
import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { createServer } from 'node:net';
import { appendFileSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

const root = process.env.CV_ROOT;
if (!root) throw new Error('CV_ROOT is required');
const { serviceSpecs } = await import(resolve(root, 'ui/e2e/stack.ts'));

const freePort = () =>
	new Promise<number>((ok, fail) => {
		const s = createServer();
		s.once('error', fail);
		s.listen(0, '127.0.0.1', () => {
			const { port } = s.address() as { port: number };
			s.close(() => ok(port));
		});
	});
const ports = { relay: await freePort(), api: await freePort(), vite: await freePort() };
const base = resolve(root, '.wrangler-e2e', `cvdb-${process.pid}`);
const stateDir = resolve(base, 'state');
mkdirSync(stateDir, { recursive: true });
const kids: ChildProcess[] = [];
const down = () => {
	for (const k of kids) {
		try {
			if (k.pid) process.kill(-k.pid, 'SIGKILL');
		} catch {
			/* gone */
		}
	}
	rmSync(base, { recursive: true, force: true });
};
for (const sig of ['SIGINT', 'SIGTERM', 'SIGHUP'] as const) process.on(sig, () => (down(), process.exit(130)));
process.on('exit', down);

const d1 = spawnSync(
	'wrangler',
	['d1', 'execute', 'careervector', '--local', `--persist-to=${stateDir}`, `--file=${resolve(root, 'migrations/schema.sql')}`],
	{ cwd: resolve(root, 'ui'), encoding: 'utf8', env: { ...process.env, WRANGLER_SEND_METRICS: 'false' } }
);
if (d1.status !== 0) throw new Error(`schema bootstrap failed\n${d1.stdout}${d1.stderr}`);
writeFileSync(resolve(stateDir, '.schema-applied'), 'bootstrapped by the cvdb e2e stack\n');

const logs = (n: string) => resolve(base, `${n}.log`);
for (const spec of (serviceSpecs(root, stateDir, ports) as any[]).filter((s) => s.name !== 'vite')) {
	writeFileSync(logs(spec.name), '');
	const p = spawn(spec.cmd, spec.args, { cwd: spec.cwd, env: { ...process.env, ...spec.env }, detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
	p.stdout?.on('data', (d) => appendFileSync(logs(spec.name), d));
	p.stderr?.on('data', (d) => appendFileSync(logs(spec.name), d));
	kids.push(p);
}
const has = (n: string, t: string) => readFileSync(logs(n), 'utf8').includes(t);
const end = Date.now() + 120_000;
for (;;) {
	if (has('relay', 'Ready on') && has('api', 'Ready on')) {
		try {
			const r = await fetch(`http://127.0.0.1:${ports.api}/version`, { signal: AbortSignal.timeout(2000) });
			if (r.status < 500) break;
		} catch {
			/* not yet */
		}
	}
	if (Date.now() > end) {
		console.error(readFileSync(logs('api'), 'utf8').split('\n').slice(-20).join('\n'));
		throw new Error('stack not healthy');
	}
	await new Promise((r) => setTimeout(r, 300));
}
console.log(`READY ${JSON.stringify({ api: `http://127.0.0.1:${ports.api}`, relay: `http://127.0.0.1:${ports.relay}`, state: stateDir })}`);
process.stdin.resume();
process.stdin.on('end', () => process.exit(0));
