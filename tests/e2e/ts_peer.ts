// A real CareerVector TS SDK peer (lib/workspace-client), human class, append path, against the
// local stack. Used by the cvdb tests for browser interop and to generate the T1 fixtures.
//
//   CV_ROOT=<checkout> bun ts_peer.ts edit <api> <wsId> <ops.json>
//       applies each op of the file to cv-profile / cl-profile through the SDK and prints one JSON
//       line per update the server accepted: {sub, opId, update: <base64>}
//   CV_ROOT=<checkout> bun ts_peer.ts fixtures <api> <wsId> <outDir>
//       on a fresh append workspace: cv_profile.replace then node.update through the SDK; writes
//       u0.bin (the server's initial head), u1.bin, u2.bin, frames.bin (framed GET /sub body), relay.bin (relay sync frame of u2),
//       expected.json, and records the CareerVector commit in source.txt
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { execFileSync } from 'node:child_process';

const root = process.env.CV_ROOT!;
const { WorkspaceClient } = await import(resolve(root, 'lib/workspace-client/src/index.ts'));
const { encodeUpdateFrames } = await import(resolve(root, 'lib/server/src/workspace-append.ts'));
const encoding = await import(resolve(root, 'node_modules/lib0/encoding.js')).catch(() => import('lib0/encoding'));

const [mode, api, wsId, arg] = process.argv.slice(2);
const mem = new Map<string, string>();
const client = new WorkspaceClient({
	workspaceId: wsId,
	baseUrl: `${api}/workspaces`,
	writePath: 'append',
	actorClass: 'human',
	idb: 'disabled',
	deviceIdStorage: { getItem: (k: string) => mem.get(k) ?? null, setItem: (k: string, v: string) => void mem.set(k, v) },
	headWriter: { opsThreshold: 0 }
});
const accepted: { sub: string; opId: string; update: Uint8Array }[] = [];
client.onAccepted((sub: string, opId: string, update: Uint8Array) => accepted.push({ sub, opId, update }));
const b64 = (u: Uint8Array) => Buffer.from(u).toString('base64');
const subOf = (op: any) => (String(op.branch_path ?? op.kind).startsWith('cl_') || String(op.kind).startsWith('cl_') ? 'cl-profile' : 'cv-profile');

async function run(ops: any[]) {
	await client.read('cv-profile');
	await client.read('cl-profile');
	for (const op of ops) await client.applyOp(subOf(op), op);
}

if (mode === 'edit') {
	await run(JSON.parse(readFileSync(arg, 'utf8')));
	for (const a of accepted) console.log(JSON.stringify({ sub: a.sub, opId: a.opId, update: b64(a.update) }));
} else if (mode === 'fixtures') {
	const node = (id: string, kind: string, data: any, children: any[] = []) => ({
		id, kind, tier: 'HYBRID', data, selection: { active: children.map((c) => c.id) }, children
	});
	const tree = node('root', 'cv-root', {}, [node('item', 'list-item', { heading: 'Engineer' }, [node('b1', 'bullet', { text: 'Built things' })])]);
	const u0 = new Uint8Array(await (await fetch(`${api}/workspaces/${wsId}/sub/cv-profile`)).arrayBuffer());
	await run([
		{ kind: 'cv_profile.replace', lang: 'en', tree },
		{ kind: 'node.update', branch_path: 'cv_profile.en', node_path: 'root.item.b1', patch: { data: { text: 'Shipped things' } } }
	]);
	const cv = accepted.filter((a) => a.sub === 'cv-profile');
	if (cv.length !== 2) throw new Error(`expected 2 accepted cv updates, got ${cv.length}`);
	mkdirSync(arg, { recursive: true });
	const [u1, u2] = [cv[0].update, cv[1].update];
	writeFileSync(`${arg}/u0.bin`, u0);
	writeFileSync(`${arg}/u1.bin`, u1);
	writeFileSync(`${arg}/u2.bin`, u2);
	writeFileSync(`${arg}/frames.bin`, encodeUpdateFrames(u0, [u1, u2]));
	const enc = encoding.createEncoder();
	encoding.writeVarUint(enc, 0);
	encoding.writeVarUint(enc, 2);
	encoding.writeVarUint8Array(enc, u2);
	writeFileSync(`${arg}/relay.bin`, encoding.toUint8Array(enc));
	writeFileSync(`${arg}/expected.json`, JSON.stringify({ branch: 'cv_profile.en', node: 'root.item.b1', first: 'Built things', second: 'Shipped things' }));
	const sha = execFileSync('git', ['-C', root, 'rev-parse', 'HEAD']).toString().trim();
	writeFileSync(`${arg}/source.txt`, `produced by lib/workspace-client (SDK append path, human class) at CareerVector ${sha}\n`);
} else {
	throw new Error('mode');
}
client.destroy?.();
process.exit(0);
