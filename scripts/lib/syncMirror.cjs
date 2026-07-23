// ──────────────────────────────────────────────────────────
// Bem na Mosca — sync mirror storage
// ──────────────────────────────────────────────────────────
// Reads/writes the local working copy of the public GitHub mirror repo
// that records the LAST-SYNCED (price, min, max) per barcode per store.
// This is what lets sync-neon-to-firestore.cjs diff "did this change?"
// without ever reading Firestore — the mirror is the read side, git
// history is the free storage, Firestore only ever receives writes.
//
// Layout inside <localPath>/:
//   mirror/<storeSlug>/<shard>.json   one JSON object keyed by barcode:
//     { "<barcode>": { price, min, max, productName, updatedAt } }
//   state/cursor.json                 per-store pagination progress:
//     { "<storeSlug>": { lastBarcode, totalSynced, completedFullPass } }
'use strict';

const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

function shardOf(barcode) {
  return String(barcode).slice(0, 3).padEnd(3, '0');
}

class SyncMirror {
  constructor(localPath) {
    this.root = path.resolve(localPath);
    this.mirrorDir = path.join(this.root, 'mirror');
    this.stateDir = path.join(this.root, 'state');
    this.cursorPath = path.join(this.stateDir, 'cursor.json');
    this._shardCache = new Map(); // "<store>/<shard>" -> object (mutated in place)
    this._dirtyShards = new Set();
    this.cursor = this._loadJson(this.cursorPath, {});
  }

  _loadJson(filePath, fallback) {
    try {
      return JSON.parse(fs.readFileSync(filePath, 'utf8'));
    } catch {
      return fallback;
    }
  }

  _shardPath(storeSlug, shard) {
    return path.join(this.mirrorDir, storeSlug, `${shard}.json`);
  }

  _loadShard(storeSlug, shard) {
    const key = `${storeSlug}/${shard}`;
    if (this._shardCache.has(key)) return this._shardCache.get(key);
    const data = this._loadJson(this._shardPath(storeSlug, shard), {});
    this._shardCache.set(key, data);
    return data;
  }

  /** Last known { price, min, max, productName, updatedAt } for a barcode at a store, or null. */
  get(storeSlug, barcode) {
    const shard = this._loadShard(storeSlug, shardOf(barcode));
    return shard[barcode] || null;
  }

  /** Record the current values as the new "last known state" for a barcode at a store. */
  set(storeSlug, barcode, record) {
    const shardKey = shardOf(barcode);
    const shard = this._loadShard(storeSlug, shardKey);
    shard[barcode] = record;
    this._dirtyShards.add(`${storeSlug}/${shardKey}`);
  }

  getCursor(storeSlug) {
    return this.cursor[storeSlug] || { lastBarcode: '', totalSynced: 0, completedFullPass: false };
  }

  setCursor(storeSlug, cursorData) {
    this.cursor[storeSlug] = cursorData;
  }

  /** Flush all dirty shards + cursor.json to disk (does NOT commit/push git). */
  flush() {
    for (const key of this._dirtyShards) {
      const [storeSlug, shard] = key.split('/');
      const data = this._shardCache.get(key);
      const filePath = this._shardPath(storeSlug, shard);
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      // Sort top-level keys for stable git diffs. IMPORTANT: build a new
      // object rather than passing the sorted-keys array as JSON.stringify's
      // replacer — a replacer array is an ALLOWLIST applied at every nesting
      // level, so passing barcode keys there would strip every field
      // (price/min/max/...) out of each nested record, silently writing
      // `{}` for every entry.
      const sorted = {};
      for (const barcodeKey of Object.keys(data).sort()) sorted[barcodeKey] = data[barcodeKey];
      fs.writeFileSync(filePath, JSON.stringify(sorted), 'utf8');
    }
    fs.mkdirSync(this.stateDir, { recursive: true });
    fs.writeFileSync(this.cursorPath, JSON.stringify(this.cursor, null, 2), 'utf8');
    this._dirtyShards.clear();
  }

  /** Commit + push the mirror's local git working copy, if it has a remote. Silently skips otherwise. */
  commitAndPush(message) {
    const isRepo = fs.existsSync(path.join(this.root, '.git'));
    if (!isRepo) return { pushed: false, reason: 'not a git repo — run setup-sync-mirror-repo.cjs first' };

    try {
      execFileSync('git', ['add', '-A'], { cwd: this.root, stdio: 'pipe' });
      const staged = execFileSync('git', ['diff', '--cached', '--name-only'], { cwd: this.root, encoding: 'utf8' }).trim();
      if (!staged) return { pushed: false, reason: 'no changes to commit' };

      execFileSync('git', ['commit', '-m', message], { cwd: this.root, stdio: 'pipe' });
      const hasRemote = execFileSync('git', ['remote'], { cwd: this.root, encoding: 'utf8' }).trim().length > 0;
      if (!hasRemote) return { pushed: false, reason: 'no git remote configured' };

      execFileSync('git', ['push'], { cwd: this.root, stdio: 'pipe' });
      return { pushed: true };
    } catch (err) {
      return { pushed: false, reason: err?.message || String(err) };
    }
  }
}

module.exports = { SyncMirror, shardOf };
