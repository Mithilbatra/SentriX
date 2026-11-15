const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../dashboard/index.html'), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];

// Small DOM stand-in: run the actual dashboard script without adding a browser dependency.
class Element {
  constructor(tag = 'div') {
    this.tag = tag;
    this.children = [];
    this.dataset = {};
    this.attrs = {};
    this.className = '';
    this.textContent = '';
    this.disabled = false;
    this.classList = {
      add: name => { this.className = `${this.className} ${name}`.trim(); },
      remove: name => { this.className = this.className.split(' ').filter(c => c !== name).join(' '); },
      contains: name => this.className.split(' ').includes(name),
    };
  }
  get rows() { return this.children; }
  append(child) { child.parent = this; this.children.push(child); }
  insertCell() { const cell = new Element('td'); this.append(cell); return cell; }
  insertBefore(child, before) {
    child.parent = this;
    this.children.splice(before ? this.children.indexOf(before) : this.children.length, 0, child);
  }
  deleteRow(index) { this.children.splice(index < 0 ? this.children.length - 1 : index, 1); }
  replaceChildren() { this.children = []; }
  remove() { this.parent.children = this.parent.children.filter(c => c !== this); }
  setAttribute(key, value) { this.attrs[key] = value; }
  querySelectorAll(tag) {
    return this.children.flatMap(child => [...(child.tag === tag ? [child] : []), ...child.querySelectorAll(tag)]);
  }
  querySelector(tag) { return this.querySelectorAll(tag)[0] ?? null; }
  click() { if (!this.disabled) return this.onclick?.(); }
}

function dashboard(fetchResult = async () => ({ok: true, json: async () => []})) {
  const nodes = Object.fromEntries([...html.matchAll(/id="([^"]+)"/g)].map(m => [m[1], new Element()]));
  nodes.graph.append(new Element('defs'));
  let events;
  const requests = [];
  const context = vm.createContext({
    document: {
      getElementById: id => nodes[id],
      createElement: tag => new Element(tag),
      createElementNS: (_, tag) => new Element(tag),
    },
    EventSource: class { constructor() { events = this; } },
    fetch: (...args) => { requests.push(args[0]); return fetchResult(...args); },
    AbortSignal, setInterval: () => {}, console,
  });
  vm.runInContext(script, context);
  return {nodes, requests, events, evaluate: code => vm.runInContext(code, context)};
}

const tick = () => new Promise(setImmediate);
const txn = (id = 'payment') => ({txn_id: id, ts: 1794728100000, payer: 'a@upi', payee: 'b@upi',
  amount: 1234.56, risk: 0.9, flag: true, new_device: 1, cnt_10m: 0, amt_ratio: 1, new_payee: 1});

test('stored alerts load without SSE and recover on reconnect without duplicates', async () => {
  let alerts = [txn('old')];
  const app = dashboard(async url => ({ok: true, json: async () => url.includes('alerts') ? [...alerts] : []}));
  await tick(); // Deliberately do not open the EventSource.
  assert.equal(app.nodes.alerts.rows.length, 1);
  assert.equal(app.evaluate('seen'), 0); // Historical alerts do not inflate session counters.
  alerts = [txn('missed'), txn('old')];
  app.events.onerror();
  app.events.onopen();
  await tick();
  assert.equal(app.nodes.alerts.rows.length, 2);
});

test('replayed live events do not inflate the feed or counters; ID history is bounded', async () => {
  const app = dashboard();
  await tick();
  const event = {data: JSON.stringify(txn())};
  app.events.onmessage(event);
  app.events.onmessage(event);
  assert.equal(app.nodes.seen.textContent, '1');
  assert.equal(app.nodes.flagged.textContent, '1');
  assert.equal(app.nodes.feed.rows.length, 1);
  assert.equal(app.nodes.alerts.rows.length, 1);
  // Fill the cache at its boundary, then exercise the real handler once more.
  app.evaluate("for (let i = 0; i < 4999; i++) received.add('cached-' + i)");
  app.events.onmessage({data: JSON.stringify(txn('next'))});
  assert.equal(app.evaluate('received.size'), 5000);
  assert.equal(app.evaluate("received.has('payment')"), false);
});

test('payment amounts retain paise and historical alerts include the date', async () => {
  const app = dashboard();
  await tick();
  const payment = txn();
  app.events.onmessage({data: JSON.stringify(payment)});
  assert.equal(app.nodes.alerts.rows[0].children[0].textContent, new Date(payment.ts).toLocaleString());
  assert.match(app.nodes.alerts.rows[0].children[2].textContent, /1,234\.56/);
});

test('failed refresh preserves alerts, shows an error, and allows retry', async () => {
  let failed = false;
  const app = dashboard(async url => ({ok: !failed, json: async () => url.includes('alerts') ? [txn()] : []}));
  await tick();
  failed = true;
  await app.nodes['alerts-refresh'].click();
  assert.equal(app.nodes.alerts.rows.length, 1);
  assert.equal(app.nodes['alerts-refresh'].disabled, false);
  assert.ok(app.nodes['alerts-status'].classList.contains('error'));
  failed = false;
  await app.nodes['alerts-refresh'].click();
  assert.equal(app.nodes['alerts-status'].classList.contains('error'), false);
  assert.match(app.nodes['alerts-status'].textContent, /Last refreshed/);
});

test('ring selection survives refresh and failures; a successful empty result clears it', async () => {
  let rings = ['one', 'two'].map(id => ({id, type: 'ring', members: ['a', 'b', 'c'], edges: [], score: 100}));
  let failed = false;
  const app = dashboard(async url => {
    if (failed) throw new Error('Offline');
    return {ok: true, json: async () => url.includes('rings') ? rings : []};
  });
  await tick();
  app.nodes['ring-list'].querySelectorAll('button')[1].click();
  await app.nodes.refresh.click();
  assert.equal(app.evaluate('selectedCluster'), 'two');
  assert.equal(app.nodes['ring-list'].querySelectorAll('button')[1].attrs['aria-pressed'], 'true');
  failed = true;
  await app.nodes.refresh.click();
  assert.equal(app.nodes['ring-list'].children.length, 2);
  assert.ok(app.nodes['rings-status'].classList.contains('error'));
  failed = false;
  rings = [];
  await app.nodes.refresh.click();
  assert.equal(app.nodes['ring-list'].children.length, 0);
  assert.equal(app.nodes.graph.querySelectorAll('g').length, 0);
  assert.equal(app.nodes.graph.attrs['aria-label'], 'No cluster selected');
  assert.equal(app.nodes.graph.children[0].tag, 'defs');
});

test('loading disables repeated refresh requests and handles malformed responses', async () => {
  let resolve;
  const app = dashboard(url => url.includes('rings') ? new Promise(r => { resolve = r; }) :
    Promise.resolve({ok: true, json: async () => []}));
  assert.equal(app.nodes.refresh.disabled, true);
  app.nodes.refresh.click();
  await app.evaluate('loadRings()');
  assert.equal(app.requests.filter(url => url.includes('rings')).length, 1);
  resolve({ok: true, json: async () => ({detail: 'not a list'})});
  await tick();
  assert.equal(app.nodes.refresh.disabled, false);
  assert.ok(app.nodes['rings-status'].classList.contains('error'));
});
