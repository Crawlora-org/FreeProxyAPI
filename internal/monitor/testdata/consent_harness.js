// Runs the analytics consent script from a rendered page against a simulated
// browser and prints one JSON result per scenario. Usage: node consent_harness.js SCRIPT.js
'use strict';
const fs = require('fs');
const vm = require('vm');

const source = fs.readFileSync(process.argv[2], 'utf8');
const ID = 'G-TESTID1234';
const KEY = 'fpa-analytics-consent';

// A cookie jar with domain scoping: a cookie is removed only by an expiry
// whose domain attribute matches the domain it was set for.
function makeJar(initial) {
  const cookies = initial.map(([name, domain]) => ({ name, domain, value: '1' }));
  const attempts = [];
  return {
    attempts,
    names: () => cookies.map((c) => c.name),
    get cookie() { return cookies.map((c) => c.name + '=' + c.value).join('; '); },
    set cookie(raw) {
      const parts = raw.split(';').map((p) => p.trim());
      const [name] = parts[0].split('=');
      const attrs = Object.fromEntries(parts.slice(1).map((p) => { const [k, v] = p.split('='); return [k.toLowerCase(), v]; }));
      attempts.push({ name, domain: attrs.domain || null });
      const expired = attrs.expires && new Date(attrs.expires).getTime() < Date.now();
      if (!expired) return;
      const index = cookies.findIndex((c) => c.name === name && (attrs.domain || null) === c.domain);
      if (index >= 0) cookies.splice(index, 1);
    },
  };
}

function setup({ saved = null, gpc, dnt, storageThrows = false, cookies = [], hostname = 'freeproxyapi.crawlora.net' } = {}) {
  const elements = {};
  const el = (id) => (elements[id] = elements[id] || {
    id, hidden: true, focused: false, listeners: {},
    addEventListener(type, fn) { this.listeners[type] = fn; },
    click() { this.listeners.click(); },
    focus() { this.focused = true; },
  });
  const store = { value: saved };
  const appended = [];
  const jar = makeJar(cookies);
  const document = {
    getElementById: el,
    createElement: () => ({}),
    head: { appendChild: (node) => appended.push(node) },
    get cookie() { return jar.cookie; },
    set cookie(v) { jar.cookie = v; },
  };
  const localStorage = storageThrows
    ? { getItem() { throw new Error('denied'); }, setItem() { throw new Error('denied'); } }
    : { getItem: (k) => (k === KEY ? store.value : null), setItem: (k, v) => { if (k === KEY) store.value = v; } };
  const window = {};
  const sandbox = { window, document, localStorage, navigator: { globalPrivacyControl: gpc, doNotTrack: dnt }, location: { hostname }, encodeURIComponent, Date, Array, Object };
  window.window = window;
  vm.runInNewContext(source.replace("'{{MEASUREMENT_ID}}'", `'${ID}'`), Object.assign(sandbox, { self: window }), { timeout: 2000 });
  return { el, store, appended, jar, window, document };
}

const results = [];
function scenario(name, fn) {
  try {
    const detail = fn();
    results.push({ name, ok: detail === true || detail === undefined, detail: detail === true ? '' : String(detail) });
  } catch (error) {
    results.push({ name, ok: false, detail: 'threw: ' + error.message });
  }
}
// Returns '' (falsy) on success and the message on failure, so a chain of
// `expect(...) || expect(...) || true` reports the first failure and otherwise runs
// every check.
const expect = (cond, message) => (cond ? '' : message);

scenario('first visit shows the banner and loads nothing', () => {
  const t = setup();
  return expect(t.el('consent').hidden === false, 'banner hidden') ||
    expect(t.el('consent-settings').hidden === true, 'settings visible') ||
    expect(t.appended.length === 0, 'script loaded before consent') ||
    expect(!t.window.dataLayer && !t.window.gtag, 'gtag initialised before consent') || true;
});

scenario('accepting loads GA once and remembers the choice', () => {
  const t = setup();
  t.el('consent-accept').click();
  t.el('consent-accept').click();
  return expect(t.store.value === 'granted', 'choice not stored') ||
    expect(t.appended.length === 1, `script appended ${t.appended.length} times`) ||
    expect(t.appended[0].src.includes('googletagmanager.com/gtag/js?id=' + ID), 'wrong script src') ||
    expect(t.el('consent').hidden === true && t.el('consent-settings').hidden === false, 'banner state wrong after accept') ||
    expect(t.window.dataLayer.length === 2, 'gtag js/config not queued') || true;
});

scenario('declining stores the choice and loads nothing', () => {
  const t = setup();
  t.el('consent-decline').click();
  return expect(t.store.value === 'denied', 'choice not stored') ||
    expect(t.appended.length === 0, 'script loaded after decline') ||
    expect(t.window['ga-disable-' + ID] === true, 'ga-disable not set') ||
    expect(t.el('consent').hidden === true, 'banner still visible') || true;
});

scenario('a returning visitor who accepted loads GA without a banner', () => {
  const t = setup({ saved: 'granted' });
  return expect(t.appended.length === 1, 'script not loaded') || expect(t.el('consent').hidden === true, 'banner shown') || true;
});

scenario('a returning visitor who declined sees no banner and no GA', () => {
  const t = setup({ saved: 'denied' });
  return expect(t.appended.length === 0, 'script loaded') || expect(t.el('consent').hidden === true, 'banner shown') ||
    expect(t.el('consent-settings').hidden === false, 'no way to change the choice') || true;
});

scenario('declining after accepting removes GA cookies on the parent domain and keeps others', () => {
  const t = setup({ saved: 'granted', cookies: [['_ga', 'crawlora.net'], ['_ga_TESTID1234', 'crawlora.net'], ['_gid', 'crawlora.net'], ['session', 'crawlora.net']] });
  t.el('consent-settings').click();
  t.el('consent-decline').click();
  return expect(JSON.stringify(t.jar.names()) === '["session"]', 'cookies left: ' + JSON.stringify(t.jar.names())) ||
    expect(t.window['ga-disable-' + ID] === true, 'ga-disable not set') ||
    expect(t.jar.attempts.some((a) => a.domain === 'crawlora.net'), 'parent domain never tried') || true;
});

for (const [label, opts] of [['Global Privacy Control', { gpc: true }], ['Do Not Track', { dnt: '1' }]]) {
  scenario(`${label} is honored without a banner, even after an earlier accept`, () => {
    const t = setup({ ...opts, saved: 'granted', cookies: [['_ga', 'crawlora.net']] });
    return expect(t.appended.length === 0, 'script loaded despite opt-out') || expect(t.el('consent').hidden === true, 'banner shown') ||
      expect(t.jar.names().length === 0, 'GA cookie kept') || true;
  });
}

scenario('unavailable storage still shows the banner and accept still works', () => {
  const t = setup({ storageThrows: true });
  const shown = t.el('consent').hidden === false;
  t.el('consent-accept').click();
  return expect(shown, 'banner hidden') || expect(t.appended.length === 1, 'accept did not load GA') || true;
});

scenario('the settings button reopens the banner and focuses accept', () => {
  const t = setup({ saved: 'denied' });
  t.el('consent-settings').click();
  return expect(t.el('consent').hidden === false, 'banner not reopened') || expect(t.el('consent-accept').focused, 'accept not focused') || true;
});

console.log(JSON.stringify(results));
