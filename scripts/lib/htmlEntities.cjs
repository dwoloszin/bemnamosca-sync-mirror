'use strict';

// ──────────────────────────────────────────────────────────
// Product names arrive with HTML entities still in them.
//
// The scrapers read pharmacy pages and store the raw text, so 6.6% of the
// catalogue carries things like:
//
//   VITRAKVI 100 MG 60 CP &#8211; LAROTRECTINIBE     (– en dash)
//   L&#039Instant de Guerlain                        (' apostrophe, NO semicolon)
//
// Left alone these reach the app, the page title, and — once products have real
// URLs — the address itself. "&#8211;" in a Google result is the kind of detail
// that makes a site look abandoned.
//
// The trap is the other direction. Measured on the same data:
//
//   Dia &Noite     &Lo     &Suaviza
//
// Those are an ordinary ampersand followed by a word, not entities at all. A
// decoder that treats "&" plus letters as an entity mangles real names to fix
// fake ones. So named entities require their semicolon, and only numeric ones
// may go without — there the digits end the token unambiguously, which is why
// "&#039Instant" can be read but "&Noite" cannot.
// ──────────────────────────────────────────────────────────

// Only the ones that actually appear in scraped pharmacy markup. A longer table
// is not safer: every extra name is another word that must never be mistaken
// for prose, and nothing here is worth guessing at.
const NAMED = {
  amp: '&',
  lt: '<',
  gt: '>',
  quot: '"',
  apos: "'",
  nbsp: ' ',
  ndash: '–',
  mdash: '—',
  lsquo: '‘',
  rsquo: '’',
  ldquo: '“',
  rdquo: '”',
  hellip: '…',
  deg: '°',
  reg: '®',
  trade: '™',
  eacute: 'é',
  ccedil: 'ç',
  atilde: 'ã',
  otilde: 'õ',
  aacute: 'á',
  oacute: 'ó',
  iacute: 'í',
  uacute: 'ú',
  acirc: 'â',
  ecirc: 'ê',
  ocirc: 'ô',
  agrave: 'à',
};

// Codepoints that must not be produced even when the source asks for them:
// nothing that could smuggle markup into a page built from this text, and
// nothing invisible that would make two different names look identical.
function safeFromCodePoint(code) {
  if (!Number.isFinite(code) || code <= 0) return null;
  if (code > 0x10ffff) return null;
  // Surrogates are not standalone characters.
  if (code >= 0xd800 && code <= 0xdfff) return null;
  // C0/C1 controls, except the whitespace that can legitimately appear.
  if (code < 0x20 && code !== 0x09 && code !== 0x0a && code !== 0x0d) return null;
  if (code >= 0x7f && code <= 0x9f) return null;

  try {
    return String.fromCodePoint(code);
  } catch {
    return null;
  }
}

/**
 * Decodes the HTML entities a scraper leaves behind, and nothing else.
 *
 * @param {unknown} value
 * @returns {string}
 */
function decodeHtmlEntities(value) {
  const text = value == null ? '' : String(value);
  if (!text.includes('&')) return text;

  return text
    // Hex: &#x2013; or &#x2013 — the hex digits end the token on their own.
    .replace(/&#[xX]([0-9a-fA-F]+);?/g, (match, hex) => {
      const ch = safeFromCodePoint(parseInt(hex, 16));
      return ch === null ? match : ch;
    })
    // Decimal: &#8211; or &#039 — same reasoning, so the semicolon is optional.
    .replace(/&#(\d+);?/g, (match, dec) => {
      const ch = safeFromCodePoint(parseInt(dec, 10));
      return ch === null ? match : ch;
    })
    // Named: the semicolon is REQUIRED. Without it "&Noite" would decode as
    // something, and "Dia &Noite" is a real product name.
    .replace(/&([a-zA-Z][a-zA-Z0-9]{1,31});/g, (match, name) => {
      const hit = NAMED[name] ?? NAMED[name.toLowerCase()];
      return hit === undefined ? match : hit;
    });
}

/** True when decoding would change the text — used to skip untouched rows. */
function hasHtmlEntities(value) {
  const text = value == null ? '' : String(value);
  return decodeHtmlEntities(text) !== text;
}

module.exports = { decodeHtmlEntities, hasHtmlEntities, NAMED };
