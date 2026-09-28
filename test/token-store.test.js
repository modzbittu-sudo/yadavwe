const fs = require('node:fs');
const test = require('node:test');
const assert = require('node:assert/strict');

const { parseTokenList, addTokenToList, persistTokenList, loadTokenFile } = require('../token-store');

test('parseTokenList reads comma and newline separated tokens', () => {
  const tokens = parseTokenList('abc, def\nghi, jkl');
  assert.deepEqual(tokens, ['abc', 'def', 'ghi', 'jkl']);
});

test('addTokenToList prevents duplicates and allows unlimited tokens by default', () => {
  const tokens = ['a', 'b'];
  const result = addTokenToList(tokens, 'b');
  assert.deepEqual(result, ['a', 'b']);

  const next = addTokenToList(tokens, 'c');
  assert.deepEqual(next, ['a', 'b', 'c']);

  const unlimited = addTokenToList(['a', 'b', 'c', 'd', 'e'], 'f');
  assert.deepEqual(unlimited, ['a', 'b', 'c', 'd', 'e', 'f']);

  const limited = addTokenToList(['a', 'b'], 'c', 2);
  assert.deepEqual(limited, ['a', 'b']);
});

test('persistTokenList writes BOT_TOKENS using comma list', () => {
  const filePath = 'test/.env.mock';
  const tokens = ['token1', 'token2'];
  const output = persistTokenList(filePath, tokens);
  assert.equal(output, 'token1,token2');
});

test('loadTokenFile reads newline and comma separated tokens from a txt file', () => {
  const filePath = 'test/.tokens.mock.txt';
  fs.writeFileSync(filePath, 'tokenA\n tokenB, tokenC\n');

  try {
    assert.deepEqual(loadTokenFile(filePath), ['tokenA', 'tokenB', 'tokenC']);
  } finally {
    fs.unlinkSync(filePath);
  }
});
