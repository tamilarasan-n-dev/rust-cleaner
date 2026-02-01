
// JSON validator
const fs = require('fs');
const readline = require('readline');

const input = fs.createReadStream('compact.ndjson');
const rl = readline.createInterface({ input });

const out = fs.createWriteStream('with_phone.ndjson');

let matched = 0;
let total = 0;

rl.on('line', (line) => {
  if (!line) return;

  total++;

  let obj;
  try {
    obj = JSON.parse(line);
  } catch (e) {
    return; // skip invalid JSON line
  }

  const hasMobile =
    typeof obj.mobile_phone === 'string' &&
    obj.mobile_phone.trim().length > 0;

  const hasPhoneNumbers =
    Array.isArray(obj.phone_numbers) &&
    obj.phone_numbers.length > 0 &&
    obj.phone_numbers.some(p => typeof p === 'string' && p.trim().length > 0);

  if (hasMobile || hasPhoneNumbers) {
    out.write(line + '\n');
    matched++;
  }
});

rl.on('close', () => {
  out.end();
  console.log(`DONE. matched=${matched}, total=${total}`);
});

