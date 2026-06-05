/**
 * Seeds the offload_poc database with realistic skewed data.
 * Generates CUSTOMER_COUNT customers, with:
 *   - ~5% "hot" customers who have 3+ addresses
 *   - ~10% with multiple relationships
 *   - ~20% sparse (missing contact or identification)
 *
 * Usage: node src/schema/seeder.js [--count=1000]
 */
import { getRdsPool } from "../db/rds.js";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => a.replace(/^--/, "").split("="))
);
const COUNT = parseInt(args.count || "1000");

const FIRST_NAMES = ["Alice","Bob","Carol","David","Emma","Frank","Grace","Henry","Isla","James","Karen","Liam","Mia","Noah","Olivia","Paul","Quinn","Ryan","Sofia","Tom"];
const LAST_NAMES  = ["Nguyen","Smith","Williams","Brown","Jones","Taylor","Anderson","Thomas","Jackson","White","Harris","Martin","Garcia","Martinez","Robinson","Clark","Lewis","Lee","Walker","Hall"];
const CITIES      = [["Sydney","NSW","2000"],["Melbourne","VIC","3000"],["Brisbane","QLD","4000"],["Perth","WA","6000"],["Adelaide","SA","5000"],["Canberra","ACT","2600"],["Hobart","TAS","7000"]];
const CONTACT_TYPES = ["EMAIL","MOBILE","HOME_PHONE","WORK_PHONE"];
const REL_TYPES   = ["JOINT_HOLDER","POWER_OF_ATTORNEY","GUARANTOR","BENEFICIARY","TRUSTEE"];

function rand(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function uuid()    { return crypto.randomUUID(); }
function pad(n)    { return String(n).padStart(4, "0"); }

async function seed() {
  const pool = getRdsPool();
  console.log(`Seeding ${COUNT} customers…`);

  const customerIds = [];

  for (let i = 1; i <= COUNT; i++) {
    const cid   = `cust-${pad(i + 1000)}`;
    const ext   = `EXT-${pad(i + 1000)}`;
    const fn    = rand(FIRST_NAMES);
    const ln    = rand(LAST_NAMES);
    const dob   = `${1940 + Math.floor(Math.random() * 65)}-${pad(Math.ceil(Math.random() * 12))}-${pad(Math.ceil(Math.random() * 28))}`;
    const status = Math.random() < 0.05 ? "INACTIVE" : "ACTIVE";

    await pool.execute(
      `INSERT IGNORE INTO customer (customer_id, external_ref, first_name, last_name, date_of_birth, gender, nationality, status)
       VALUES (?, ?, ?, ?, ?, ?, 'AUS', ?)`,
      [cid, ext, fn, ln, dob, Math.random() < 0.5 ? "M" : "F", status]
    );
    customerIds.push(cid);

    // Addresses: hot customers (5%) get 3, rest get 1
    const addrCount = Math.random() < 0.05 ? 3 : 1;
    for (let a = 0; a < addrCount; a++) {
      const [city, state, postcode] = rand(CITIES);
      await pool.execute(
        `INSERT IGNORE INTO customer_address (address_id, customer_id, address_type, line1, city, state, postcode, country, is_primary)
         VALUES (?, ?, ?, ?, ?, ?, ?, 'AUS', ?)`,
        [uuid(), cid, a === 0 ? "RESIDENTIAL" : "MAILING", `${Math.ceil(Math.random()*200)} Main St`, city, state, postcode, a === 0 ? 1 : 0]
      );
    }

    // Contacts: ~80% have at least one
    if (Math.random() < 0.8) {
      const ctype = rand(CONTACT_TYPES);
      const val   = ctype === "EMAIL" ? `user${i}@example.com` : `+614${String(i).padStart(8,"0")}`;
      await pool.execute(
        `INSERT IGNORE INTO customer_contact (contact_id, customer_id, contact_type, contact_value, is_primary, is_verified)
         VALUES (?, ?, ?, ?, 1, ?)`,
        [uuid(), cid, ctype, val, Math.random() < 0.7 ? 1 : 0]
      );
    }

    // Tax: 90% have TFN
    if (Math.random() < 0.9) {
      await pool.execute(
        `INSERT IGNORE INTO customer_tax (tax_record_id, customer_id, tax_country, tax_id, tin_type)
         VALUES (?, ?, 'AUS', ?, 'TFN')`,
        [uuid(), cid, String(Math.floor(100000000 + Math.random() * 899999999))]
      );
    }

    if (i % 100 === 0) process.stdout.write(`  ${i}/${COUNT}\r`);
  }

  // Relationships: ~10% of customers linked to another
  const relCandidates = customerIds.filter(() => Math.random() < 0.1);
  for (const fromId of relCandidates) {
    const toId = rand(customerIds.filter((id) => id !== fromId));
    await pool.execute(
      `INSERT IGNORE INTO relationship (relationship_id, party_id_from, party_id_to, relationship_type, valid_from, status)
       VALUES (?, ?, ?, ?, CURDATE(), 'ACTIVE')`,
      [uuid(), fromId, toId, rand(REL_TYPES)]
    );
  }

  console.log(`\nDone. Seeded ${COUNT} customers.`);
  process.exit(0);
}

seed().catch((err) => { console.error(err); process.exit(1); });
