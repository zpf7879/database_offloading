import mysql from "mysql2/promise";
import "dotenv/config";

let pool;

export function getRdsPool() {
  if (!pool) {
    pool = mysql.createPool({
      host:     process.env.MYSQL_HOST     || "localhost",
      port:     parseInt(process.env.MYSQL_PORT || "3306"),
      user:     process.env.MYSQL_USER     || "poc_user",
      password: process.env.MYSQL_PASSWORD || "poc_pass",
      database: process.env.MYSQL_DATABASE || "offload_poc",
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
    });
  }
  return pool;
}

// Baseline read: full join across all customer sub-tables (simulates the mainframe join cost)
export async function getCustomerRelationalFull(customerId) {
  const pool = getRdsPool();
  const [rows] = await pool.execute(
    `SELECT
       c.customer_id, c.external_ref, c.first_name, c.last_name,
       c.date_of_birth, c.gender, c.nationality, c.status, c.updated_at,
       ca.address_id, ca.address_type, ca.line1, ca.line2,
       ca.city, ca.state, ca.postcode, ca.country, ca.is_primary AS addr_primary,
       cc.contact_id, cc.contact_type, cc.contact_value, cc.is_verified,
       ci.id_type, ci.id_number, ci.expiry_date,
       ct.tax_country, ct.tax_id,
       r.relationship_id, r.party_id_from, r.party_id_to,
       r.relationship_type, r.status AS rel_status
     FROM customer c
     LEFT JOIN customer_address        ca ON ca.customer_id = c.customer_id
     LEFT JOIN customer_contact        cc ON cc.customer_id = c.customer_id
     LEFT JOIN customer_identification ci ON ci.customer_id = c.customer_id
     LEFT JOIN customer_tax            ct ON ct.customer_id = c.customer_id
     LEFT JOIN relationship             r ON r.party_id_from = c.customer_id OR r.party_id_to = c.customer_id
     WHERE c.customer_id = ?`,
    [customerId]
  );
  return rows;
}
