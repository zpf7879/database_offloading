-- =============================================================
-- POC source schema: Customer Profile + Relationship domain
-- =============================================================

CREATE DATABASE IF NOT EXISTS offload_poc;
USE offload_poc;

-- Dedicated CDC user (binlog replication privilege required by Debezium)
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'debezium_pass';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

-- -------------------------------------------------------------
-- customer
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customer (
  customer_id       VARCHAR(36)  NOT NULL DEFAULT (UUID()),
  external_ref      VARCHAR(64)  UNIQUE,
  first_name        VARCHAR(100) NOT NULL,
  last_name         VARCHAR(100) NOT NULL,
  date_of_birth     DATE,
  gender            CHAR(1),
  nationality       VARCHAR(3),
  preferred_locale  VARCHAR(10)  DEFAULT 'en-AU',
  status            ENUM('ACTIVE','INACTIVE','DECEASED','SUSPENDED') NOT NULL DEFAULT 'ACTIVE',
  created_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (customer_id),
  INDEX idx_customer_external_ref (external_ref),
  INDEX idx_customer_status (status),
  INDEX idx_customer_updated_at (updated_at)
) ENGINE=InnoDB;

-- -------------------------------------------------------------
-- customer_address
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customer_address (
  address_id        VARCHAR(36)  NOT NULL DEFAULT (UUID()),
  customer_id       VARCHAR(36)  NOT NULL,
  address_type      ENUM('RESIDENTIAL','MAILING','BUSINESS') NOT NULL,
  line1             VARCHAR(200),
  line2             VARCHAR(200),
  city              VARCHAR(100),
  state             VARCHAR(50),
  postcode          VARCHAR(20),
  country           VARCHAR(3)   DEFAULT 'AUS',
  is_primary        TINYINT(1)   NOT NULL DEFAULT 0,
  valid_from        DATE,
  valid_to          DATE,
  updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (address_id),
  INDEX idx_addr_customer_id (customer_id),
  CONSTRAINT fk_addr_customer FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
) ENGINE=InnoDB;

-- -------------------------------------------------------------
-- customer_contact
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customer_contact (
  contact_id        VARCHAR(36)  NOT NULL DEFAULT (UUID()),
  customer_id       VARCHAR(36)  NOT NULL,
  contact_type      ENUM('EMAIL','MOBILE','HOME_PHONE','WORK_PHONE') NOT NULL,
  contact_value     VARCHAR(255) NOT NULL,
  is_primary        TINYINT(1)   NOT NULL DEFAULT 0,
  is_verified       TINYINT(1)   NOT NULL DEFAULT 0,
  updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (contact_id),
  INDEX idx_contact_customer_id (customer_id),
  CONSTRAINT fk_contact_customer FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
) ENGINE=InnoDB;

-- -------------------------------------------------------------
-- customer_identification
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customer_identification (
  id_record_id      VARCHAR(36)  NOT NULL DEFAULT (UUID()),
  customer_id       VARCHAR(36)  NOT NULL,
  id_type           ENUM('PASSPORT','DRIVERS_LICENCE','NATIONAL_ID','TFN') NOT NULL,
  id_number         VARCHAR(100) NOT NULL,
  issuing_authority VARCHAR(100),
  issue_date        DATE,
  expiry_date       DATE,
  updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id_record_id),
  INDEX idx_ident_customer_id (customer_id),
  CONSTRAINT fk_ident_customer FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
) ENGINE=InnoDB;

-- -------------------------------------------------------------
-- customer_tax
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customer_tax (
  tax_record_id     VARCHAR(36)  NOT NULL DEFAULT (UUID()),
  customer_id       VARCHAR(36)  NOT NULL,
  tax_country       VARCHAR(3)   NOT NULL DEFAULT 'AUS',
  tax_id            VARCHAR(50),
  tin_type          VARCHAR(50),
  updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (tax_record_id),
  INDEX idx_tax_customer_id (customer_id),
  CONSTRAINT fk_tax_customer FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
) ENGINE=InnoDB;

-- -------------------------------------------------------------
-- relationship  (party-to-party links)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS relationship (
  relationship_id   VARCHAR(36)  NOT NULL DEFAULT (UUID()),
  party_id_from     VARCHAR(36)  NOT NULL,
  party_id_to       VARCHAR(36)  NOT NULL,
  relationship_type ENUM('JOINT_HOLDER','POWER_OF_ATTORNEY','GUARANTOR','BENEFICIARY','TRUSTEE','EMPLOYER') NOT NULL,
  valid_from        DATE,
  valid_to          DATE,
  status            ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
  updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (relationship_id),
  INDEX idx_rel_party_from (party_id_from),
  INDEX idx_rel_party_to (party_id_to),
  CONSTRAINT fk_rel_from FOREIGN KEY (party_id_from) REFERENCES customer(customer_id),
  CONSTRAINT fk_rel_to   FOREIGN KEY (party_id_to)   REFERENCES customer(customer_id)
) ENGINE=InnoDB;

-- =============================================================
-- Seed data: realistic volumes with skewed patterns
-- =============================================================

-- Insert 10 base customers (procedure expands this to thousands via seeder script)
INSERT INTO customer (customer_id, external_ref, first_name, last_name, date_of_birth, gender, nationality, status) VALUES
  ('cust-0001', 'EXT-0001', 'Alice',   'Nguyen',    '1985-03-12', 'F', 'AUS', 'ACTIVE'),
  ('cust-0002', 'EXT-0002', 'Bob',     'Smith',     '1972-07-22', 'M', 'AUS', 'ACTIVE'),
  ('cust-0003', 'EXT-0003', 'Carol',   'Williams',  '1990-11-05', 'F', 'NZL', 'ACTIVE'),
  ('cust-0004', 'EXT-0004', 'David',   'Brown',     '1968-01-30', 'M', 'AUS', 'INACTIVE'),
  ('cust-0005', 'EXT-0005', 'Emma',    'Jones',     '1995-06-18', 'F', 'AUS', 'ACTIVE'),
  ('cust-0006', 'EXT-0006', 'Frank',   'Taylor',    '1980-09-09', 'M', 'AUS', 'ACTIVE'),
  ('cust-0007', 'EXT-0007', 'Grace',   'Anderson',  '1978-12-25', 'F', 'AUS', 'SUSPENDED'),
  ('cust-0008', 'EXT-0008', 'Henry',   'Thomas',    '1962-04-14', 'M', 'GBR', 'ACTIVE'),
  ('cust-0009', 'EXT-0009', 'Isla',    'Jackson',   '2000-08-01', 'F', 'AUS', 'ACTIVE'),
  ('cust-0010', 'EXT-0010', 'James',   'White',     '1955-02-28', 'M', 'AUS', 'ACTIVE');

-- Addresses (hot customer cust-0001 has 3 addresses to simulate skew)
INSERT INTO customer_address (address_id, customer_id, address_type, line1, city, state, postcode, country, is_primary) VALUES
  ('addr-0001', 'cust-0001', 'RESIDENTIAL', '12 George St',     'Sydney',    'NSW', '2000', 'AUS', 1),
  ('addr-0002', 'cust-0001', 'MAILING',     'PO Box 100',       'Sydney',    'NSW', '2001', 'AUS', 0),
  ('addr-0003', 'cust-0001', 'BUSINESS',    '1 Martin Place',   'Sydney',    'NSW', '2000', 'AUS', 0),
  ('addr-0004', 'cust-0002', 'RESIDENTIAL', '45 Collins St',    'Melbourne', 'VIC', '3000', 'AUS', 1),
  ('addr-0005', 'cust-0003', 'RESIDENTIAL', '88 Queen St',      'Brisbane',  'QLD', '4000', 'AUS', 1),
  ('addr-0006', 'cust-0004', 'RESIDENTIAL', '200 Adelaide St',  'Brisbane',  'QLD', '4001', 'AUS', 1),
  ('addr-0007', 'cust-0005', 'RESIDENTIAL', '5 Rundle Mall',    'Adelaide',  'SA',  '5000', 'AUS', 1),
  ('addr-0008', 'cust-0006', 'RESIDENTIAL', '100 St Georges Tc','Perth',     'WA',  '6000', 'AUS', 1),
  ('addr-0009', 'cust-0007', 'RESIDENTIAL', '33 Murray St',     'Hobart',    'TAS', '7000', 'AUS', 1),
  ('addr-0010', 'cust-0008', 'RESIDENTIAL', '7 London Circuit', 'Canberra',  'ACT', '2600', 'AUS', 1);

-- Contacts
INSERT INTO customer_contact (contact_id, customer_id, contact_type, contact_value, is_primary, is_verified) VALUES
  ('cont-0001', 'cust-0001', 'EMAIL',       'alice@example.com',   1, 1),
  ('cont-0002', 'cust-0001', 'MOBILE',      '+61400000001',        1, 1),
  ('cont-0003', 'cust-0002', 'EMAIL',       'bob@example.com',     1, 1),
  ('cont-0004', 'cust-0002', 'MOBILE',      '+61400000002',        1, 0),
  ('cont-0005', 'cust-0003', 'EMAIL',       'carol@example.com',   1, 1),
  ('cont-0006', 'cust-0004', 'HOME_PHONE',  '+61700000004',        1, 0),
  ('cont-0007', 'cust-0005', 'EMAIL',       'emma@example.com',    1, 1),
  ('cont-0008', 'cust-0006', 'MOBILE',      '+61400000006',        1, 1),
  ('cont-0009', 'cust-0007', 'EMAIL',       'grace@example.com',   1, 0),
  ('cont-0010', 'cust-0008', 'WORK_PHONE',  '+61200000008',        1, 1);

-- Relationships
INSERT INTO relationship (relationship_id, party_id_from, party_id_to, relationship_type, valid_from, status) VALUES
  ('rel-0001', 'cust-0001', 'cust-0002', 'JOINT_HOLDER',      '2020-01-01', 'ACTIVE'),
  ('rel-0002', 'cust-0003', 'cust-0001', 'POWER_OF_ATTORNEY', '2021-06-01', 'ACTIVE'),
  ('rel-0003', 'cust-0005', 'cust-0004', 'GUARANTOR',         '2019-03-15', 'ACTIVE'),
  ('rel-0004', 'cust-0008', 'cust-0009', 'BENEFICIARY',       '2022-08-10', 'ACTIVE'),
  ('rel-0005', 'cust-0010', 'cust-0006', 'TRUSTEE',           '2018-11-20', 'INACTIVE');
