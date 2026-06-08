# ER Diagram — Offload POC Source Schema

```mermaid
erDiagram
    customer {
        VARCHAR customer_id PK
        VARCHAR external_ref
        VARCHAR first_name
        VARCHAR last_name
        DATE date_of_birth
        CHAR gender
        VARCHAR nationality
        VARCHAR preferred_locale
        ENUM status
        DATETIME created_at
        DATETIME updated_at
    }

    customer_address {
        VARCHAR address_id PK
        VARCHAR customer_id FK
        ENUM address_type
        VARCHAR line1
        VARCHAR line2
        VARCHAR city
        VARCHAR state
        VARCHAR postcode
        VARCHAR country
        TINYINT is_primary
        DATE valid_from
        DATE valid_to
        DATETIME updated_at
    }

    customer_contact {
        VARCHAR contact_id PK
        VARCHAR customer_id FK
        ENUM contact_type
        VARCHAR contact_value
        TINYINT is_primary
        TINYINT is_verified
        DATETIME updated_at
    }

    customer_identification {
        VARCHAR id_record_id PK
        VARCHAR customer_id FK
        ENUM id_type
        VARCHAR id_number
        VARCHAR issuing_authority
        DATE issue_date
        DATE expiry_date
        DATETIME updated_at
    }

    customer_tax {
        VARCHAR tax_record_id PK
        VARCHAR customer_id FK
        VARCHAR tax_country
        VARCHAR tax_id
        VARCHAR tin_type
        DATETIME updated_at
    }

    relationship {
        VARCHAR relationship_id PK
        VARCHAR party_id_from FK
        VARCHAR party_id_to FK
        ENUM relationship_type
        DATE valid_from
        DATE valid_to
        ENUM status
        DATETIME updated_at
    }

    customer ||--o{ customer_address       : "has"
    customer ||--o{ customer_contact       : "has"
    customer ||--o{ customer_identification : "has"
    customer ||--o{ customer_tax           : "has"
    customer ||--o{ relationship           : "initiates"
    customer ||--o{ relationship           : "receives"
```

## Relationship Notes

| Relationship | Cardinality | Description |
|---|---|---|
| customer → customer_address | one-to-many | A customer can have multiple addresses (residential, mailing, business) |
| customer → customer_contact | one-to-many | A customer can have multiple contact methods (email, mobile, phone) |
| customer → customer_identification | one-to-many | A customer can hold multiple ID documents (passport, licence, etc.) |
| customer → customer_tax | one-to-many | A customer can have tax records across multiple countries |
| customer → relationship (as from) | one-to-many | A customer can initiate multiple party-to-party relationships |
| customer → relationship (as to) | one-to-many | A customer can be the target of multiple party-to-party relationships |

## MongoDB Target Mapping

All 6 tables collapse into **one document per customer** in MongoDB:

```
customer_profile {
  customer_id        ← customer.customer_id
  ...core fields...  ← customer.*
  addresses[]        ← customer_address.*
  contacts[]         ← customer_contact.*
  identifications[]  ← customer_identification.*
  tax_records[]      ← customer_tax.*
  relationships[]    ← relationship.*
}
```
