CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

CREATE TABLE document_header (
    id uuid NOT NULL,
    name character varying(100),
    context uuid,
    type uuid,
    state character varying(50) NOT NULL,
    current_version character varying(64),
    date_created timestamp with time zone NOT NULL,
    date_last_modified timestamp with time zone NOT NULL,

    PRIMARY KEY (id),
    UNIQUE (context, type, name)
);

CREATE TABLE document (
    id uuid NOT NULL,
    document jsonb,

    previous character varying(64),
    version character varying(64) NOT NULL,

    PRIMARY KEY (id, version),
    FOREIGN KEY (id) REFERENCES document_header(id),
    UNIQUE (version)
);

ALTER TABLE document ADD FOREIGN KEY (previous) REFERENCES document(version);

CREATE INDEX idx_document_version_path
ON document
USING btree (previous);

CREATE TABLE transaction_stub (
    id uuid NOT NULL,
    context uuid NOT NULL,
    timeout integer NOT NULL,
    state character varying(40) NOT NULL,
    last_operation timestamp with time zone NOT NULL,

    PRIMARY KEY (id)
);

CREATE TABLE transaction_document_item (
    transaction_id uuid NOT NULL,
    document_id uuid NOT NULL,
    version character varying(64) NOT NULL,

    UNIQUE (transaction_id, document_id, version),
    FOREIGN KEY (transaction_id) REFERENCES transaction_stub(id),
    FOREIGN KEY (document_id) REFERENCES document_header(id),
    FOREIGN KEY (document_id, version) REFERENCES document(id, version)
);

CREATE TABLE transaction_document_state_item (
    transaction_id uuid NOT NULL,
    document_id uuid NOT NULL,
    version character varying(64) NOT NULL,
    new_state character varying (50),

    UNIQUE (transaction_id, document_id, version),
    FOREIGN KEY (transaction_id) REFERENCES transaction_stub(id),
    FOREIGN KEY (document_id) REFERENCES document_header(id)
);
