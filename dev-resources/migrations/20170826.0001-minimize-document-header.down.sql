CREATE TABLE transaction_document_state_item (
    transaction_id uuid NOT NULL,
    document_id uuid NOT NULL,
    version character varying(64) NOT NULL,
    new_state character varying (50),

    UNIQUE (transaction_id, document_id, version),
    FOREIGN KEY (transaction_id) REFERENCES transaction_stub(id),
    FOREIGN KEY (document_id) REFERENCES document_header(id)
);

ALTER TABLE document_header
    ADD COLUMN state character varying(50),
    ADD COLUMN date_last_modified timestamp with time zone;

UPDATE document_header
  SET state = d.state,
      date_last_modified = d.date_modified
FROM (
  SELECT
    d.id,
    d.version,
    d.state,
    d.date_modified
  FROM
    document d
) d
WHERE
  d.id = document_header.id
  AND d.version = document_header.current_version;

ALTER TABLE document_header
    ALTER COLUMN state SET NOT NULL,
    ALTER COLUMN date_last_modified SET NOT NULL;

ALTER TABLE document
    DROP COLUMN state,
    DROP COLUMN date_modified;
