ALTER TABLE document
    ADD COLUMN state character varying(50),
    ADD COLUMN date_modified timestamp with time zone DEFAULT NOW();

UPDATE document
  SET state = h.state,
      date_modified = h.date_last_modified
FROM (
  SELECT
    h.id,
    h.state,
    h.date_last_modified
  FROM
    document_header h
) h
WHERE
  h.id = document.id;

ALTER TABLE document
    ALTER COLUMN state SET NOT NULL,
    ALTER COLUMN date_modified SET NOT NULL;

ALTER TABLE document_header
    DROP COLUMN state,
    DROP COLUMN date_last_modified;


DROP TABLE transaction_document_state_item;
