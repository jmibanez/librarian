ALTER TABLE transaction_document_item
    ADD COLUMN expected_version character varying(64),
    DROP CONSTRAINT transaction_document_item_transaction_id_document_id_versio_key,
    ADD UNIQUE (transaction_id, document_id);
