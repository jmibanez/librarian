ALTER TABLE transaction_document_item
    DROP COLUMN expected_version,
    DROP CONSTRAINT transaction_document_item_transaction_id_document_id_key,
    ADD UNIQUE (transaction_id, document_id, version);
