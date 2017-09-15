ALTER TABLE document_index
DROP CONSTRAINT document_index_document_id_fkey,
ADD FOREIGN KEY (document_id, version)
    REFERENCES document (id, version);
