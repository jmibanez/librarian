-- :name ensure-paths! :! :1
INSERT INTO type_index_path (type, path)
SELECT h.type, :path
FROM
    document_header h
    WHERE h.id = :document_id
ON CONFLICT DO NOTHING

-- :name ensure-values! :! :1
INSERT INTO document_index_value (path_id, value)
SELECT p.id, jsonb(:value)
FROM
    document_header h
JOIN
    type_index_path p
    ON p.path = :path
    AND h.type = p.type
WHERE h.id = :document_id
ON CONFLICT DO NOTHING

-- :name insert-indexes! :! :1
INSERT INTO document_index
(document_id, version, value_id)
SELECT
    :document_id, :version, idx_val.id
FROM
    type_index_path p
JOIN
    document_index_value idx_val
    ON p.id = idx_val.path_id
       AND jsonb(:value) = idx_val.value
WHERE p.path = :path
ON CONFLICT DO NOTHING;

-- :name invalidate-index-for-document-and-version! :!
DELETE FROM document_index
WHERE document_id = :id
      AND version = :version;

-- :name select-unindexed-documents
SELECT
  h.id, h.type, h.name, h.state, h.context, d.version,
  h.date_created, h.date_last_modified,
  d.document
FROM
  document_header h
JOIN
  document d
  ON h.id = d.id
WHERE
  (h.id, d.version) NOT IN (
     SELECT DISTINCT document_id, version
     FROM document_index
  );

-- :name select-documents-for-open-transactions
SELECT
  td.transaction_id, td.document_id, td.version
FROM
  transaction_document_item td
JOIN
  transaction_stub t
  ON t.id = td.transaction_id
WHERE
  t.state = 'dirty'
GROUP BY td.transaction_id, td.document_id, td.version;
