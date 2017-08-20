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
