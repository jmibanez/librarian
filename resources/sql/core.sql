-- :name insert-document-version! :insert
INSERT INTO document
(id, previous, version, document)
VALUES
(:id, :previous, :version, :document)
RETURNING id, previous, version, document;

-- :name insert-document-header! :insert
INSERT INTO document_header
(id, type, name, context, state, date_created, date_last_modified)
VALUES
(:id, :type, :name, :context, :state, NOW(), NOW())
RETURNING id, type, name, state, context, date_created, date_last_modified;

-- :name insert-transaction-row! :insert
INSERT INTO transaction_stub
(id, context, timeout, state, last_operation)
VALUES
(:id, :context, :timeout, :state, NOW())
RETURNING id, context, timeout, state, last_operation;

-- :name bind-document-version-to-tx! :! :n
INSERT INTO transaction_document_item
(transaction_id, document_id, version)
VALUES
(:transaction-id, :document-id, :version);

-- :name bind-document-state-update-to-tx! :! :n
INSERT INTO transaction_document_state_item
(transaction_id, document_id, version, new_state)
VALUES
(:transaction-id, :document-id, :version, :state);

-- :name touch-transaction-stub! :! :n
UPDATE transaction_stub
SET
  state = 'dirty',
  last_operation = NOW()
WHERE id = :id;

-- :name update-transaction-stub-state! :! :update
UPDATE transaction_stub
SET
  state = :state,
  last_operation = NOW()
WHERE id = :id;

-- :name select-applicable-count-in-transaction :? :1
WITH transaction_docs AS (
    SELECT
      h.id,
      h.current_version,
      h.state,
      d.previous AS expected_version,
      d.version AS new_version,
      COALESCE(t_state.new_state, h.state) AS new_state
    FROM
      document_header h
    JOIN
      document d
      ON
        h.id = d.id
    LEFT JOIN
      transaction_document_item t_doc
      ON
        t_doc.document_id = d.id
        AND t_doc.version = d.version
    LEFT JOIN
      transaction_document_state_item t_state
      ON
        t_state.document_id = d.id
        AND t_state.version = d.version
    JOIN
      transaction_stub t_stub
      ON
        t_doc.transaction_id = t_stub.id
        OR t_state.transaction_id = t_stub.id
    WHERE
      t_stub.id = :transaction-id
), applicable AS (
   SELECT
     COUNT(1) num
   FROM
     transaction_docs t_doc
   WHERE
     t_doc.current_version = t_doc.expected_version
     OR (t_doc.current_version IS NULL AND t_doc.expected_version IS NULL)
), total AS (
   SELECT
     COUNT(1) num
   FROM
     transaction_docs t_doc
)
SELECT
  total.num AS total_num,
  applicable.num AS applicable_num
FROM
  total, applicable

-- :name commit-transaction-details! :! :n
UPDATE
  document_header
SET current_version = transaction_docs.new_version,
    state = transaction_docs.new_state,
    date_last_modified = now()
FROM (
  SELECT
    h.id,
    h.current_version,
    h.state,
    d.previous AS expected_version,
    d.version AS new_version,
    COALESCE(t_state.new_state, h.state) AS new_state
  FROM
    document_header h
  JOIN
    document d
    ON
      h.id = d.id
  LEFT JOIN
    transaction_document_item t_doc
    ON
      t_doc.document_id = d.id
      AND t_doc.version = d.version
  LEFT JOIN
    transaction_document_state_item t_state
    ON
      t_state.document_id = d.id
  JOIN
    transaction_stub t_stub
    ON
      t_doc.transaction_id = t_stub.id
      OR t_state.transaction_id = t_stub.id
  WHERE
    t_stub.id = :transaction-id
) transaction_docs
WHERE document_header.id = transaction_docs.id
  AND ((document_header.current_version = transaction_docs.expected_version AND transaction_docs.expected_version IS NOT NULL)
       OR (document_header.current_version IS NULL AND transaction_docs.expected_version IS NULL));

-- :name clear-transaction-documents! :! :n
DELETE FROM transaction_document_item
WHERE transaction_id = :id

-- :name clear-transaction-state-updates! :! :n
DELETE FROM transaction_document_state_item
WHERE transaction_id = :id

-- :name select-transaction-stub :? :1
SELECT
  id, context, timeout, state, last_operation
FROM
  transaction_stub
WHERE id = :id

-- :name select-document-header :? :1
SELECT
  h.id, h.type, h.name, h.state, h.context, h.current_version,
  h.date_created, h.date_last_modified
FROM
  document_header h
WHERE
  h.id = :id;

-- :name select-recent-document-by-id :? :1
SELECT
  h.id, h.type, h.name, h.state, h.context, h.current_version AS version,
  h.date_created, h.date_last_modified,
  d.document
FROM
  document_header h
JOIN
  document d
  ON h.id = d.id AND h.current_version = d.version
WHERE
  h.id = :id
  AND (h.context = :context OR h.context IS NULL);

-- :name select-recent-document-by-name :? :1
SELECT
  h.id, h.type, h.name, h.state, h.context, h.current_version AS version,
  h.date_created, h.date_last_modified,
  d.document
FROM
  document_header h
JOIN
  document d
  ON h.id = d.id AND h.current_version = d.version
WHERE
  h.name = :name
  AND (h.context = :context OR h.context IS NULL);

-- :name select-document-by-id-and-version :? :1
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
  h.id = :id
  AND (h.context = :context OR h.context IS NULL)
  AND d.version = :version;

-- :name select-all-recent-documents-by-type :? :*
SELECT
  h.id, h.type, h.state, h.context, d.version,
  h.date_created, h.date_last_modified,
  d.document
FROM
  document_header h
JOIN
  document d
  ON h.id = d.id AND h.current_version = d.version
WHERE
  h.type = :type
  AND (h.context = :context OR h.context IS NULL)

-- :name select-current-version-for-document :? :1
WITH RECURSIVE version_list AS (
    SELECT
      0 as version_id, d.id, d.previous, d.version
    FROM
      document d
    WHERE
      d.previous IS NULL
      AND d.id = :id

    UNION

    SELECT
      vv.version_id + 1, prev.id, prev.previous, prev.version
    FROM
      document prev
    JOIN
      version_list vv
      ON prev.previous = vv.version
), version_heads AS (
    SELECT
      vv.version_id, vv.version
    FROM
      version_list vv
    JOIN
      transaction_document_item t_doc
      ON
        t_doc.document_id = vv.id
        AND t_doc.version = vv.version
    WHERE
      t_doc.transaction_id = :transaction-id

    UNION

    SELECT
      vv.version_id, vv.version
    FROM
      version_list vv
    JOIN
      document_header h
      ON
        h.id = vv.id
        AND h.current_version = vv.version
    WHERE
      h.id = :id
)
SELECT
  vv.version
FROM (
    SELECT
      MAX(version_id) version_id
    FROM
      version_heads
) latest_head
JOIN
  version_list vv
  ON
    vv.version_id = latest_head.version_id

-- :name select-next-version-for-document :? :1
SELECT
  MAX(version) + 1 AS version
FROM (
  SELECT
    h.current_version AS version
  FROM
    document_header h
  WHERE
    h.id = :id

  UNION

  SELECT
    d.version
  FROM
    transaction_document_item t_doc
  JOIN
    document d
    ON
      t_doc.document_id = d.id
      AND t_doc.version = d.version
  WHERE
    t_doc.transaction_id = :transaction-id
    AND t_doc.document_id = :id
) doc_view;


-- :name select-versions-of-document-by-id :? :*
WITH RECURSIVE version_list AS (
    SELECT
      0 as version_id, d.id, d.previous, d.version
    FROM
      document d
    JOIN
      document_header h
      ON
        h.id = d.id
    WHERE
      d.previous IS NULL
      AND d.id = :id
      AND (h.context = :context OR h.context IS NULL)

    UNION

    SELECT
      vv.version_id + 1, prev.id, prev.previous, prev.version
    FROM
      document prev
    JOIN
      version_list vv
      ON prev.previous = vv.version
)
SELECT
  h.id, h.type, h.name, h.state, h.context, d.version,
  h.date_created, h.date_last_modified,
  d.document
FROM
  document_header h
JOIN
  version_list v
  ON h.id = v.id
JOIN
  document d
  ON v.version = d.version
ORDER BY v.version_id;

