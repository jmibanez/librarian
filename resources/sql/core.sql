-- :name insert-document-version! :insert
INSERT INTO document
(id, previous, version, document, state, date_modified)
VALUES
(:id, :previous, :version, :document, :state, NOW())
RETURNING id, previous, version, document, state, date_modified;

-- :name insert-document-header! :insert
INSERT INTO document_header
(id, type, name, context, date_created)
VALUES
(:id, :type, :name, :context, NOW())
RETURNING id, type, name, context, date_created;

-- :name insert-transaction-row! :insert
INSERT INTO transaction_stub
(id, context, timeout, state, last_operation)
VALUES
(:id, :context, :timeout, :state, NOW())
RETURNING id, context, timeout, state, last_operation;

-- :name bind-document-version-to-tx! :! :n
INSERT INTO transaction_document_item
(transaction_id, document_id, expected_version, version)
VALUES
(:transaction-id, :document-id, :expected-version, :version)
ON CONFLICT (transaction_id, document_id)
DO UPDATE
  SET version = :version

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
      t_doc.expected_version,
      t_doc.version AS new_version
    FROM
      document_header h
    JOIN
      transaction_document_item t_doc
      ON
        t_doc.document_id = h.id
    JOIN
      transaction_stub t_stub
      ON
        t_doc.transaction_id = t_stub.id
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
SET current_version = transaction_docs.new_version
FROM (
  SELECT
    h.id,
    h.current_version,
    t_doc.expected_version,
    t_doc.version AS new_version
  FROM
    document_header h
  JOIN
    transaction_document_item t_doc
    ON
      t_doc.document_id = h.id
  JOIN
    transaction_stub t_stub
    ON
      t_doc.transaction_id = t_stub.id
  WHERE
    t_stub.id = :transaction-id
) transaction_docs
WHERE document_header.id = transaction_docs.id
  AND ((document_header.current_version = transaction_docs.expected_version AND transaction_docs.expected_version IS NOT NULL)
       OR (document_header.current_version IS NULL AND transaction_docs.expected_version IS NULL));

-- :name clear-transaction-documents! :! :n
DELETE FROM transaction_document_item
WHERE transaction_id = :id;

-- :name clear-unreferenced-documents! :! :n
WITH document_refs AS (
    SELECT
        DISTINCT document_id, version
    FROM
        transaction_document_item
), unrefed_version AS (
    DELETE FROM document
    WHERE (id, version) NOT IN (
       SELECT
           document_id, version
       FROM
           document_refs
    )
)
DELETE FROM document_header
WHERE
    current_version IS NULL
    AND id NOT IN (
        SELECT
            document_id
        FROM
            document_refs
    )

-- :name select-transaction-stub :? :1
SELECT
  id, context, timeout, state, last_operation
FROM
  transaction_stub
WHERE id = :id

-- :name select-open-transaction-stubs :? :*
SELECT
  id, context, timeout, state, last_operation
FROM
  transaction_stub
WHERE
  state = 'dirty' OR state = 'started';

-- :name select-document-header :? :1
SELECT
  h.id, h.type, h.name, h.context, h.current_version,
  h.date_created
FROM
  document_header h
WHERE
  h.id = :id;

-- :name select-recent-document-by-id :? :1
SELECT
  h.id, h.type, h.name, h.context,
  h.date_created,
  d.document, d.state, d.date_modified AS date_last_modified
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
  h.id, h.type, h.name, h.context,
  h.date_created,
  d.document, d.state, d.date_modified AS date_last_modified
FROM
  document_header h
JOIN
  document d
  ON h.id = d.id AND h.current_version = d.version
WHERE
  h.name = :name
  AND h.type = :type
  AND (h.context = :context OR h.context IS NULL);

-- :name select-transaction-document-by-id :? :1
SELECT
  h.id, h.type, h.name, h.context,
  h.date_created,
  d.document, d.state, d.date_modified AS date_last_modified
FROM (
    SELECT
      h.id, h.type, h.name, h.context, h.date_created,
      COALESCE(t_doc.version, h.current_version) AS current_version
    FROM
      document_header h
    LEFT OUTER JOIN
      transaction_document_item t_doc
      ON
        h.id = t_doc.document_id
        AND t_doc.transaction_id = :transaction-id
    -- Do we need this WHERE clause here, or can the outer WHERE limit
    -- the inner query properly for the COALESCE to work?
    WHERE
      h.id = :id
) h
JOIN
  document d
  ON h.id = d.id AND h.current_version = d.version
WHERE
  h.id = :id
  AND (h.context = :context OR h.context IS NULL);

-- :name select-transaction-document-by-name :? :1
SELECT
  h.id, h.type, h.name, h.context, h.current_version AS version,
  h.date_created,
  d.document, d.state, d.date_modified AS date_last_modified
FROM (
    SELECT
      h.id, h.type, h.name, h.context, h.date_created,
      COALESCE(t_doc.version, h.current_version) AS current_version
    FROM
      document_header h
    LEFT OUTER JOIN
      transaction_document_item t_doc
      ON
        h.id = t_doc.document_id
        AND t_doc.transaction_id = :transaction-id
    -- Do we need this WHERE clause here, or can the outer WHERE limit
    -- the inner query properly for the COALESCE to work?
    WHERE
      h.name = :name
) h
JOIN
  document d
  ON h.id = d.id AND h.current_version = d.version
WHERE
  h.name = :name
  AND h.type = :type
  AND (h.context = :context OR h.context IS NULL);

-- :name select-document-by-id-and-version :? :1
SELECT
  h.id, h.type, h.name, h.context,
  h.date_created,
  d.document, d.state, d.date_modified AS date_last_modified
FROM
  document_header h
JOIN
  document d
  ON h.id = d.id
WHERE
  h.id = :id
  AND (h.context = :context OR h.context IS NULL)
  AND d.version = :version;

-- :name select-current-version-for-document :? :1
SELECT
  COALESCE(t_doc.version, h.current_version) AS version
FROM
  document_header h
LEFT OUTER JOIN
  transaction_document_item t_doc
  ON
    h.id = t_doc.document_id
    AND t_doc.transaction_id = :transaction-id
WHERE
  h.id = :id

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
  h.id, h.type, h.name, h.context, d.version,
  h.date_created,
  d.document, d.state, d.date_modified AS date_last_modified
FROM
  document_header h
JOIN
  version_list v
  ON h.id = v.id
JOIN
  document d
  ON v.version = d.version
ORDER BY v.version_id;

