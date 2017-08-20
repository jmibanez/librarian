-- :name select-type-row-by-context-and-id :? :1
SELECT
  id, owner, name,
  definition,
FROM
  document_type t
WHERE
  t.owner = :context
  AND t.id = :id;

-- :name select-type-row-by-context-and-name
SELECT
  id, owner, name,
  definition,
FROM
  document_type t
WHERE
  t.owner = :context
  AND t.name = :name;

-- :name upsert-type-row
INSERT INTO document_type
(id, owner, name, definition)
VALUES
(:id, :context, :name, :definition)
ON CONFLICT DO
UPDATE
SET name = :name
    definition = :definition;
