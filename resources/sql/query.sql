-- :name query-document-full :? :*
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
  h.type = :v:type
  AND (h.context = :context OR h.context IS NULL)
  AND h.id IN (:snip:query_filter)
/*~ (if-not (nil? (:sort params)) */
ORDER BY :snip:sort
/*~ ) ~*/
LIMIT :count OFFSET :offset;


-- :name query-document-size :? :1
SELECT
  COUNT(1)
FROM
  document_header h
JOIN
  document d
  ON h.id = d.id
WHERE
  h.type = :v:type
  AND (h.context = :context OR h.context IS NULL)
  AND h.id IN (:snip:query_filter)
