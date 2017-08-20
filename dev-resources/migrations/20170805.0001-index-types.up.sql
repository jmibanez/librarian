CREATE TABLE type_index_path (
    id serial,
    type uuid NOT NULL,
    path character varying(200) NOT NULL,

    PRIMARY KEY (id),
    UNIQUE (type, path),
    FOREIGN KEY (type) REFERENCES document_header(id)
);

CREATE TABLE document_index_value (
    id serial,
    path_id bigint NOT NULL,
    value jsonb,

    PRIMARY KEY (id),
    UNIQUE (path_id, value),
    FOREIGN KEY (path_id) REFERENCES type_index_path(id)
);

CREATE INDEX idx_document_index_value_path_value
ON document_index_value
USING btree (path_id, value);

CREATE TABLE document_index (
    id serial,
    document_id uuid NOT NULL,
    version character varying(64) NOT NULL,
    value_id bigint NOT NULL,

    PRIMARY KEY (id),
    UNIQUE (document_id, version, value_id),
    FOREIGN KEY (document_id, version) REFERENCES document(id, version),
    FOREIGN KEY (value_id) REFERENCES document_index_value(id)
);

CREATE VIEW document_index_view AS
SELECT
    idx.document_id, idx.version,
    p.path, idx_val.value
FROM
    document_index idx
JOIN
    document_index_value idx_val
    ON idx.value_id = idx_val.id
RIGHT JOIN
    type_index_path p
    ON idx_val.path_id = p.id
JOIN
    document d
    ON idx.document_id = d.id
       AND idx.version = d.version
JOIN
    document_header h
    ON d.id = h.id
       AND p.type = h.type;
