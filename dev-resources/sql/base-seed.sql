-- :name seed-test-document-headers! :!
INSERT INTO document_header
(id, context, name, type, current_version, date_created)
VALUES
('34a79262-f324-4911-ac33-8bb62f020d09'::uuid, '008dc16e-f7d9-45ce-a043-e482919561c1'::uuid,
 'Foo', '143246e3-c0b7-59e3-ae17-29b99ff0d5ac'::uuid,
 'd22b30f48fe55d0633be7761aadb7b5e631830644dc9efaa3b74fd55d943c34e', NOW()),
('e649ce0d-801d-414c-bdc1-9605ff7090d1'::uuid, '008dc16e-f7d9-45ce-a043-e482919561c1'::uuid,
 'Foo', '13068b05-bc96-4ed6-9a5f-d49936da40da'::uuid,
 'ef0bb1322bf43f4ab9832c05a7aee34754d45c8cee3e7a707e0ee330765b2bea', NOW()),
('60053ded-32e9-48ee-b6b1-d546b3c071b3'::uuid, '008dc16e-f7d9-45ce-a043-e482919561c1'::uuid,
 'TestDoc', '143246e3-c0b7-59e3-ae17-29b99ff0d5ac'::uuid,
 '8b9dbb0333d0f51a917840e616720e21839e8b9ba1fc3ffcf1bbcb55c19e5dad', NOW()),
('1ececa2b-0bae-478f-80f4-a291deaf2186'::uuid, '008dc16e-f7d9-45ce-a043-e482919561c1'::uuid,
 'TestDocWithRef', '143246e3-c0b7-59e3-ae17-29b99ff0d5ac'::uuid,
 'ea8a6ca94985aeaeeb09f38bf63e540c1a93e7d32e4180dac8c9ec9663ab23f0', NOW());

-- :name seed-test-documents! :!
INSERT INTO document
(id, document, version,
 state, date_modified)
VALUES
('34a79262-f324-4911-ac33-8bb62f020d09'::uuid,
 '{"name":"::string"}',
 'd22b30f48fe55d0633be7761aadb7b5e631830644dc9efaa3b74fd55d943c34e',
 'posted', NOW()),
('e649ce0d-801d-414c-bdc1-9605ff7090d1'::uuid,
 '{"name":"other"}',
 'ef0bb1322bf43f4ab9832c05a7aee34754d45c8cee3e7a707e0ee330765b2bea',
 'posted', NOW()),
('60053ded-32e9-48ee-b6b1-d546b3c071b3'::uuid,
 '{"definition": {"id": "::string", "name": "::string", "inner": [{"name": "::string", "value": "::integer"}]}}',
 '8b9dbb0333d0f51a917840e616720e21839e8b9ba1fc3ffcf1bbcb55c19e5dad',
 'posted', NOW()),
('1ececa2b-0bae-478f-80f4-a291deaf2186'::uuid,
 '{"definition": {"id": "::string", "name": "::string", "referred": "TestDoc"}}',
 'ea8a6ca94985aeaeeb09f38bf63e540c1a93e7d32e4180dac8c9ec9663ab23f0',
 'posted', NOW());

-- :name clear-test-documents! :!
DELETE FROM document
WHERE id IN (
  '34a79262-f324-4911-ac33-8bb62f020d09'::uuid,
  '60053ded-32e9-48ee-b6b1-d546b3c071b3'::uuid
);

-- :name clear-test-document-headers! :!
DELETE FROM document_header
WHERE id IN (
  '34a79262-f324-4911-ac33-8bb62f020d09'::uuid,
  '60053ded-32e9-48ee-b6b1-d546b3c071b3'::uuid
);
