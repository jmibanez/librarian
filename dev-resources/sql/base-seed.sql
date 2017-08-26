-- :name seed-test-document-headers! :!
INSERT INTO document_header
(id, context, name, type, current_version, date_created)
VALUES
('34a79262-f324-4911-ac33-8bb62f020d09'::uuid, '008dc16e-f7d9-45ce-a043-e482919561c1'::uuid,
 'Foo', '143246e3-c0b7-59e3-ae17-29b99ff0d5ac'::uuid,
 'ea06354b4d2594116a3ab44a0033bf4b643715a2', NOW()),
('e649ce0d-801d-414c-bdc1-9605ff7090d1'::uuid, '008dc16e-f7d9-45ce-a043-e482919561c1'::uuid,
 'Foo', '13068b05-bc96-4ed6-9a5f-d49936da40da'::uuid,
 'feb03dfc47835f1e0f3998c544c441e879a2e29b49743eaf2065a9cd2d15bc39', NOW()),
('60053ded-32e9-48ee-b6b1-d546b3c071b3'::uuid, '008dc16e-f7d9-45ce-a043-e482919561c1'::uuid,
 'TestDoc', '143246e3-c0b7-59e3-ae17-29b99ff0d5ac'::uuid,
 'cff5ae93b8c74e0dcc4abf226f78c28b026610e2', NOW()),
('1ececa2b-0bae-478f-80f4-a291deaf2186'::uuid, '008dc16e-f7d9-45ce-a043-e482919561c1'::uuid,
 'TestDocWithRef', '143246e3-c0b7-59e3-ae17-29b99ff0d5ac'::uuid,
 '9c7bb7e2ec219b71f89e9106d123bc3723f33d60', NOW());

-- :name seed-test-documents! :!
INSERT INTO document
(id, document, version,
 state, date_modified)
VALUES
('34a79262-f324-4911-ac33-8bb62f020d09'::uuid,
 '{"name":"::string"}',
 'ea06354b4d2594116a3ab44a0033bf4b643715a2',
 'posted', NOW()),
('e649ce0d-801d-414c-bdc1-9605ff7090d1'::uuid,
 '{"name":"other"}',
 'feb03dfc47835f1e0f3998c544c441e879a2e29b49743eaf2065a9cd2d15bc39',
 'posted', NOW()),
('60053ded-32e9-48ee-b6b1-d546b3c071b3'::uuid,
 '{"definition": {"id": "::string", "name": "::string", "inner": [{"name": "::string", "value": "::integer"}]}}',
 'cff5ae93b8c74e0dcc4abf226f78c28b026610e2',
 'posted', NOW()),
('1ececa2b-0bae-478f-80f4-a291deaf2186'::uuid,
 '{"definition": {"id": "::string", "name": "::string", "referred": "TestDoc"}}',
 '9c7bb7e2ec219b71f89e9106d123bc3723f33d60',
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
