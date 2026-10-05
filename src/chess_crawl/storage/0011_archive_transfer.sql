-- External archive transfers recheck and repoint current import references.
-- raw_payloads already has the corresponding archive_object_id index.
CREATE INDEX ix_archive_import_object ON archive_imports(archive_object_id);
