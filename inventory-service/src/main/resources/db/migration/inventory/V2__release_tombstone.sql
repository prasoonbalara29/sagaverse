-- A ReleaseStock that arrives when nothing was reserved writes a tombstone: status RELEASED,
-- quantity 0, and no sku (ReleaseStock doesn't carry one). A late ReserveStock for that saga is then
-- refused (ADR-0008).
ALTER TABLE reservation ALTER COLUMN sku DROP NOT NULL;
