-- Zero was previously used as the NoConfidence clear marker. Clear markers
-- are now NULL so an enacted zero UnitInterval quorum remains distinguishable.
UPDATE committee_quorum SET quorum = NULL WHERE quorum = '0';
