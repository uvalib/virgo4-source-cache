ALTER TABLE source_cache SET (autovacuum_vacuum_scale_factor = 0.0);
ALTER TABLE source_cache SET (autovacuum_vacuum_threshold = 10000);
ALTER TABLE source_cache SET (autovacuum_analyze_scale_factor = 0.0);
ALTER TABLE source_cache SET (autovacuum_analyze_threshold = 10000);  
