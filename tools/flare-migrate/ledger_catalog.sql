SELECT jsonb_build_object(
  'kind',c.relkind,'persistence',c.relpersistence,'row_security',c.relrowsecurity,'force_row_security',c.relforcerowsecurity,
  'columns',(SELECT jsonb_agg(jsonb_build_object('name',a.attname,'type',format_type(a.atttypid,a.atttypmod),
    'not_null',a.attnotnull,'default',pg_get_expr(d.adbin,d.adrelid),'identity',a.attidentity,'generated',a.attgenerated) ORDER BY a.attname)
    FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
    WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped),
  'constraints',(SELECT jsonb_agg(jsonb_build_object('name',k.conname,'type',k.contype,'definition',pg_get_constraintdef(k.oid,false),
    'validated',k.convalidated,'deferrable',k.condeferrable,'deferred',k.condeferred) ORDER BY k.conname)
    FROM pg_constraint k WHERE k.conrelid=c.oid),
  'indexes',(SELECT jsonb_agg(jsonb_build_object('definition',pg_get_indexdef(i.indexrelid),
    'valid',i.indisvalid,'ready',i.indisready,'unique',i.indisunique,'primary',i.indisprimary) ORDER BY i.indexrelid::regclass::text)
    FROM pg_index i WHERE i.indrelid=c.oid),
  'triggers',(SELECT count(*) FROM pg_trigger t WHERE t.tgrelid=c.oid AND NOT t.tgisinternal),
  'rules',(SELECT count(*) FROM pg_rewrite r WHERE r.ev_class=c.oid)
) FROM pg_class c WHERE c.oid='public.flare_schema_migrations'::regclass;
