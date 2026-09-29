-- Logical public schema; excludes extension-owned objects and this tool's ledger.
-- No application rows, sequence positions, chunk counts, OIDs, owners or timestamps.
WITH relations AS (
  SELECT c.* FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='public' AND c.relname<>'flare_schema_migrations'
    AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid='pg_class'::regclass
      AND d.objid=c.oid AND d.deptype='e')
), items AS (
  SELECT jsonb_build_object('kind','relation','name',c.relname,'type',c.relkind,
    'persistence',c.relpersistence,'row_security',c.relrowsecurity,
    'force_row_security',c.relforcerowsecurity,'replica_identity',c.relreplident,
    'options',c.reloptions,'partition',pg_get_partkeydef(c.oid)) AS item
  FROM relations c WHERE c.relkind IN ('r','p','v','m','f')
  UNION ALL
  SELECT jsonb_build_object('kind','column','table',c.relname,'name',a.attname,
    'type',format_type(a.atttypid,a.atttypmod),'not_null',a.attnotnull,
    'default',pg_get_expr(d.adbin,d.adrelid),'identity',a.attidentity,
    'generated',a.attgenerated,'collation',coll.collname)
  FROM relations c JOIN pg_attribute a ON a.attrelid=c.oid
    LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
    LEFT JOIN pg_collation coll ON coll.oid=a.attcollation
  WHERE a.attnum>0 AND NOT a.attisdropped AND c.relkind IN ('r','p','v','m','f')
  UNION ALL
  SELECT jsonb_build_object('kind','constraint','table',c.relname,'name',k.conname,
    'type',k.contype,'definition',pg_get_constraintdef(k.oid,false),
    'validated',k.convalidated,'deferrable',k.condeferrable,'deferred',k.condeferred)
  FROM relations c JOIN pg_constraint k ON k.conrelid=c.oid
  UNION ALL
  SELECT jsonb_build_object('kind','index','table',c.relname,'name',ic.relname,
    'definition',pg_get_indexdef(i.indexrelid),'valid',i.indisvalid,
    'ready',i.indisready,'replica_identity',i.indisreplident,'clustered',i.indisclustered)
  FROM relations c JOIN pg_index i ON i.indrelid=c.oid JOIN pg_class ic ON ic.oid=i.indexrelid
  UNION ALL
  SELECT jsonb_build_object('kind','trigger','table',c.relname,'name',t.tgname,
    'definition',pg_get_triggerdef(t.oid,false),'enabled',t.tgenabled)
  FROM relations c JOIN pg_trigger t ON t.tgrelid=c.oid WHERE NOT t.tgisinternal
  UNION ALL
  SELECT jsonb_build_object('kind','sequence','name',c.relname,'type',format_type(s.seqtypid,NULL),
    'start',s.seqstart,'increment',s.seqincrement,'min',s.seqmin,'max',s.seqmax,
    'cache',s.seqcache,'cycle',s.seqcycle,
    'owned_by',(SELECT r.relname||'.'||a.attname FROM pg_depend d JOIN pg_class r ON r.oid=d.refobjid
      JOIN pg_attribute a ON a.attrelid=r.oid AND a.attnum=d.refobjsubid
      WHERE d.classid='pg_class'::regclass AND d.objid=c.oid AND d.deptype IN ('a','i') LIMIT 1))
  FROM relations c JOIN pg_sequence s ON s.seqrelid=c.oid
  UNION ALL
  SELECT jsonb_build_object('kind','view','name',c.relname,'definition',pg_get_viewdef(c.oid,false))
  FROM relations c WHERE c.relkind IN ('v','m')
  UNION ALL
  SELECT jsonb_build_object('kind','policy','table',c.relname,'name',p.polname,'command',p.polcmd,
    'permissive',p.polpermissive,'roles',(SELECT jsonb_agg(COALESCE(r.rolname,'public') ORDER BY r.rolname)
      FROM unnest(p.polroles) AS role_oid LEFT JOIN pg_roles r ON r.oid=role_oid),
    'using',pg_get_expr(p.polqual,p.polrelid),'check',pg_get_expr(p.polwithcheck,p.polrelid))
  FROM relations c JOIN pg_policy p ON p.polrelid=c.oid
  UNION ALL
  SELECT jsonb_build_object('kind','function','name',p.proname,'identity',pg_get_function_identity_arguments(p.oid),
    'definition',pg_get_functiondef(p.oid))
  FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
  WHERE n.nspname='public' AND p.prokind IN ('f','p')
    AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.deptype='e')
  UNION ALL
  SELECT jsonb_build_object('kind','type','name',t.typname,'type',t.typtype,
    'base',format_type(t.typbasetype,t.typtypmod),'not_null',t.typnotnull,'default',t.typdefault,
    'enum',(SELECT jsonb_agg(e.enumlabel ORDER BY e.enumsortorder) FROM pg_enum e WHERE e.enumtypid=t.oid))
  FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
  WHERE n.nspname='public' AND t.typtype IN ('d','e','r','m')
    AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid='pg_type'::regclass AND d.objid=t.oid AND d.deptype='e')
)
SELECT COALESCE(jsonb_agg(item ORDER BY item::text),'[]'::jsonb) FROM items;
