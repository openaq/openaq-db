  -- Data
  SET search_path = fetcher, public;

  INSERT INTO handlers (handlers_id, label, description, queue_name)
  OVERRIDING SYSTEM VALUE
  VALUES
  (1, 'default', 'Fetch handler to use a default for deployments', 'default-fetcher');

  INSERT INTO fetcher_clients (name, description, authorization_method) VALUES
    ('clarity', 'Transform client written for the Clarity API', 'API Key')
  , ('air4thai', 'Custom transform client written for the air4thai API', NULL)
  , ('bam', 'Custom transform client written for BAM data', NULL)
  , ('data354', 'Custom transform client written for the data354 API', NULL)
  , ('eea', 'Custom transform client written for EEA data', NULL)
  , ('habitatmap', 'Custom transform client written for the HabitatMap API', NULL)
  , ('senstate', 'Custom transform client written for the Senstate API', 'API Key')
  , ('airqo', 'Custom transform client written for the Senstate API', NULL)
  , ('openaq', 'Custom transform client written for the OpenAQ API', 'API Key')
  , ('airnow', 'Custom transform client written for the OpenAQ API', NULL)
  , ('london', 'Custom transform client written for the OpenAQ API', NULL)
  , ('japan', 'Custom transform client written for the OpenAQ API', NULL)
  , ('mexico', 'Custom transform client written for the OpenAQ API', NULL)
  , ('hanoi', 'Custom transform client written for the OpenAQ API', NULL)
  ON CONFLICT (name) DO UPDATE
  SET description = EXCLUDED.description
  , authorization_method = EXCLUDED.authorization_method;


  -- now create an adapter with config for each of these
  INSERT INTO adapters (fetcher_clients_id, providers_id, config)
  SELECT a.fetcher_clients_id, p.providers_id, '{}'
  FROM fetcher_clients a
  JOIN public.providers p ON (lower(p.source_name) = lower(a.name))
  ON CONFLICT DO NOTHING;


  INSERT INTO adapters (fetcher_clients_id, providers_id, config)
  SELECT ac.fetcher_clients_id
  , p.providers_id
  , '{}'
  FROM (VALUES
      ('hanoi', 'stateair_hanoi')
    , ('mexico', 'sinaica mexico')
    , ('airnow', 'airnow')
    , ('london', 'london air quality network')
    , ('japan', 'japan-soramame')
  ) as v (fetcher_clients_name, source_name)
  JOIN fetcher_clients ac ON (v.fetcher_clients_name = ac.name)
  JOIN providers p ON (v.source_name = lower(p.source_name))
  ON CONFLICT DO NOTHING;



  -- create the deployments we currently have
INSERT INTO deployments (deployments_id, label, description, temporal_offset, filename_prefix, schedule)
OVERRIDING SYSTEM VALUE
VALUES
    (1, 'realtime', 'most government data', 0, 'realtime', '*/15 * * * *')
  , (2, 'airnow cleanup', 'a deployment that rechecks for data from yesterday', 24, 'airnow', '0 * * * *')
  , (3, 'london', 'breath london fetcher', 0, 'london', '*/15 * * * *')
  , (4, 'acumar', '', 75, 'acumar', '*/15 * * * *')
  , (5, 'japan', '', 0, 'japan', '*/15 * * * *')
  , (6, 'mexico', '', 0, 'mexico', '*/15 * * * *')
  , (7, 'hanoi', '', 12, 'hanoi', '*/15 * * * *')
  , (8, 'clarity', '', 0, 'clarity', '0 * * * *')
  , (9, 'senstate', '', 0, 'senstate', '*/5 * * * *')
  , (10, 'testing-1min', 'this is one that should always fire unless its already been deployed for the current time', 0, 'senstate', '* * * * *')
 ON CONFLICT DO NOTHING;


  -- realtime should run them all
  INSERT INTO deployment_adapters (deployments_id, adapters_id)
  SELECT deployments_id, adapters_id
  FROM deployments d, adapters
  WHERE d.label ~* 'realtime'
  ON CONFLICT DO NOTHING;

  -- the rest should be one offs
  INSERT INTO deployment_adapters (deployments_id, adapters_id)
  SELECT d.deployments_id
  , adapters_id
  FROM adapters a
  JOIN fetcher_clients c USING (fetcher_clients_id)
  JOIN deployments d ON (c.name = d.label)
  ON CONFLICT DO NOTHING;

  -- airnow cleanup
  INSERT INTO deployment_adapters (deployments_id, adapters_id)
  SELECT deployments_id, adapters_id
  FROM deployments d, adapters
  JOIN fetcher_clients c USING (fetcher_clients_id)
  WHERE d.label ~* 'cleanup'
  AND c.name = 'airnow'
  ON CONFLICT DO NOTHING
  ;


  INSERT INTO deployment_adapters (deployments_id, adapters_id)
  SELECT deployments_id, adapters_id
  FROM deployments d, adapters
  JOIN fetcher_clients c USING (fetcher_clients_id)
  WHERE d.label ~* 'testing'
  AND c.name = 'senstate'
  ON CONFLICT DO NOTHING
  ;


SELECT setval(
  pg_get_serial_sequence('fetcher.handlers', 'handlers_id'),
  COALESCE((SELECT MAX(handlers_id) FROM fetcher.handlers), 1)
);

SELECT setval(
  pg_get_serial_sequence('fetcher.adapters', 'adapters_id'),
  COALESCE((SELECT MAX(adapters_id) FROM fetcher.adapters), 1)
);

SELECT setval(
  pg_get_serial_sequence('fetcher.deployments', 'deployments_id'),
  COALESCE((SELECT MAX(deployments_id) FROM fetcher.deployments), 1)
);

-- fetcher_clients uses an explicit sequence, not identity:
SELECT setval(
  'fetcher.fetcher_clients_sq',
  COALESCE((SELECT MAX(fetcher_clients_id) FROM fetcher.fetcher_clients), 1)
);

SELECT setval(
  'providers_sq',
  COALESCE((SELECT MAX(providers_id) FROM providers), 1)
);

SELECT setval(
  'instruments_sq',
  COALESCE((SELECT MAX(instruments_id) FROM instruments), 1)
);

SELECT setval(
  'entities_sq',
  COALESCE((SELECT MAX(entities_id) FROM entities), 1)
);

--SELECT * FROM fetcher.get_ready_deployments('2026-01-26 12:45:00');

--SELECT * FROM fetcher.queue_deployments('2026-01-26 11:45:00');
--SELECT * FROM fetcher.get_and_mark_queued_jobs();
