  -- Data
  SET search_path = fetcher, public;

  INSERT INTO handlers (handlers_id, label, description, queue_name)
  OVERRIDING SYSTEM VALUE
  VALUES
  (1, 'default', 'Fetch handler to use a default for deployments', 'default-fetcher');

  INSERT INTO adapter_clients (name, handler, description, authorization_method) VALUES
    ('clarity', NULL, 'Transform client written for the Clarity API', 'API Key')
  , ('air4thai', NULL, 'Custom transform client written for the air4thai API', NULL)
  , ('bam', NULL, 'Custom transform client written for BAM data', NULL)
  , ('data354', NULL, 'Custom transform client written for the data354 API', NULL)
  , ('eea', NULL, 'Custom transform client written for EEA data', NULL)
  , ('habitatmap', NULL, 'Custom transform client written for the HabitatMap API', NULL)
  , ('senstate', NULL, 'Custom transform client written for the Senstate API', 'API Key')
  , ('airqo', NULL, 'Custom transform client written for the Senstate API', NULL)
  , ('openaq', NULL, 'Custom transform client written for the OpenAQ API', 'API Key')
  , ('airnow', NULL, 'Custom transform client written for the OpenAQ API', NULL)
  , ('london', NULL, 'Custom transform client written for the OpenAQ API', NULL)
  , ('japan', NULL, 'Custom transform client written for the OpenAQ API', NULL)
  , ('mexico', NULL, 'Custom transform client written for the OpenAQ API', NULL)
  , ('hanoi', NULL, 'Custom transform client written for the OpenAQ API', NULL)
  ON CONFLICT (name) DO UPDATE
  SET description = EXCLUDED.description
  , authorization_method = EXCLUDED.authorization_method;


  -- now create an adapter with config for each of these
  INSERT INTO adapters (adapter_clients_id, providers_id, config)
  SELECT a.adapter_clients_id, p.providers_id, '{}'
  FROM adapter_clients a
  JOIN public.providers p ON (lower(p.source_name) = lower(a.name))
  ON CONFLICT DO NOTHING;

  INSERT INTO adapters (adapter_clients_id, providers_id, config)
  SELECT ac.adapter_clients_id
  , p.providers_id
  , '{}'
  FROM (VALUES
      ('hanoi', 'stateair_hanoi')
    , ('mexico', 'sinaica mexico')
    , ('airnow', 'airnow')
    , ('london', 'london air quality network')
    , ('japan', 'japan-soramame')
  ) as v (adapter_clients_name, source_name)
  JOIN adapter_clients ac ON (v.adapter_clients_name = ac.name)
  JOIN providers p ON (v.source_name = lower(p.source_name));



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
  ON CONFLICT DO NOTHING;
