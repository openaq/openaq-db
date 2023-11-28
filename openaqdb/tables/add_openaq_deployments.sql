INSERT INTO adapters (adapters_id, name, handler, description) VALUES
(5, 'versioning', 'openaq-lcs-fetcher', 'Config based adapter in LCS fetcher. Built specifically for CAC project');


INSERT INTO providers (
  providers_id
  , label
  , description
  , source_name
  , export_prefix
  , license
  , is_active
  , adapters_id
  , metadata) VALUES (
  5
  , 'CAC project'
  , 'Source of data for the CAC project'
  , 'cac'
  , 'cac-project'
  , 'More to come'
  , 't'
  , 5
  , '{
  "name": "cac",
  "schema": "v1",
  "provider": "versioning",
  "frequency": "minute",
  "type": "google-bucket",
  "config" : {
    "bucket":"openaq-staging",
    "folder":"pending"
  },
  "parameters": {
    "co": ["co", "ppb"],
    "no": ["no", "ppb"],
    "no2": ["no2", "ppb"],
    "o3": ["o3", "ppb"],
    "p": ["pressure", "hpa"],
    "pm025": ["pm25", "ppb"],
    "pm10": ["pm10", "ppb"],
    "rh": ["relativehumidity", "%"],
    "so2": ["so2", "ppb"],
    "temp": ["temperature", "c"],
    "ws": ["wind_speed", "m/s"],
    "wd": ["wind_direction", "deg"],
    "bc": ["bc","ppb"],
    "bc_375": ["bc_375","ppb"],
    "bc_528": ["bc_528","ppb"],
    "bc_625": ["bc_625","ppb"],
    "bc_880": ["bc_880","ppb"],
    "ec": ["ec","ppb"],
    "oc": ["oc","ppb"],
    "so4": ["so4","ppb"],
    "cl": ["cl","ppb"],
    "k": ["k","ppb"],
    "no3": ["no3","ppb"],
    "pb": ["pb","ppb"],
    "as": ["as","ppb"],
    "ca": ["ca","ppb"],
    "fe": ["fe","ppb"],
    "ni": ["ni","ppb"],
    "v": ["v","ppb"]
  }
}');




INSERT INTO deployments (
         deployments_id
       , name
       , temporal_offset
       , providers_id
       , adapters_id
       ) VALUES
  (1, 'cac', NULL, 5, NULL)
;
