COPY public.measurands (measurands_id, measurand, units, display, description) FROM stdin;
11	bc	µg/m³	BC	Black Carbon mass concentration
21	co2	ppm	CO₂	Carbon Dioxide concentration
8	co	ppm	CO	Carbon Monoxide concentration
28	ch4	ppm	CH₄	Methane concentration
7	no2	ppm	NO₂	Nitrogen Dioxide concentration
35	no	ppm	NO	Nitrogen Monoxide concentration
27	nox	µg/m³	NOx mass	Nitrogen Oxides mass concentration
10	o3	ppm	O₃	Ozone concentration
19	pm1	µg/m³	PM1	Particulate matter less than 1 micrometer in diameter mass concentration
1	pm10	µg/m³	PM10	Particulate matter less than 10 micrometers in diameter mass concentration
2	pm25	µg/m³	PM2.5	Particulate matter less than 2.5 micrometers in diameter mass concentration
9	so2	ppm	SO₂	Sulfur Dioxide concentration
37	ambient_temp	deg_c	\N	\N
17	bc	ng/m3	\N	\N
102	co	ppb	\N	\N
14	co2	umol/mol	CO2	\N
134	humidity	%	\N	\N
24	no	ppb	\N	\N
15	no2	ppb	\N	\N
23	nox	ppb	NOX	\N
32	o3	ppb	\N	\N
676	ozone	ppb	\N	\N
36	pm	µg/m³	PM	\N
131	pm100	µg/m³	PM100	\N
95	pressure	hpa	\N	\N
132	pressure	mb	\N	\N
98	relativehumidity	%	\N	\N
25	rh	%	\N	\N
101	so2	ppb	\N	\N
100	temperature	c	\N	\N
128	temperature	f	\N	\N
22	wind_direction	deg	\N	\N
34	wind_speed	m/s	\N	\N
19840	nox	ppm	NOx	Nitrogen Oxides concentration
150	voc	iaq	\N	\N
19841	bc	ppm	\N	\N
33	ufp	particles/cm³	UFP count	Ultrafine Particles count concentration
29	pn	particles/cm³	\N	\N
126	um010	particles/cm³	PM1 count	PM1 count
130	um025	particles/cm³	PM2.5 count	PM2.5 count
135	um100	particles/cm³	PM10 count	PM10 count
125	um003	particles/cm³	\N	\N
129	um050	particles/cm³	\N	\N
133	um005	particles/cm³	\N	\N
4	co	µg/m³	CO mass	Carbon Monoxide mass concentration
5	no2	µg/m³	NO₂ mass	Nitrogen Dioxide mass concentration
6	so2	µg/m³	SO₂ mass	Sulfur Dioxide mass concentration
3	o3	µg/m³	O₃ mass	Ozone mass concentration
19843	no	µg/m³	NO mass	Nitrogen Monoxide mass concentration
\.

INSERT INTO measurands (measurands_id, measurand, units, description)
OVERRIDING SYSTEM VALUE VALUES
(19844, 'pm4', 'µg/m³', 'Particulate matter less than 4 micrometers in diameter mass concentration')
ON CONFLICT DO NOTHING;

-- need to fix the sequence now
SELECT setval(pg_get_serial_sequence('measurands', 'measurands_id'), (SELECT max(measurands_id) FROM measurands));

-- now lets add some new ones
INSERT INTO measurands (measurand, units, display, description) VALUES
( 'wind_speed'
, 'm/s'
, 'ws'
, 'Average wind speed in meters per second')
, ( 'pressure'
, 'hpa'
, 'atm'
, 'Atmospheric or barometric pressure')
, ( 'wind_direction'
, 'deg'
, 'wd'
, 'Direction that the wind originates from')
, ( 'bc_880'
, 'ug/m2'
, 'BC @ 880 nm'
, 'Estimate of black carbon (BC)')
, ( 'bc_625'
, 'ug/m2'
, 'BC @ 625 nm'
, '')
, ( 'bc_528'
, 'ug/m2'
, 'BC @ 528 nm'
, '')
, ( 'bc_470'
, 'ug/m2'
, 'BC @ 470 nm'
, '')
, ( 'bc_375'
, 'ug/m2'
, 'BC @ 375 nm'
, 'Estimate of ultraviolet particulate matter (UVPM)')
, ( 'so4'
, 'ppb'
, 'SO4'
, 'Sulfate')
, ( 'ec'
, 'ppb'
, 'EC'
, 'Elemental Carbon')
, ( 'oc'
, 'ppb'
, 'OC'
, 'Organic Carbon')
, ( 'cl'
, 'ppb'
, 'Cl'
, 'Chloride')
, ( 'k'
, 'ppb'
, 'K'
, 'Potassium')
, ( 'no3'
, 'ppb'
, 'NO3'
, 'Nitrite')
, ( 'pb'
, 'ppb'
, 'Pb'
, 'Lead')
, ( 'as'
, 'ppb'
, 'As'
, 'Arsenic')
, ( 'ca'
, 'ppb'
, 'Ca'
, 'Calcium')
, ( 'fe'
, 'ppb'
, 'Fe'
, 'Iron')
, ( 'ni'
, 'ppb'
, 'Ni'
, 'Nickle')
, ( 'v'
, 'ppb'
, 'V'
, 'Vanadium')
ON CONFLICT (measurand, units) DO UPDATE
SET description = EXCLUDED.description
, display = EXCLUDED.display
;


UPDATE measurands
SET parameter_type = 'meteorological'
WHERE measurand ~* 'temp|rh|relative|pressure|humid|wind';
