COPY public.measurands (measurands_id, measurand, units, display, description, is_core, max_color_value) FROM stdin;
11	bc	µg/m³	BC	Black Carbon mass concentration	t	3
21	co2	ppm	CO₂	Carbon Dioxide concentration	f	\N
8	co	ppm	CO	Carbon Monoxide concentration	t	11
28	ch4	ppm	CH₄	Methane concentration	f	\N
7	no2	ppm	NO₂	Nitrogen Dioxide concentration	t	0.65
35	no	ppm	NO	Nitrogen Monoxide concentration	f	\N
27	nox	µg/m³	NOx mass	Nitrogen Oxides mass concentration	f	\N
10	o3	ppm	O₃	Ozone concentration	t	0.165
19	pm1	µg/m³	PM1	Particulate matter less than 1 micrometer in diameter mass concentration	f	\N
1	pm10	µg/m³	PM10	Particulate matter less than 10 micrometers in diameter mass concentration	t	275
2	pm25	µg/m³	PM2.5	Particulate matter less than 2.5 micrometers in diameter mass concentration	t	110
9	so2	ppm	SO₂	Sulfur Dioxide concentration	t	0.22
37	ambient_temp	deg_c	\N	\N	\N	\N
17	bc	ng/m3	\N	\N	\N	\N
102	co	ppb	\N	\N	\N	\N
14	co2	umol/mol	CO2	\N	\N	\N
134	humidity	%	\N	\N	\N	\N
24	no	ppb	\N	\N	\N	\N
15	no2	ppb	\N	\N	\N	\N
23	nox	ppb	NOX	\N	\N	\N
32	o3	ppb	\N	\N	\N	\N
676	ozone	ppb	\N	\N	\N	\N
36	pm	µg/m³	PM	\N	\N	\N
131	pm100	µg/m³	PM100	\N	\N	\N
97	pm25	μg/m³	\N	\N	\N	\N
95	pressure	hpa	\N	\N	\N	\N
132	pressure	mb	\N	\N	\N	\N
98	relativehumidity	%	\N	\N	\N	\N
25	rh	%	\N	\N	\N	\N
101	so2	ppb	\N	\N	\N	\N
100	temperature	c	\N	\N	\N	\N
128	temperature	f	\N	\N	\N	\N
22	wind_direction	deg	\N	\N	\N	\N
34	wind_speed	m/s	\N	\N	\N	\N
19840	nox	ppm	NOx	Nitrogen Oxides concentration	f	\N
150	voc	iaq	\N	\N	\N	\N
19841	bc	ppm	\N	\N	\N	\N
33	ufp	particles/cm³	UFP count	Ultrafine Particles count concentration	f	\N
29	pn	particles/cm³	\N	\N	\N	\N
126	um010	particles/cm³	PM1 count	PM1 count	f	\N
130	um025	particles/cm³	PM2.5 count	PM2.5 count	f	\N
135	um100	particles/cm³	PM10 count	PM10 count	f	\N
125	um003	particles/cm³	\N	\N	\N	\N
129	um050	particles/cm³	\N	\N	\N	\N
133	um005	particles/cm³	\N	\N	\N	\N
4	co	µg/m³	CO mass	Carbon Monoxide mass concentration	f	12163.042264360405
5	no2	µg/m³	NO₂ mass	Nitrogen Dioxide mass concentration	f	1180.7619365949006
6	so2	µg/m³	SO₂ mass	Sulfur Dioxide mass concentration	f	556.0245257363534
3	o3	µg/m³	O₃ mass	Ozone mass concentration	f	312.7641909643373
19843	no	µg/m³	NO mass	Nitrogen Monoxide mass concentration	f	\N
\.

-- need to fix the sequence now
SELECT setval(pg_get_serial_sequence('measurands', 'measurands_id'), (SELECT max(measurands_id) FROM measurands));


-- now lets add some new ones and

INSERT INTO measurands (measurand, units, display, description, is_core) VALUES
( 'wind_speed'
, 'm/s'
, 'ws'
, 'Average wind speed in meters per second'
, true)
, ( 'pressure'
, 'hpa'
, 'atm'
, 'Atmospheric or barometric pressure'
, true)
, ( 'wind_direction'
, 'deg'
, 'wd'
, 'Direction that the wind originates from'
, true)
, ( 'so4'
, 'ppb'
, 'SO4'
, 'Sulfate'
, true)
, ( 'ec'
, 'ppb'
, 'EC'
, 'Elemental Carbon'
, true)
, ( 'oc'
, 'ppb'
, 'OC'
, 'Organic Carbon'
, true)
, ( 'cl'
, 'ppb'
, 'Cl'
, 'Chloride'
, true)
, ( 'k'
, 'ppb'
, 'K'
, 'Potassium'
, true)
, ( 'no3'
, 'ppb'
, 'NO3'
, 'Nitrite'
, true)
, ( 'pb'
, 'ppb'
, 'Pb'
, 'Lead'
, true)
, ( 'as'
, 'ppb'
, 'As'
, 'Arsenic'
, true)
, ( 'ca'
, 'ppb'
, 'Ca'
, 'Calcium'
, true)
, ( 'fe'
, 'ppb'
, 'Fe'
, 'Iron'
, true)
, ( 'ni'
, 'ppb'
, 'Ni'
, 'Nickle'
, true)
, ( 'v'
, 'ppb'
, 'V'
, 'Vanadium'
, true)
ON CONFLICT (measurand, units) DO UPDATE
SET description = EXCLUDED.description
, is_core = EXCLUDED.is_core
, display = EXCLUDED.display
;
