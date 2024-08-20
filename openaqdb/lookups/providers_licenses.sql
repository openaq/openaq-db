COPY public.providers_licenses (providers_licenses_id, licenses_id, providers_id, active_period, url, notes, metadata) FROM stdin;
95	30	156	[2017-07-13,)	https://epa.tas.gov.au/Air/Live/EPA_tas_latest_particle_data.txt	\N	\N
96	32	162	[2017-08-10,)	https://buenosaires.gob.ar/terminos-y-condicione	\N	\N
97	30	20	[2019-12-23,)	https://data.sa.gov.au/copyright	\N	\N
98	30	52	[2023-03-19,)	https://www.data.gov.cy/dataset/τρέχουσες-μετρήσεις-ατμοσφαιρικών-ρύπων-api	\N	\N
99	33	119	[2016-01-30,)	https://catalog.data.gov/dataset/airnow-real-time-air-quality-rest-api	\N	\N
100	30	170	[2017-09-14,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
101	30	171	[2016-11-18,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
102	30	172	[2016-11-17,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
103	30	173	[2020-04-20,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
104	30	174	[2016-11-17,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
105	30	175	[2020-04-20,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
106	30	176	[2020-04-20,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
107	30	177	[2016-11-21,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
108	30	178	[2020-04-20,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
109	30	179	[2016-12-10,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
110	30	180	[2016-11-21,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
111	30	181	[2016-11-21,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
112	30	182	[2016-12-10,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
113	30	183	[2020-04-20,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
114	30	184	[2016-12-12,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
115	30	185	[2016-12-10,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
116	30	186	[2017-11-01,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
117	30	187	[2017-09-21,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
118	30	188	[2017-09-22,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
119	30	189	[2016-11-21,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
120	30	190	[2016-12-10,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
121	30	191	[2016-11-21,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
122	30	192	[2017-09-21,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
123	30	193	[2020-04-19,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
124	30	194	[2017-09-22,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
125	30	195	[2017-09-21,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
126	30	196	[2016-11-17,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
127	30	197	[2017-02-08,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
128	30	198	[2017-09-21,)	https://www.eea.europa.eu/en/legal-notice	\N	\N
129	34	169	[2016-01-30,)	https://uk-air.defra.gov.uk/about-these-pages	\N	\N
130	35	199	[2018-11-21,)	https://powietrze.gios.gov.pl/pjp/home	\N	\N
131	36	223	[2016-03-07,)	https://datos.gob.mx/	\N	\N
132	30	154	[2017-07-18,)	https://www.data.qld.gov.au/dataset/air-quality-monitoring-live-data-feed/resource/50410fbe-8766-45cd-a259-c4c1f3c89dd1	\N	\N
133	37	152	[2019-12-23,)	https://www.health.act.gov.au/copyright	\N	\N
134	38	16	[2016-11-18,)	https://web.gencat.cat/ca/ajuda/avis_legal/	\N	\N
135	39	63	[2023-07-14,)	https://www.env.go.jp/mail.html	\N	\N
137	41	17	[2023-03-31,)	https://data.ecan.govt.nz/Catalogue/Search?Query=air&CollectionId=0	\N	\N
138	41	224	[2017-08-22,)	https://podatki.gov.si/pogoji-uporabe	\N	\N
\.

-- updating those dates above
UPDATE providers_licenses
SET active_period = '[-infinity, infinity)';
