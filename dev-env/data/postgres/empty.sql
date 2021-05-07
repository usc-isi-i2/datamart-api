--
-- PostgreSQL database dump
--

-- Dumped from database version 11.9 (Debian 11.9-1.pgdg90+1)
-- Dumped by pg_dump version 11.9 (Debian 11.9-1.pgdg90+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: alembic_version; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.alembic_version (
    version_num character varying(32) NOT NULL
);


ALTER TABLE public.alembic_version OWNER TO postgres;

--
-- Name: coordinates; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.coordinates (
    edge_id character varying NOT NULL,
    latitude numeric NOT NULL,
    longitude numeric NOT NULL,
    "precision" character varying
);


ALTER TABLE public.coordinates OWNER TO postgres;

--
-- Name: dates; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dates (
    edge_id character varying NOT NULL,
    date_and_time timestamp without time zone NOT NULL,
    "precision" character varying,
    calendar character varying
);


ALTER TABLE public.dates OWNER TO postgres;

--
-- Name: edges; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.edges (
    id character varying NOT NULL,
    node1 character varying NOT NULL,
    label character varying NOT NULL,
    node2 character varying NOT NULL,
    data_type character varying NOT NULL
);


ALTER TABLE public.edges OWNER TO postgres;

--
-- Name: fuzzy_admin1; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.fuzzy_admin1 AS
 SELECT e_var_name.node2 AS variable_id,
    e_dataset.node1 AS dataset_qnode,
    e_admin1.node2 AS admin1_qnode
   FROM (((((public.edges e_var
     JOIN public.edges e_var_property ON ((((e_var_property.node1)::text = (e_var.node1)::text) AND ((e_var_property.label)::text = 'P1687'::text))))
     JOIN public.edges e_var_name ON ((((e_var_name.node1)::text = (e_var.node1)::text) AND ((e_var_name.label)::text = 'P1813'::text))))
     JOIN public.edges e_dataset ON ((((e_dataset.label)::text = 'P2006020003'::text) AND ((e_dataset.node2)::text = (e_var.node1)::text))))
     JOIN public.edges e_main ON (((e_var_property.node2)::text = (e_main.label)::text)))
     JOIN public.edges e_admin1 ON ((((e_admin1.node1)::text = (e_main.node1)::text) AND ((e_admin1.label)::text = 'P2006190001'::text))))
  WHERE (((e_var.label)::text = 'P31'::text) AND ((e_var.node2)::text = 'Q50701'::text))
UNION
 SELECT e_var_name.node2 AS variable_id,
    e_dataset.node1 AS dataset_qnode,
    e_admin1.node2 AS admin1_qnode
   FROM ((((((public.edges e_var
     JOIN public.edges e_var_property ON ((((e_var_property.node1)::text = (e_var.node1)::text) AND ((e_var_property.label)::text = 'P1687'::text))))
     JOIN public.edges e_var_name ON ((((e_var_name.node1)::text = (e_var.node1)::text) AND ((e_var_name.label)::text = 'P1813'::text))))
     JOIN public.edges e_dataset ON ((((e_dataset.label)::text = 'P2006020003'::text) AND ((e_dataset.node2)::text = (e_var.node1)::text))))
     JOIN public.edges e_main ON (((e_var_property.node2)::text = (e_main.label)::text)))
     JOIN public.edges e_location ON ((((e_main.node1)::text = (e_location.node1)::text) AND ((e_location.label)::text = 'P276'::text))))
     JOIN public.edges e_admin1 ON ((((e_admin1.node1)::text = (e_location.node2)::text) AND ((e_admin1.label)::text = 'P2006190001'::text))))
  WHERE (((e_var.label)::text = 'P31'::text) AND ((e_var.node2)::text = 'Q50701'::text))
  WITH NO DATA;


ALTER TABLE public.fuzzy_admin1 OWNER TO postgres;

--
-- Name: fuzzy_admin2; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.fuzzy_admin2 AS
 SELECT e_var_name.node2 AS variable_id,
    e_dataset.node1 AS dataset_qnode,
    e_admin2.node2 AS admin2_qnode
   FROM (((((public.edges e_var
     JOIN public.edges e_var_property ON ((((e_var_property.node1)::text = (e_var.node1)::text) AND ((e_var_property.label)::text = 'P1687'::text))))
     JOIN public.edges e_var_name ON ((((e_var_name.node1)::text = (e_var.node1)::text) AND ((e_var_name.label)::text = 'P1813'::text))))
     JOIN public.edges e_dataset ON ((((e_dataset.label)::text = 'P2006020003'::text) AND ((e_dataset.node2)::text = (e_var.node1)::text))))
     JOIN public.edges e_main ON (((e_var_property.node2)::text = (e_main.label)::text)))
     JOIN public.edges e_admin2 ON ((((e_admin2.node1)::text = (e_main.node1)::text) AND ((e_admin2.label)::text = 'P2006190002'::text))))
  WHERE (((e_var.label)::text = 'P31'::text) AND ((e_var.node2)::text = 'Q50701'::text))
UNION
 SELECT e_var_name.node2 AS variable_id,
    e_dataset.node1 AS dataset_qnode,
    e_admin2.node2 AS admin2_qnode
   FROM ((((((public.edges e_var
     JOIN public.edges e_var_property ON ((((e_var_property.node1)::text = (e_var.node1)::text) AND ((e_var_property.label)::text = 'P1687'::text))))
     JOIN public.edges e_var_name ON ((((e_var_name.node1)::text = (e_var.node1)::text) AND ((e_var_name.label)::text = 'P1813'::text))))
     JOIN public.edges e_dataset ON ((((e_dataset.label)::text = 'P2006020003'::text) AND ((e_dataset.node2)::text = (e_var.node1)::text))))
     JOIN public.edges e_main ON (((e_var_property.node2)::text = (e_main.label)::text)))
     JOIN public.edges e_location ON ((((e_main.node1)::text = (e_location.node1)::text) AND ((e_location.label)::text = 'P276'::text))))
     JOIN public.edges e_admin2 ON ((((e_admin2.node1)::text = (e_location.node2)::text) AND ((e_admin2.label)::text = 'P2006190002'::text))))
  WHERE (((e_var.label)::text = 'P31'::text) AND ((e_var.node2)::text = 'Q50701'::text))
  WITH NO DATA;


ALTER TABLE public.fuzzy_admin2 OWNER TO postgres;

--
-- Name: fuzzy_admin3; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.fuzzy_admin3 AS
 SELECT e_var_name.node2 AS variable_id,
    e_dataset.node1 AS dataset_qnode,
    e_admin3.node2 AS admin3_qnode
   FROM (((((public.edges e_var
     JOIN public.edges e_var_property ON ((((e_var_property.node1)::text = (e_var.node1)::text) AND ((e_var_property.label)::text = 'P1687'::text))))
     JOIN public.edges e_var_name ON ((((e_var_name.node1)::text = (e_var.node1)::text) AND ((e_var_name.label)::text = 'P1813'::text))))
     JOIN public.edges e_dataset ON ((((e_dataset.label)::text = 'P2006020003'::text) AND ((e_dataset.node2)::text = (e_var.node1)::text))))
     JOIN public.edges e_main ON (((e_var_property.node2)::text = (e_main.label)::text)))
     JOIN public.edges e_admin3 ON ((((e_admin3.node1)::text = (e_main.node1)::text) AND ((e_admin3.label)::text = 'P2006190003'::text))))
  WHERE (((e_var.label)::text = 'P31'::text) AND ((e_var.node2)::text = 'Q50701'::text))
UNION
 SELECT e_var_name.node2 AS variable_id,
    e_dataset.node1 AS dataset_qnode,
    e_admin3.node2 AS admin3_qnode
   FROM ((((((public.edges e_var
     JOIN public.edges e_var_property ON ((((e_var_property.node1)::text = (e_var.node1)::text) AND ((e_var_property.label)::text = 'P1687'::text))))
     JOIN public.edges e_var_name ON ((((e_var_name.node1)::text = (e_var.node1)::text) AND ((e_var_name.label)::text = 'P1813'::text))))
     JOIN public.edges e_dataset ON ((((e_dataset.label)::text = 'P2006020003'::text) AND ((e_dataset.node2)::text = (e_var.node1)::text))))
     JOIN public.edges e_main ON (((e_var_property.node2)::text = (e_main.label)::text)))
     JOIN public.edges e_location ON ((((e_main.node1)::text = (e_location.node1)::text) AND ((e_location.label)::text = 'P276'::text))))
     JOIN public.edges e_admin3 ON ((((e_admin3.node1)::text = (e_location.node2)::text) AND ((e_admin3.label)::text = 'P2006190003'::text))))
  WHERE (((e_var.label)::text = 'P31'::text) AND ((e_var.node2)::text = 'Q50701'::text))
  WITH NO DATA;


ALTER TABLE public.fuzzy_admin3 OWNER TO postgres;

--
-- Name: fuzzy_country; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.fuzzy_country AS
 SELECT e_var_name.node2 AS variable_id,
    e_dataset.node1 AS dataset_qnode,
    e_country.node2 AS country_qnode
   FROM (((((public.edges e_var
     JOIN public.edges e_var_property ON ((((e_var_property.node1)::text = (e_var.node1)::text) AND ((e_var_property.label)::text = 'P1687'::text))))
     JOIN public.edges e_var_name ON ((((e_var_name.node1)::text = (e_var.node1)::text) AND ((e_var_name.label)::text = 'P1813'::text))))
     JOIN public.edges e_dataset ON ((((e_dataset.label)::text = 'P2006020003'::text) AND ((e_dataset.node2)::text = (e_var.node1)::text))))
     JOIN public.edges e_main ON (((e_var_property.node2)::text = (e_main.label)::text)))
     JOIN public.edges e_country ON ((((e_country.node1)::text = (e_main.node1)::text) AND ((e_country.label)::text = 'P17'::text))))
  WHERE (((e_var.label)::text = 'P31'::text) AND ((e_var.node2)::text = 'Q50701'::text))
UNION
 SELECT e_var_name.node2 AS variable_id,
    e_dataset.node1 AS dataset_qnode,
    e_country.node2 AS country_qnode
   FROM ((((((public.edges e_var
     JOIN public.edges e_var_property ON ((((e_var_property.node1)::text = (e_var.node1)::text) AND ((e_var_property.label)::text = 'P1687'::text))))
     JOIN public.edges e_var_name ON ((((e_var_name.node1)::text = (e_var.node1)::text) AND ((e_var_name.label)::text = 'P1813'::text))))
     JOIN public.edges e_dataset ON ((((e_dataset.label)::text = 'P2006020003'::text) AND ((e_dataset.node2)::text = (e_var.node1)::text))))
     JOIN public.edges e_main ON (((e_var_property.node2)::text = (e_main.label)::text)))
     JOIN public.edges e_location ON ((((e_main.node1)::text = (e_location.node1)::text) AND ((e_location.label)::text = 'P276'::text))))
     JOIN public.edges e_country ON ((((e_country.node1)::text = (e_location.node2)::text) AND ((e_country.label)::text = 'P17'::text))))
  WHERE (((e_var.label)::text = 'P31'::text) AND ((e_var.node2)::text = 'Q50701'::text))
  WITH NO DATA;


ALTER TABLE public.fuzzy_country OWNER TO postgres;

--
-- Name: quantities; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.quantities (
    edge_id character varying NOT NULL,
    number numeric NOT NULL,
    unit character varying,
    low_tolerance numeric,
    high_tolerance numeric
);


ALTER TABLE public.quantities OWNER TO postgres;

--
-- Name: strings; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.strings (
    edge_id character varying NOT NULL,
    text character varying NOT NULL,
    language character varying
);


ALTER TABLE public.strings OWNER TO postgres;

--
-- Name: symbols; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.symbols (
    edge_id character varying NOT NULL,
    symbol character varying NOT NULL
);


ALTER TABLE public.symbols OWNER TO postgres;


-- Just add what is needed for unit test

--
-- Data for Name: edges; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.edges (id, node1, label, node2, data_type) FROM stdin;
E6	P131	label	'located in the administrative territorial entity'@en	language_qualified_string
E1864	P248	label	'stated in'@en	language_qualified_string
E1871	P585	label	'point in time'@en	language_qualified_string
\.

--
-- Data for Name: strings; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.strings (edge_id, text, language) FROM stdin;
E6	located in the administrative territorial entity	en
E1864	stated in	en
E1871	point in time	en
\.


--
-- Name: alembic_version alembic_version_pkc; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.alembic_version
    ADD CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num);


--
-- Name: coordinates coordinates_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.coordinates
    ADD CONSTRAINT coordinates_pkey PRIMARY KEY (edge_id);


--
-- Name: dates dates_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dates
    ADD CONSTRAINT dates_pkey PRIMARY KEY (edge_id);


--
-- Name: edges edges_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.edges
    ADD CONSTRAINT edges_pkey PRIMARY KEY (id);


--
-- Name: quantities quantities_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.quantities
    ADD CONSTRAINT quantities_pkey PRIMARY KEY (edge_id);


--
-- Name: strings strings_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.strings
    ADD CONSTRAINT strings_pkey PRIMARY KEY (edge_id);


--
-- Name: symbols symbols_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.symbols
    ADD CONSTRAINT symbols_pkey PRIMARY KEY (edge_id);


--
-- Name: ix_edges_label_node2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_edges_label_node2 ON public.edges USING btree (label, node2);


--
-- Name: ix_edges_node1_label; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_edges_node1_label ON public.edges USING btree (node1, label);


--
-- Name: ix_edges_node2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_edges_node2 ON public.edges USING btree (node2);


--
-- Name: ix_fuzzy_admin1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ix_fuzzy_admin1 ON public.fuzzy_admin1 USING btree (variable_id, dataset_qnode, admin1_qnode);


--
-- Name: ix_fuzzy_admin2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ix_fuzzy_admin2 ON public.fuzzy_admin2 USING btree (variable_id, dataset_qnode, admin2_qnode);


--
-- Name: ix_fuzzy_admin3; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ix_fuzzy_admin3 ON public.fuzzy_admin3 USING btree (variable_id, dataset_qnode, admin3_qnode);


--
-- Name: ix_fuzzy_country; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ix_fuzzy_country ON public.fuzzy_country USING btree (variable_id, dataset_qnode, country_qnode);


--
-- Name: ix_symbols_symbol; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_symbols_symbol ON public.symbols USING btree (symbol);


--
-- Name: coordinates coordinates_edge_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.coordinates
    ADD CONSTRAINT coordinates_edge_id_fkey FOREIGN KEY (edge_id) REFERENCES public.edges(id) ON DELETE CASCADE DEFERRABLE;


--
-- Name: dates dates_edge_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dates
    ADD CONSTRAINT dates_edge_id_fkey FOREIGN KEY (edge_id) REFERENCES public.edges(id) ON DELETE CASCADE DEFERRABLE;


--
-- Name: quantities quantities_edge_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.quantities
    ADD CONSTRAINT quantities_edge_id_fkey FOREIGN KEY (edge_id) REFERENCES public.edges(id) ON DELETE CASCADE DEFERRABLE;


--
-- Name: strings strings_edge_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.strings
    ADD CONSTRAINT strings_edge_id_fkey FOREIGN KEY (edge_id) REFERENCES public.edges(id) ON DELETE CASCADE DEFERRABLE;


--
-- Name: symbols symbols_edge_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.symbols
    ADD CONSTRAINT symbols_edge_id_fkey FOREIGN KEY (edge_id) REFERENCES public.edges(id) ON DELETE CASCADE DEFERRABLE;


--
-- Name: fuzzy_admin1; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: postgres
--

REFRESH MATERIALIZED VIEW public.fuzzy_admin1;


--
-- Name: fuzzy_admin2; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: postgres
--

REFRESH MATERIALIZED VIEW public.fuzzy_admin2;


--
-- Name: fuzzy_admin3; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: postgres
--

REFRESH MATERIALIZED VIEW public.fuzzy_admin3;


--
-- Name: fuzzy_country; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: postgres
--

REFRESH MATERIALIZED VIEW public.fuzzy_country;


--
-- PostgreSQL database dump complete
--
