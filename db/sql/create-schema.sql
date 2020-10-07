--
-- PostgreSQL database dump
--
-- This file contains the statements to create the database schema of Datamart API

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

--
-- Data for Name: alembic_version; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.alembic_version (version_num) FROM stdin;
a4a24ff3ad22
\.


--
-- Data for Name: coordinates; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.coordinates (edge_id, latitude, longitude, "precision") FROM stdin;
\.


--
-- Data for Name: dates; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.dates (edge_id, date_and_time, "precision", calendar) FROM stdin;
\.


--
-- Data for Name: edges; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.edges (id, node1, label, node2, data_type) FROM stdin;
\.


--
-- Data for Name: quantities; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.quantities (edge_id, number, unit, low_tolerance, high_tolerance) FROM stdin;
\.


--
-- Data for Name: strings; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.strings (edge_id, text, language) FROM stdin;
\.


--
-- Data for Name: symbols; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.symbols (edge_id, symbol) FROM stdin;
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
-- PostgreSQL database dump complete
--