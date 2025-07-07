CREATE SCHEMA IF NOT EXISTS staging;

--
-- Name: acquisition; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE staging.acquisition (
    acquisition_id integer NOT NULL,
    acquiring_object_id character varying(255),
    acquired_object_id character varying(255),
    term_code character varying(255),
    price_amount numeric(15,2),
    price_currency_code character varying(3),
    acquired_at timestamp without time zone,
    source_url text,
    source_description text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE staging.acquisition OWNER TO postgres;

--
-- Name: company; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE staging.company (
    office_id integer NOT NULL,
    object_id character varying(255),
    description text,
    region character varying(255),
    address1 text,
    address2 text,
    city character varying(255),
    zip_code character varying(200),
    state_code character varying(255),
    country_code character varying(255),
    latitude numeric(9,6),
    longitude numeric(9,6),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE staging.company OWNER TO postgres;

--
-- Name: funding_rounds; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE staging.funding_rounds (
    funding_round_id integer NOT NULL,
    object_id character varying(255),
    funded_at date,
    funding_round_type character varying(255),
    funding_round_code character varying(255),
    raised_amount_usd numeric(15,2),
    raised_amount numeric(15,2),
    raised_currency_code character varying(255),
    pre_money_valuation_usd numeric(15,2),
    pre_money_valuation numeric(15,2),
    pre_money_currency_code character varying(255),
    post_money_valuation_usd numeric(15,2),
    post_money_valuation numeric(15,2),
    post_money_currency_code character varying(255),
    participants text,
    is_first_round boolean,
    is_last_round boolean,
    source_url text,
    source_description text,
    created_by character varying(255),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE staging.funding_rounds OWNER TO postgres;

--
-- Name: funds; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE staging.funds (
    fund_id character varying(255) NOT NULL,
    object_id character varying(255),
    name character varying(255),
    funded_at date,
    raised_amount numeric(15,2),
    raised_currency_code character varying(3),
    source_url text,
    source_description text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE staging.funds OWNER TO postgres;

--
-- Name: investments; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE staging.investments (
    investment_id integer NOT NULL,
    funding_round_id integer,
    funded_object_id character varying,
    investor_object_id character varying,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE staging.investments OWNER TO postgres;

--
-- Name: ipos; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE staging.ipos (
    ipo_id character varying(255) NOT NULL,
    object_id character varying(255),
    valuation_amount numeric(15,2),
    valuation_currency_code character varying(3),
    raised_amount numeric(15,2),
    raised_currency_code character varying(3),
    public_at timestamp without time zone,
    stock_symbol character varying(255),
    source_url text,
    source_description text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE staging.ipos OWNER TO postgres;

--
-- Name: people; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE staging.people (
    people_id character varying(255) NOT NULL,
    object_id character varying(255),
    first_name character varying(255),
    last_name character varying(255),
    birthplace character varying(255),
    affiliation_name character varying(255),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE staging.people OWNER TO postgres;

--
-- Name: relationships; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE staging.relationships (
    relationship_id character varying(255) NOT NULL,
    person_object_id character varying(255),
    relationship_object_id character varying(255),
    start_at character varying(255),
    end_at character varying(255),
    is_past character varying(255),
    sequence character varying(255),
    title character varying(255),
    created_at character varying(255),
    updated_at character varying(255)
);

ALTER TABLE staging.relationships OWNER TO postgres;

--
-- Name: milestones; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE staging.milestones (
    created_at character varying(255),
    description text,
    milestone_at character varying(255),
    milestone_code character varying(255),
    milestone_id integer NOT NULL,
    object_id character varying(255),
    source_description text,
    source_url text,
    updated_at character varying(255)
);

ALTER TABLE staging.milestones OWNER TO postgres;

--
-- Name: acquisition acquisition_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.acquisition
    ADD CONSTRAINT acquisition_pkey PRIMARY KEY (acquisition_id);


--
-- Name: funding_rounds funding_rounds_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.funding_rounds
    ADD CONSTRAINT funding_rounds_pkey PRIMARY KEY (funding_round_id);


--
-- Name: funds funds_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.funds
    ADD CONSTRAINT funds_pkey PRIMARY KEY (fund_id);


--
-- Name: investments investments_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.investments
    ADD CONSTRAINT investments_pkey PRIMARY KEY (investment_id);


--
-- Name: ipos ipos_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.ipos
    ADD CONSTRAINT ipos_pkey PRIMARY KEY (ipo_id);


--
-- Name: company offices_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.company
    ADD CONSTRAINT offices_pkey PRIMARY KEY (office_id);

   
--
-- Name: people people_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.people
    ADD CONSTRAINT people_pkey PRIMARY KEY (people_id, object_id);
   
   
--
-- Name: relationships relationships_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.relationships
    ADD CONSTRAINT relationships_pkey PRIMARY KEY (relationship_id);
   
   
--
-- Name: milestones milestones_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.milestones
    ADD CONSTRAINT milestones_pkey PRIMARY KEY (milestone_id);   
   
   
--    
   
   
--
-- Name: company offices_un; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.company
    ADD CONSTRAINT offices_un UNIQUE (object_id);


--
-- Name: acquisition acquisition_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.acquisition
    ADD CONSTRAINT acquisition_fk FOREIGN KEY (acquiring_object_id) REFERENCES staging.company(object_id);


--
-- Name: acquisition acquisition_fk_1; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.acquisition
    ADD CONSTRAINT acquisition_fk_1 FOREIGN KEY (acquired_object_id) REFERENCES staging.company(object_id);


--
-- Name: funding_rounds funding_rounds_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.funding_rounds
    ADD CONSTRAINT funding_rounds_fk FOREIGN KEY (object_id) REFERENCES staging.company(object_id);


--
-- Name: funds funds_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.funds
    ADD CONSTRAINT funds_fk FOREIGN KEY (object_id) REFERENCES staging.company(object_id);


--
-- Name: investments investments_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.investments
    ADD CONSTRAINT investments_fk FOREIGN KEY (funded_object_id) REFERENCES staging.company(object_id);


--
-- Name: investments investments_fk_2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.investments
    ADD CONSTRAINT investments_fk_2 FOREIGN KEY (investor_object_id) REFERENCES staging.company(object_id);


--
-- Name: investments investments_fk_3; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.investments
    ADD CONSTRAINT investments_fk_3 FOREIGN KEY (funding_round_id) REFERENCES staging.funding_rounds(funding_round_id);


--
-- Name: ipos ipos_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.ipos
    ADD CONSTRAINT ipos_fk FOREIGN KEY (object_id) REFERENCES staging.company(object_id);


--
-- Name: milestones milestones_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY staging.milestones
    ADD CONSTRAINT milestone_fk FOREIGN KEY (object_id) REFERENCES staging.company(object_id);
