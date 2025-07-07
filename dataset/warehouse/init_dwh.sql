CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE warehouse.dim_date (
	date_id int4 NOT NULL,
	date_actual date NOT NULL,
	day_suffix varchar(4) NOT NULL,
	day_name varchar(9) NOT NULL,
	day_of_year int4 NOT NULL,
	week_of_month int4 NOT NULL,
	week_of_year int4 NOT NULL,
	week_of_year_iso bpchar(10) NOT NULL,
	month_actual int4 NOT NULL,
	month_name varchar(9) NOT NULL,
	month_name_abbreviated bpchar(3) NOT NULL,
	quarter_actual int4 NOT NULL,
	quarter_name varchar(9) NOT NULL,
	year_actual int4 NOT NULL,
	first_day_of_week date NOT NULL,
	last_day_of_week date NOT NULL,
	first_day_of_month date NOT NULL,
	last_day_of_month date NOT NULL,
	first_day_of_quarter date NOT NULL,
	last_day_of_quarter date NOT NULL,
	first_day_of_year date NOT NULL,
	last_day_of_year date NOT NULL,
	mmyyyy bpchar(6) NOT NULL,
	mmddyyyy bpchar(10) NOT NULL,
	weekend_indr varchar(20) NOT NULL
);

CREATE TABLE warehouse.dim_company (
	sk_company_id BIGSERIAL,
	nk_company_id VARCHAR(255),
	entity_type VARCHAR(20),
	full_address TEXT,
	region VARCHAR(255),
	city VARCHAR(255),
	country_code VARCHAR(10),
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE warehouse.dim_people (
	sk_people_id BIGSERIAL,
	nk_people_id VARCHAR(255),
	full_name VARCHAR(255),
	affiliation_name VARCHAR(255),
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE warehouse.dim_funds(
	sk_fund_id BIGSERIAL,
	nk_fund_id VARCHAR(255),
	fund_name VARCHAR(255),
	raised_amount_usd numeric,
	funded_at int,
	fund_description text,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE warehouse.bridge_company_people (
	sk_company_people_id BIGSERIAL,
	sk_company_id int,
	sk_people_id int,
	title varchar(255),
	is_past varchar(20),
	relationship_start_at int,
	relationship_end_at int,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE warehouse.fct_investments(
	sk_investment_id BIGSERIAL,
	dd_investment_id int,
	sk_company_id int,
	sk_fund_id int,
	funded_at int,
	funding_round_type varchar(255),
	num_of_participants text,
	raised_amount_usd numeric,
	pre_money_valuation_usd numeric,
	post_money_valuation_usd numeric,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP	
);

CREATE TABLE warehouse.fct_ipos (
	sk_ipo_id BIGSERIAL,
	dd_ipo_id int,
	sk_company_id int,
	valuation_amount_usd numeric,
	raised_amount_usd numeric,
	public_at int,
	stock_symbol varchar(100),
	ipo_description text,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE warehouse.fct_acquisition(
	sk_acquisition_id BIGSERIAL,
	dd_acquisition_id int,
	sk_acquiring_company_id int,
	sk_acquired_company_id int,
	price_amount_usd numeric,
	term_code varchar(100),
	acquired_at int,
	acquisition_description text,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP
);


-- Primary Key
alter table warehouse.dim_company
	add constraint dim_company_pk primary key (sk_company_id);

alter table warehouse.dim_people
	add constraint dim_people_pk primary key (sk_people_id);

alter table warehouse.dim_funds
	add constraint dim_funds_pk primary key (sk_fund_id);

alter table warehouse.dim_date
	add constraint dim_date_pk primary key (date_id);

alter table warehouse.bridge_company_people
	add constraint company_people_pk primary key (sk_company_people_id);

alter table warehouse.fct_investments 
	add constraint investments_pk primary key (sk_investment_id);

alter table warehouse.fct_ipos
	add constraint ipos_pk primary key (sk_ipo_id);

alter table warehouse.fct_acquisition
	add constraint acquisition_pk primary key (sk_acquisition_id);


-- Add unique constraint
alter table warehouse.dim_company
	add constraint unique_company_id unique (nk_company_id) ; 


-- Add foreign key

-- dim fund FK
alter table warehouse.dim_funds
	add constraint funds_company_fk foreign key (nk_fund_id) references warehouse.dim_company(nk_company_id);

alter table warehouse.dim_funds
	add constraint funds_date_fk foreign key (funded_at) references warehouse.dim_date(date_id);


-- Bridge people company FK
alter table warehouse.bridge_company_people
	add constraint bridge_company_fk foreign key (sk_company_id) references warehouse.dim_company(sk_company_id);

alter table warehouse.bridge_company_people
	add constraint bridge_people_fk foreign key (sk_people_id) references warehouse.dim_people(sk_people_id);

alter table warehouse.bridge_company_people
	add constraint bridge_date1_fk foreign key (relationship_start_at) references warehouse.dim_date(date_id);

alter table warehouse.bridge_company_people
	add constraint bridge_date2_fk foreign key (relationship_end_at) references warehouse.dim_date(date_id);

-- Fct investments FK
alter table warehouse.fct_investments 
	add constraint investment_company_fk foreign key (sk_company_id) references warehouse.dim_company(sk_company_id);

alter table warehouse.fct_investments 
	add constraint investment_fund_fk foreign key (sk_fund_id) references warehouse.dim_funds(sk_fund_id);

alter table warehouse.fct_investments 
	add constraint investment_date_fk foreign key (funded_at) references warehouse.dim_date(date_id);

-- Fct ipos FK
alter table warehouse.fct_ipos
	add constraint ipos_company_fk foreign key (sk_company_id) references warehouse.dim_company(sk_company_id);

alter table warehouse.fct_ipos 
	add constraint ipos_date_fk foreign key (public_at) references warehouse.dim_date(date_id);

-- Fct acquisition FK
alter table warehouse.fct_acquisition
	add constraint acquired_company_fk foreign key (sk_acquired_company_id) references warehouse.dim_company(sk_company_id);

alter table warehouse.fct_acquisition
	add constraint acquiring_company_fk foreign key (sk_acquiring_company_id) references warehouse.dim_company(sk_company_id);

alter table warehouse.fct_acquisition
	add constraint acquisition_date_fk foreign key (acquired_at) references warehouse.dim_date(date_id);

-- Generate dim_date
INSERT INTO warehouse.dim_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
       datum AS date_actual,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW') AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN 'weekend'
           ELSE 'weekday'
           END AS weekend_indr
FROM (SELECT '1950-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;
