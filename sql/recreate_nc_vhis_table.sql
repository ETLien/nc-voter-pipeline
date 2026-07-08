DROP TABLE nc_vhis_history;

CREATE TABLE nc_vhis_history (
	county_id TEXT,
	county_desc TEXT,
	voter_reg_num TEXT,
	election_lbl DATE,
	election_desc TEXT,
	voting_method TEXT,
	voted_party_cd TEXT,
	voted_party_desc TEXT,
	pct_label TEXT,
	pct_description TEXT,
	ncid TEXT,
	voted_county_id TEXT,
	voted_county_desc TEXT,
	vtd_label TEXT,
	vtd_description TEXT
);

/*
COPY nc_vhis_history FROM ''
WITH (FORMAT csv, DELIMITER ',', QUOTE '"', ESCAPE '"', HEADER true, ENCODING 'UTF8');
*/