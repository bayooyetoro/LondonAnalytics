CREATE TABLE [silver].[silver_companies_london] (

	[company_number] varchar(8000) NULL, 
	[company_name] varchar(8000) NULL, 
	[postcode] varchar(8000) NULL, 
	[postcode_district] varchar(8000) NULL, 
	[address_line1] varchar(8000) NULL, 
	[post_town] varchar(8000) NULL, 
	[company_category] varchar(8000) NULL, 
	[company_status] varchar(8000) NULL, 
	[incorporation_date] date NULL, 
	[incorporation_year] int NULL, 
	[company_age_years] int NULL, 
	[company_age_band] varchar(11) NULL, 
	[sic_code_primary] varchar(8000) NULL, 
	[sic_code_secondary] varchar(8000) NULL
);