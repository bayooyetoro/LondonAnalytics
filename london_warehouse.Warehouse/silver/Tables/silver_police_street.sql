CREATE TABLE [silver].[silver_police_street] (

	[crime_id] varchar(8000) NULL, 
	[crime_date] date NULL, 
	[crime_year] int NULL, 
	[crime_month_num] int NULL, 
	[crime_quarter] varchar(2) NULL, 
	[crime_type] varchar(8000) NULL, 
	[lsoa_code] varchar(8000) NULL, 
	[lsoa_name] varchar(8000) NULL, 
	[latitude] float NULL, 
	[longitude] float NULL, 
	[location] varchar(8000) NULL, 
	[last_outcome] varchar(8000) NULL, 
	[reported_by] varchar(8000) NULL, 
	[falls_within] varchar(8000) NULL
);