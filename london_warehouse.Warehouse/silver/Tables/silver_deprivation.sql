CREATE TABLE [silver].[silver_deprivation] (

	[lsoa_code] varchar(8000) NULL, 
	[lsoa_name] varchar(8000) NULL, 
	[lad_code] varchar(8000) NULL, 
	[lad_name] varchar(8000) NULL, 
	[imd_rank] int NULL, 
	[imd_decile] int NULL, 
	[income_rank] int NULL, 
	[income_decile] int NULL, 
	[employment_rank] int NULL, 
	[employment_decile] int NULL, 
	[education_rank] int NULL, 
	[education_decile] int NULL, 
	[health_rank] int NULL, 
	[health_decile] int NULL, 
	[crime_deprivation_rank] int NULL, 
	[crime_deprivation_decile] int NULL, 
	[living_env_rank] int NULL, 
	[living_env_decile] int NULL, 
	[deprivation_category] varchar(15) NULL
);