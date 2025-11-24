use database raw_gene_variant_data ; 
use schema raw_gene_variant_data.public;

select * from raw_gene_variant_data.public.gene_phenotype limit 10 ; --PAX3

select * from raw_gene_variant_data.public.variant limit 10;


SELECT count(*) FROM raw_gene_variant_data.public.variant 

SELECT count(*) FROM gene_phenotype WHERE gene IS NULL

select distinct zygosity from variant 
--------------------------------------------------------------
select sum(total_count) from raw_gene_variant_data.public.gene_phenotype;


select count(cdot,pdot, variant_classification, inheritance_pattern, zygosity) 
from raw_gene_variant_data.public.variant 



-----------------------------------------------------------

DROP database GENE_VARIANT_DB;

Create database GENE_VARIANT_DB;
Create schema GENE_VARIANT_DB.analytics;

use database GENE_VARIANT_DB;
use schema  GENE_VARIANT_DB.analytics;

Create or replace table DIM_GENE(
ID INT autoincrement, 
GENE varchar(255)
)
Create or replace table DIM_PHENOTYPE(
ID INT autoincrement,
HPO_ID varchar(255),
HPO_NAME varchar(255)
)

create or replace table DIM_VARIANT (
ID INT autoincrement,
CDOT varchar(255),
PDOT varchar(255),
variant_classification varchar(255),
inheritance_pattern varchar(255),
zygosity varchar(255)
)

Create or replace table fact_gene_phenotype( 
ID INT autoincrement,
gene_id number , 
phenotype_id number, 
TOTAL_COUNT number(18,0)
)


Create or replace table fact_variant (
ID INT autoincrement,
gene_id number , 
variant_id number, 
variant_occurrence_count number(18,0)
)




-----------------------------------------------------
use database GENE_VARIANT_DB;
use schema  GENE_VARIANT_DB.analytics;
---------------------------------------------------
INSERT INTO DIM_GENE(GENE)
SELECT DISTINCT Gene
FROM (
    SELECT Gene FROM raw_gene_variant_data.public.variant
    UNION
    SELECT Gene FROM raw_gene_variant_data.public.gene_phenotype
) AS combined;


INSERT INTO dim_phenotype (hpo_id, hpo_name)
SELECT DISTINCT HPO_Id, HPO_Name
FROM raw_gene_variant_data.public.gene_phenotype;

INSERT INTO dim_variant (cdot, pdot, variant_classification, inheritance_pattern, zygosity)
SELECT DISTINCT Cdot, Pdot, Variant_Classification, Inheritance_Pattern, Zygosity
FROM raw_gene_variant_data.public.variant;


INSERT INTO fact_gene_phenotype (gene_id, phenotype_id, total_count)
SELECT g.id, p.id, src.Total_Count
FROM raw_gene_variant_data.public.gene_phenotype src
JOIN dim_gene g ON src.Gene = g.gene
JOIN dim_phenotype p ON src.HPO_Id = p.hpo_id AND src.HPO_Name = p.hpo_name;


INSERT INTO fact_variant (gene_id, variant_id, variant_occurrence_count)
SELECT g.id, v.id, 1
FROM raw_gene_variant_data.public.variant src
JOIN dim_gene g ON src.Gene = g.gene
JOIN dim_variant v ON src.Cdot = v.cdot AND src.Pdot = v.pdot
    AND src.Variant_Classification = v.variant_classification
    AND src.Inheritance_Pattern = v.inheritance_pattern
    AND src.Zygosity = v.zygosity;



-------------------------------------------------------------------

select sum(total_count) from fact_gene_phenotype 

select sum(total_count) from raw_gene_variant_data.public.gene_phenotype;

---------------------------------------------------