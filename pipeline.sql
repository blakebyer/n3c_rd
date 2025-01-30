@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9d2ba62d-1bc5-47eb-9944-bdaf29338b36"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b"),
    icd_to_snomed=Input(rid="ri.foundry.main.dataset.953fad34-b9c1-4241-af19-d8848cdc1970")
)
WITH icd10 AS (
    SELECT concept_code
    FROM icd_to_snomed
    WHERE vocabulary_id = 'ICD10CM'
),
icd9 AS (
    SELECT concept_code
    FROM icd_to_snomed
    WHERE vocabulary_id = 'ICD9CM'
),
joined AS (
SELECT l.person_id,
    l.condition_concept_id,
    l.condition_concept_name,
    l.condition_start_date,
    l.condition_end_date,
    l.condition_source_value,
    l.condition_source_concept_id,
    i.icd_concept_id,
    i.vocabulary_id,
    i.icd_concept_name,
    CASE 
        WHEN l.condition_source_value LIKE '%ICD9CM:%' THEN 
            SUBSTRING_INDEX(l.condition_source_value, 'ICD9CM:', -1) -- get everything afterwards
        WHEN l.condition_source_value LIKE '%:ICD-9-CM%' THEN
            SUBSTRING_INDEX(l.condition_source_value, ':ICD-9-CM', 1) -- everything before
        WHEN l.condition_source_value IN (SELECT concept_code FROM icd9) THEN
            l.condition_source_value
    END AS icd_9_concept_code,
    CASE 
        WHEN l.condition_source_value LIKE '%ICD10CM:%' THEN 
            SUBSTRING_INDEX(l.condition_source_value, 'ICD10CM:', -1) -- everything after
        WHEN l.condition_source_value LIKE '%:ICD-10-CM%' THEN
            SUBSTRING_INDEX(l.condition_source_value, ':ICD-10-CM', 1) -- everything before
        WHEN l.condition_source_value LIKE '%|ICD-10-CM%' THEN
            SUBSTRING_INDEX(l.condition_source_value, '|ICD-10-CM', 1) -- everything before
        WHEN l.condition_source_value IN (SELECT concept_code FROM icd10) THEN
            l.condition_source_value
    END AS icd_10_concept_code,
    concept_code,
    --COALESCE(i.concept_code, icd_10_concept_code) AS combined_concept_code,
    COALESCE(i.concept_code, icd_10_concept_code, icd_9_concept_code) AS combined_concept_code,
    l.data_partner_id   
FROM condition_occurrence l
LEFT JOIN icd_to_snomed i ON i.icd_concept_id = l.condition_source_concept_id),
new AS (
    SELECT * 
    FROM joined
    WHERE combined_concept_code IS NOT NULL 
        AND vocabulary_id IS NOT NULL -- make sure combined_concept_code is not null and vocab_id is not null so that a phecode can be mapped
),
-- phetk format (Python)
phe AS (SELECT
    person_id,
    vocabulary_id,
    combined_concept_code,
    condition_start_date,
    ROW_NUMBER() OVER (
        PARTITION BY person_id,
                vocabulary_id,
                combined_concept_code
        ORDER BY condition_start_date DESC) AS rn -- select unique phenotypes. If there is a tie select the latest ones
FROM new)
SELECT person_id,
    vocabulary_id,
    combined_concept_code,
    condition_start_date
FROM phe
WHERE rn = 1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a14e4e39-499b-4e43-8607-95b3d65ec838"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b"),
    lsd_concept_set=Input(rid="ri.foundry.main.dataset.6c7bd99e-16cb-4e86-806a-296b17a7d975")
)
SELECT condition_occurrence.*
FROM condition_occurrence JOIN lsd_concept_set ON condition_occurrence.condition_concept_id = lsd_concept_set.concept_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.228393db-ef06-40ad-be16-0239447aade8"),
    cohort_match=Input(rid="ri.foundry.main.dataset.1b418f38-b1b8-4857-827c-6aef6c1c7512"),
    lsd_cov=Input(rid="ri.foundry.main.dataset.50e2ba66-ba7c-4ca3-882c-69d23ff433de")
)
SELECT lsd_cov.*
FROM cohort_match 
JOIN lsd_cov ON lsd_cov.person_id = cohort_match.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.81c5302c-57c4-4a67-a560-57ef414f842e")
)
WITH conditions AS (SELECT DISTINCT condition_occurrence.*,
    cohort.has_lsd
FROM cohort JOIN condition_occurrence ON cohort.person_id = condition_occurrence.person_id),
icd10 AS (
    SELECT concept_code, vocabulary_id
    FROM icd_to_snomed
    WHERE vocabulary_id = 'ICD10CM'
        OR vocabulary_id = 'ICD10'
),
icd9 AS (
    SELECT concept_code, vocabulary_id
    FROM icd_to_snomed
    WHERE vocabulary_id = 'ICD9CM'
        OR vocabulary_id = 'ICD9'
),
joined AS (
SELECT l.person_id,
    l.condition_concept_id,
    l.condition_concept_name,
    l.condition_start_date,
    l.condition_end_date,
    l.condition_source_value,
    l.condition_source_concept_id,
    l.has_lsd,
    i.icd_concept_id,
    i.vocabulary_id,
    i.icd_concept_name,
    -- CASE 
    --     WHEN l.condition_source_value LIKE '%ICD9CM:%' THEN 
    --         SUBSTRING_INDEX(l.condition_source_value, 'ICD9CM:', -1) -- get everything afterwards
    --     WHEN l.condition_source_value LIKE '%:ICD-9-CM%' THEN
    --         SUBSTRING_INDEX(l.condition_source_value, ':ICD-9-CM', 1) -- everything before
    --     WHEN l.condition_source_value IN (SELECT concept_code FROM icd9) THEN
    --         l.condition_source_value
    -- END AS icd_9_concept_code,
    CASE 
        WHEN l.condition_source_value LIKE '%ICD10CM:%' THEN 
            SUBSTRING_INDEX(l.condition_source_value, 'ICD10CM:', -1) -- everything after
        WHEN l.condition_source_value LIKE '%:ICD-10-CM%' THEN
            SUBSTRING_INDEX(l.condition_source_value, ':ICD-10-CM', 1) -- everything before
        WHEN l.condition_source_value LIKE '%|ICD-10-CM%' THEN
            SUBSTRING_INDEX(l.condition_source_value, '|ICD-10-CM', 1) -- everything before
        WHEN l.condition_source_value IN (SELECT concept_code FROM icd10) THEN
            l.condition_source_value
    END AS icd_10_concept_code,
    concept_code,
    COALESCE(i.concept_code, icd_10_concept_code) AS combined_concept_code,
    --COALESCE(i.concept_code, icd_10_concept_code, icd_9_concept_code) AS combined_concept_code,
    l.data_partner_id   
FROM conditions l
LEFT JOIN icd_to_snomed i ON i.icd_concept_id = l.condition_source_concept_id),
new AS (
    SELECT j.*,
        icd10.vocabulary_id AS icd10_vocab_id,
        icd9.vocabulary_id AS icd9_vocab_id,
        COALESCE(j.vocabulary_id, icd10_vocab_id, icd9_vocab_id) AS combined_vocab_id
    FROM joined j
    LEFT JOIN icd10 ON icd10.concept_code = j.combined_concept_code
    LEFT JOIN icd9 ON icd9.concept_code = j.combined_concept_code
)
SELECT *
FROM new
-- new AS (
--     SELECT * 
--     FROM joined
--     WHERE combined_concept_code IS NOT NULL 
--         --AND vocabulary_id IS NOT NULL -- make sure combined_concept_code is not null and vocab_id is not null so that a phecode can be mapped
-- ),
-- SELECT *
-- FROM joined
-- -- phetk format (Python)
-- phe AS (SELECT
--     person_id,
--     vocabulary_id,
--     combined_concept_code,
--     condition_start_date,
--     has_lsd,
--     ROW_NUMBER() OVER (
--         PARTITION BY person_id,
--                 vocabulary_id,
--                 combined_concept_code,
--                 has_lsd
--         ORDER BY condition_start_date DESC) AS rn -- select unique phenotypes. If there is a tie select the latest ones
-- FROM new)
-- SELECT person_id,
--     vocabulary_id,
--     combined_concept_code,
--     condition_start_date,
--     has_lsd
-- FROM phe
-- WHERE rn = 1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7bde3f0f-e4da-4120-9019-9f1175cd7be0"),
    cohort=Input(rid="ri.foundry.main.dataset.228393db-ef06-40ad-be16-0239447aade8"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b"),
    icd_to_snomed=Input(rid="ri.foundry.main.dataset.953fad34-b9c1-4241-af19-d8848cdc1970")
)
WITH conditions AS (
    SELECT DISTINCT co.*,
    ch.has_lsd
FROM cohort ch JOIN condition_occurrence co ON ch.person_id = co.person_id),
conditions_source AS (
    SELECT person_id,
    has_lsd,
    condition_start_date,
    condition_source_value,
    condition_source_concept_id,
    icd_concept_name,
    vocabulary_id,
    concept_code,
    data_partner_id
    FROM conditions cs
    JOIN icd_to_snomed icd ON cs.condition_source_concept_id = icd.icd_concept_id
),
conditions_match AS (
    SELECT 
        cs.person_id,
        cs.has_lsd,
        cs.condition_start_date,
        cs.condition_source_value,
        cs.condition_source_concept_id,
        icd.icd_concept_name,
        icd.vocabulary_id,
        icd.concept_code AS concept_code,
        cs.data_partner_id
    FROM conditions cs
    JOIN icd_to_snomed icd ON cs.condition_source_value = icd.concept_code
),
conditions_substring AS (
    SELECT person_id,
    has_lsd,
    condition_source_value,
    condition_start_date,
    condition_source_concept_id,
    CASE 
        WHEN condition_source_value LIKE '%ICD10CM:%' THEN 
            SUBSTRING_INDEX(condition_source_value, 'ICD10CM:', -1) -- everything after
        WHEN condition_source_value LIKE '%:ICD-10-CM%' THEN
            SUBSTRING_INDEX(condition_source_value, ':ICD-10-CM', 1) -- everything before
        WHEN condition_source_value LIKE '%|ICD-10-CM%' THEN
            SUBSTRING_INDEX(condition_source_value, '|ICD-10-CM', 1) -- everything before
    END AS concept_code,
    data_partner_id
    FROM conditions cs
),
condition_sub_joined AS (
    SELECT
    person_id,
    has_lsd,
    condition_start_date,
    condition_source_value,
    condition_source_concept_id,
    icd_concept_name,
    vocabulary_id,
    cst.concept_code,
    data_partner_id
    FROM conditions_substring cst
    JOIN icd_to_snomed ON cst.concept_code = icd_to_snomed.concept_code
),
joined AS (
SELECT * FROM conditions_source
UNION
SELECT * FROM conditions_match
UNION
SELECT * FROM condition_sub_joined)
-- try a group by to get the earliest unique condition for each person. There are repeats
-- SELECT person_id,
--     has_lsd,
-- FROM joined
-- GROUP BY person_id,
--     condition_start_date,

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e50e9f88-3eb1-4b9f-883d-90e096771660"),
    cohort=Input(rid="ri.foundry.main.dataset.228393db-ef06-40ad-be16-0239447aade8"),
    death=Input(rid="ri.foundry.main.dataset.9c6c12b0-8e09-4691-91e4-e5ff3f837e69")
)
WITH new AS (SELECT * FROM death WHERE death_date IS NOT NULL),
list AS (SELECT DISTINCT person_id,
    concat_ws(';', collect_list(cause_concept_id)) AS cause_concept_id,
    concat_ws(';', collect_list(cause_concept_name)) AS cause_concept_name,
    concat_ws(';', collect_list(death_type_concept_id)) AS death_type_concept_id,
    any_value(death_date) AS death_date,
    any_value(data_partner_id) AS data_partner_id
FROM new
GROUP BY person_id),
cov AS (SELECT list.*,
    datediff(death_date, COVID_first_poslab_or_diagnosis_date) AS days_diff,
    cohort.Severity_Type
    FROM list
JOIN cohort ON cohort.person_id = list.person_id)

SELECT *
FROM cov
WHERE days_diff >= 0
   

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0aee8b3f-8eac-4d5b-917e-811062a10a17"),
    cohort=Input(rid="ri.foundry.main.dataset.228393db-ef06-40ad-be16-0239447aade8"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.fd499c1d-4b37-4cda-b94f-b7bf70a014da")
)
WITH drug AS (SELECT drug_exposure.*,
    cohort.Severity_Type,
    cohort.COVID_first_poslab_or_diagnosis_date,
    cohort.has_lsd,
    datediff(drug_exposure_start_date, COVID_first_poslab_or_diagnosis_date) AS days_diff
FROM drug_exposure
JOIN cohort ON cohort.person_id = drug_exposure.person_id)

SELECT *
FROM drug
WHERE days_diff BETWEEN 0 AND 30 -- drug administered in first 30 days. First 10 days might be better

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4ea455cb-97a6-4ae0-957a-d9b258032533"),
    lsd_cov=Input(rid="ri.foundry.main.dataset.50e2ba66-ba7c-4ca3-882c-69d23ff433de")
)
WITH MedianAge AS (
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Age) AS median_age
    FROM lsd_cov
    WHERE Age IS NOT NULL
)
SELECT person_id, 
has_lsd, 
CASE 
    WHEN Sex IS NULL THEN 'Unknown'
    ELSE Sex
    END AS Sex,
CASE 
    WHEN Age IS NULL THEN (SELECT median_age FROM MedianAge)
    ELSE Age
    END AS Age,
CASE 
    WHEN Race IS NULL OR Race = 'Other' THEN 'Other/Unknown'
    ELSE Race
    END AS Race,
Severity_Type, 
data_partner_id
FROM lsd_cov

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f3b658ac-f008-4d1e-8e33-ebee67086b30"),
    cohort=Input(rid="ri.foundry.main.dataset.228393db-ef06-40ad-be16-0239447aade8"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    measurement=Input(rid="ri.foundry.main.dataset.29834e2c-f924-45e8-90af-246d29456293")
)
WITH joined AS (SELECT measurement.*,
    cohort.Severity_Type,
    cohort.COVID_first_poslab_or_diagnosis_date
FROM measurement
JOIN cohort ON cohort.person_id = measurement.person_id),
complete AS (
    SELECT *
    FROM joined
    WHERE harmonized_value_as_number IS NOT NULL
    --AND M.range_low IS NOT NULL
    --AND M.range_high IS NOT NULL
    AND harmonized_unit_concept_id IS NOT NULL
)
SELECT complete.*,
    concept.concept_code AS units,
    datediff(complete.measurement_date, complete.COVID_first_poslab_or_diagnosis_date) AS days_diff
FROM complete 
JOIN concept ON concept.concept_id = complete.harmonized_unit_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9148c50d-1263-4446-9781-5c5b249f993f"),
    cohort_measurement=Input(rid="ri.foundry.main.dataset.f3b658ac-f008-4d1e-8e33-ebee67086b30"),
    ml_measurements=Input(rid="ri.foundry.main.dataset.8b443c3f-7205-4da9-90f0-bb1bc933fb12")
)
SELECT cm.*
FROM ml_measurements ml
JOIN cohort_measurement cm ON ml.concept_id = cm.measurement_concept_id
WHERE days_diff BETWEEN 0 AND 3 -- first four days of COVID to predict future hospitalization

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3eea9aef-4915-4257-9fae-524ffb1cc7b5"),
    cohort_conditions_simple=Input(rid="ri.foundry.main.dataset.7bde3f0f-e4da-4120-9019-9f1175cd7be0"),
    phecode_map=Input(rid="ri.foundry.main.dataset.78b961ea-3fd8-4e0d-bdc0-d9805d8de1a4")
)
SELECT chc.person_id,
    chc.condition_start_date,
    chc.has_lsd,
    phm.*
FROM cohort_conditions_simple chc
JOIN phecode_map phm ON chc.concept_code = phm.code
-- 5560 has_lsd = 1 2,187,573 rows. Total 5628 so missing 68 people. 393 rows per person
-- 5045 has_lsd = 0 730,195 rows. Total 5628 so missing 583 people. 144 rows per person

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.41ee23d6-76f8-44ea-af1c-7677ec730a30"),
    cohort=Input(rid="ri.foundry.main.dataset.228393db-ef06-40ad-be16-0239447aade8"),
    procedure_occurrence=Input(rid="ri.foundry.main.dataset.f8826e21-741d-49bb-a7eb-47ea98bb2b5f")
)
WITH pro AS (SELECT procedure_occurrence.*,
    cohort.COVID_first_poslab_or_diagnosis_date,
    cohort.Severity_Type,
    cohort.has_lsd,
    datediff(procedure_date, COVID_first_poslab_or_diagnosis_date) AS days_diff
FROM procedure_occurrence
JOIN cohort ON cohort.person_id = procedure_occurrence.person_id)

SELECT *
FROM pro
WHERE days_diff BETWEEN 0 AND 30 -- only procedures within the 30 days following COVID

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7aa5352a-84a7-41b9-8bd1-39594475a47d"),
    cov_drugs=Input(rid="ri.foundry.main.dataset.e5bdbe0a-a566-4a68-b979-44859549af3a")
)
SELECT concept_id,
    concept_name,
    CASE
    WHEN LOWER(concept_name) LIKE '%dexamethasone%' THEN 'dexamethasone'
    WHEN LOWER(concept_name) LIKE '%baricitinib%' THEN 'baricitinib'
    WHEN LOWER(concept_name) LIKE '%nirmatrelvir%' 
        OR LOWER(concept_name) LIKE '%ritonavir%' 
        OR LOWER(concept_name) LIKE '%paxlovid%' THEN 'paxlovid'
    WHEN LOWER(concept_name) LIKE '%remdesivir%' THEN 'remdesivir'
    WHEN LOWER (concept_name) LIKE '%tocilizumab%' THEN 'tocilizumab'
    WHEN LOWER(concept_name) LIKE '%hydrocortisone%' THEN 'hydrocortisone'
     WHEN LOWER(concept_name) LIKE '%prednisolone%' THEN 'prednisolone'
    ELSE 'Other'
  END AS drug_name
FROM cov_drugs

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e5bdbe0a-a566-4a68-b979-44859549af3a"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT *
FROM concept_set_members 
WHERE codeset_id = 373794531 AND is_most_recent_version = true -- fda drugs
    OR codeset_id = 84943298 -- corticosteroids

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ffc7b3b2-038c-462f-a535-6ea666c9b6b1"),
    cohort_death=Input(rid="ri.foundry.main.dataset.e50e9f88-3eb1-4b9f-883d-90e096771660"),
    cohort_drug_exposure=Input(rid="ri.foundry.main.dataset.0aee8b3f-8eac-4d5b-917e-811062a10a17"),
    cov_drug_type=Input(rid="ri.foundry.main.dataset.7aa5352a-84a7-41b9-8bd1-39594475a47d")
)
WITH drugs AS (
    SELECT cd.*,
           f.concept_name,
           f.drug_name
    FROM cohort_drug_exposure cd 
    JOIN cov_drug_type f ON f.concept_id = cd.drug_concept_id
),
death AS (
    SELECT d.*, 
           co.death_date
    FROM drugs d
    LEFT JOIN cohort_death co ON co.person_id = d.person_id 
),
combined AS (SELECT 
    e.person_id,
    e.drug_concept_id,
    e.concept_name,
    e.drug_name,
    --e.has_lsd,
    --e.Severity_Type,
    e.death_date,
    COALESCE(
        DATEDIFF(e.death_date, MAX(e.drug_exposure_end_date)), -- if
        DATEDIFF('2024-12-31', MAX(e.drug_exposure_end_date)) -- else
    ) AS days,
    MAX(e.drug_exposure_end_date) AS drug_exposure_date
FROM death e
GROUP BY 
    e.person_id,
    e.drug_concept_id,
    e.concept_name,
    --e.has_lsd,
    --e.Severity_Type,
    e.drug_name,
    e.death_date)
SELECT d.*,
    CASE 
        WHEN d.death_date IS NOT NULL THEN 1
        ELSE 0
    END AS status
FROM combined d
WHERE d.drug_exposure_date IS NOT NULL
    AND d.days >= 0 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.953fad34-b9c1-4241-af19-d8848cdc1970"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    concept_relationship=Input(rid="ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71")
)
WITH icd AS (SELECT * -- Get ICD10CM
FROM concept
WHERE vocabulary_id = 'ICD10CM'),
    -- OR vocabulary_id = 'ICD9CM'
    -- OR vocabulary_id = 'ICD10'
    -- OR vocabulary_id = 'ICD9'),
joined AS (
SELECT icd.*,
    concept_id_1,
    concept_id_2, 
    relationship_id
FROM icd
LEFT JOIN concept_relationship ON icd.concept_id = concept_relationship.concept_id_1
    AND (relationship_id = 'Maps to' OR relationship_id IS NULL)), -- keep all ICD with a LEFT JOIN
snomed_names AS (
SELECT j.*, -- Bring in the name
    c.concept_name AS mapped_concept_name,
    c.domain_id AS mapped_domain_id,
    c.vocabulary_id AS mapped_vocabulary_id
FROM joined j
LEFT JOIN concept c ON j.concept_id_2 = c.concept_id
    AND (c.vocabulary_id = 'SNOMED' OR c.vocabulary_id IS NULL))
SELECT concept_id_1 AS icd_concept_id,
    concept_name AS icd_concept_name,
    domain_id,
    vocabulary_id, 
    concept_code,
    relationship_id,
    concept_id_2 AS snomed_concept_id,
    mapped_concept_name,
    mapped_domain_id,
    mapped_vocabulary_id
FROM snomed_names

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1548c5bb-369b-4ffb-a026-6a7691496bad"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    concept_synonym=Input(rid="ri.foundry.main.dataset.37b74b8c-f136-4b41-b022-2d61a10fc5db")
)
-- Only select LOINC concepts
WITH loinc AS (SELECT *
FROM concept
WHERE vocabulary_id = 'LOINC'),
-- Find synonyms of LOINC concepts to get their scale
synonyms AS (
SELECT loinc.*,
    C.concept_synonym_name,
    C.language_concept_id
FROM loinc
JOIN concept_synonym C ON loinc.concept_id = C.concept_id
WHERE C.language_concept_id = 4180186 -- Loinc language id
    AND C.concept_synonym_name REGEXP '[A-Za-z]'), -- At least one English character
scales AS (SELECT *,
  CASE
    WHEN LOWER(concept_synonym_name) LIKE '%quantitative%' AND LOWER(concept_synonym_name) LIKE '%ordinal%' THEN 'OrdQn'
    WHEN LOWER(concept_synonym_name) LIKE '%ordinal%' THEN 'OrdQn'
    WHEN LOWER(concept_synonym_name) LIKE '%quantitative%' THEN 'Qn'
    WHEN LOWER(concept_synonym_name) LIKE '%nominal%' OR LOWER(concept_synonym_name) LIKE '%qualitative%' THEN 'Nom'
    WHEN LOWER(concept_synonym_name) LIKE '%narrative%' THEN 'Nar'
    WHEN LOWER(concept_synonym_name) LIKE '%doc%' THEN 'Doc'
    WHEN LOWER(concept_synonym_name) LIKE '%set%' THEN 'Set'
    WHEN LOWER(concept_synonym_name) LIKE '%multi%' THEN 'Multi'
    WHEN LOWER(concept_synonym_name) LIKE '%panel%' OR LOWER(concept_synonym_name) LIKE '%pnl%' OR LOWER(concept_synonym_name) LIKE '%panl%' THEN 'Pnl'
    ELSE 'Other'
  END AS scale
FROM synonyms),
ranked AS ( -- Rank synonyms if there are more than one per term
    SELECT *,
ROW_NUMBER() OVER (
                PARTITION BY concept_id
                ORDER BY 
                    CASE 
                        WHEN scale = 'Qn' THEN 1
                        WHEN scale = 'OrdQn' THEN 2
                        WHEN scale = 'Nom' THEN 3
                        WHEN scale = 'Nar' THEN 4
                        WHEN scale = 'Doc' THEN 5
                        WHEN scale = 'Set' THEN 6
                        WHEN scale = 'Multi' THEN 7
                        WHEN scale = 'Pnl' THEN 8
                        ELSE 9  -- "Other" or any less preferred value
                    END
            ) AS rn
FROM scales)

SELECT concept_id, 
    concept_name, 
    domain_id,
    vocabulary_id,
    concept_class_id,
    standard_concept,
    concept_code,
    scale    
FROM ranked
WHERE rn = 1; -- If there are multiple synonyms select Ord, Qn, or Nom over Other

-- https://loinc.org/kb/users-guide/major-parts-of-a-loinc-term/#type-of-scale-5th-part
-- Parts of LOINC Term outcome H, L, NEG, POS, etc

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c9d370c7-6e0c-4820-aa13-48985c85f22f"),
    concept_ancestor=Input(rid="ri.foundry.main.dataset.c5e0521a-147e-4608-b71e-8f53bcdbe03c"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    lsd_concept_set=Input(rid="ri.foundry.main.dataset.6c7bd99e-16cb-4e86-806a-296b17a7d975")
)
WITH descendants AS (
    SELECT 
        concept_ancestor.*,
        concept_set_members.concept_name AS descendant_name
FROM concept_ancestor
JOIN concept_set_members ON concept_ancestor.descendant_concept_id = concept_set_members.concept_id),
ancestors AS (
    SELECT descendants.*,
        concept_set_members.concept_name AS ancestor_name
    FROM descendants
    JOIN concept_set_members ON descendants.ancestor_concept_id = concept_set_members.concept_id)

SELECT DISTINCT ancestors.ancestor_concept_id,
     ancestors.ancestor_name,
     ancestors.descendant_concept_id,
    ancestors.descendant_name,        
    ancestors.min_levels_of_separation,
    ancestors.max_levels_of_separation
FROM ancestors
JOIN lsd_concept_set ON ancestors.ancestor_concept_id = lsd_concept_set.concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6c7bd99e-16cb-4e86-806a-296b17a7d975"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT *
FROM concept_set_members 
WHERE codeset_id = 79972333 AND is_most_recent_version = true;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.115d8d02-843c-4c0d-b03e-b1d2e24aad95"),
    icd_to_snomed=Input(rid="ri.foundry.main.dataset.953fad34-b9c1-4241-af19-d8848cdc1970"),
    lsds_conditions=Input(rid="ri.foundry.main.dataset.5ec35013-423e-47db-928d-bfa1b3e3aa70")
)
WITH icd10 AS (
    SELECT concept_code
    FROM icd_to_snomed
    WHERE vocabulary_id = 'ICD10CM'
),
icd9 AS (
    SELECT concept_code
    FROM icd_to_snomed
    WHERE vocabulary_id = 'ICD9CM'
),
joined AS (
SELECT l.person_id,
    l.condition_concept_id,
    l.condition_concept_name,
    l.condition_start_date,
    l.condition_end_date,
    l.condition_source_value,
    l.condition_source_concept_id,
    i.icd_concept_id,
    i.vocabulary_id,
    i.icd_concept_name,
    CASE 
        WHEN l.condition_source_value LIKE '%ICD9CM:%' THEN 
            SUBSTRING_INDEX(l.condition_source_value, 'ICD9CM:', -1) -- get everything afterwards
        WHEN l.condition_source_value LIKE '%:ICD-9-CM%' THEN
            SUBSTRING_INDEX(l.condition_source_value, ':ICD-9-CM', 1) -- everything before
        WHEN l.condition_source_value IN (SELECT concept_code FROM icd9) THEN
            l.condition_source_value
    END AS icd_9_concept_code,
    CASE 
        WHEN l.condition_source_value LIKE '%ICD10CM:%' THEN 
            SUBSTRING_INDEX(l.condition_source_value, 'ICD10CM:', -1) -- everything after
        WHEN l.condition_source_value LIKE '%:ICD-10-CM%' THEN
            SUBSTRING_INDEX(l.condition_source_value, ':ICD-10-CM', 1) -- everything before
        WHEN l.condition_source_value LIKE '%|ICD-10-CM%' THEN
            SUBSTRING_INDEX(l.condition_source_value, '|ICD-10-CM', 1) -- everything before
        WHEN l.condition_source_value IN (SELECT concept_code FROM icd10) THEN
            l.condition_source_value
    END AS icd_10_concept_code,
    concept_code,
    --COALESCE(i.concept_code, icd_10_concept_code) AS combined_concept_code,
    COALESCE(i.concept_code, icd_10_concept_code, icd_9_concept_code) AS combined_concept_code,
    l.data_partner_id   
FROM lsds_conditions l
LEFT JOIN icd_to_snomed i ON i.icd_concept_id = l.condition_source_concept_id),
new AS (
    SELECT * 
    FROM joined
    WHERE combined_concept_code IS NOT NULL 
        AND vocabulary_id IS NOT NULL -- make sure combined_concept_code is not null and vocab_id is not null so that a phecode can be mapped
),
-- phetk format (Python)
phe AS (SELECT
    person_id,
    vocabulary_id,
    combined_concept_code,
    condition_start_date,
    ROW_NUMBER() OVER (
        PARTITION BY person_id,
                vocabulary_id,
                combined_concept_code
        ORDER BY condition_start_date DESC) AS rn -- select unique phenotypes. If there is a tie select the latest ones
FROM new)
SELECT person_id,
    vocabulary_id,
    combined_concept_code,
    condition_start_date
FROM phe
WHERE rn = 1

-- SELECT person_id, 
--     condition_concept_id, 
--     combined_concept_code,
--     condition_start_date
-- FROM final 
-- WHERE rn = 1

-- SELECT person_id,
--     vocabulary_id, -- R-phewas input format
--     combined_concept_code,
--     CAST(COUNT(DISTINCT condition_start_date) AS INT) AS condition_count
-- FROM new
-- GROUP BY 
--     person_id,
--     vocabulary_id,
--     combined_concept_code
-- SELECT
--     person_id,
--     condition_concept_id,
--     combined_concept_code,
--     condition_start_date,
--     ROW_NUMBER() OVER (
--         PARTITION BY person_id, condition_concept_id, combined_concept_code
--         ORDER BY condition_start_date DESC) AS rn -- select unique phenotypes. If there is a tie select the latest ones
-- FROM joined
-- WHERE combined_concept_code IS NOT NULL)

-- SELECT person_id, 
--     condition_concept_id, 
--     combined_concept_code,
--     condition_start_date
-- FROM final 
-- WHERE rn = 1

-- when grouping all lsd conditions by person_id, condition_concept_id and condition_start_date and only picking the final start date I got 1,392,740 rows. So I guess source codes have more variation than the condition_concept_ids 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.50e2ba66-ba7c-4ca3-882c-69d23ff433de"),
    Logic_Liaison_Covid_19_Patient_Summary_Facts_Table_De_identified_=Input(rid="ri.foundry.main.dataset.ae01d2c8-5c70-428f-a0aa-de30d587b2bb"),
    specific_lsds=Input(rid="ri.foundry.main.dataset.d6021714-b1ff-4530-b6b9-56db702a6337")
)
WITH joined AS (SELECT 
    L.person_id,
    L.COVID_first_PCR_or_AG_lab_positive,
    L.COVID_first_diagnosis_date,
    L.COVID_first_poslab_or_diagnosis_date,
    CAST(L.number_of_visits_before_covid AS INT) AS number_of_visits_before_covid,
    L.observation_period_before_covid,
    CAST(L.number_of_visits_post_covid AS INT) AS number_of_visits_post_covid,
    L.observation_period_post_covid,
    L.city,
    L.state,
    L.postal_code,
    L.county,
    CAST(L.age_at_covid AS INT) AS Age,
    L.race AS Race,
    L.race_ethnicity,
    L.data_partner_id,
    L.data_extraction_date,
    L.cdm_name,
    L.cdm_version,
    L.shift_date_yn,
    L.max_num_shift_days,
    CAST(L.BMI_max_observed_or_calculated_before_or_day_of_covid AS FLOAT) AS BMI_max_observed_or_calculated_before_or_day_of_covid,
    L.TUBERCULOSIS_before_or_day_of_covid_indicator,
    L.MILDLIVERDISEASE_before_or_day_of_covid_indicator,
    L.MODERATESEVERELIVERDISEASE_before_or_day_of_covid_indicator,
    L.THALASSEMIA_before_or_day_of_covid_indicator,
    L.RHEUMATOLOGICDISEASE_before_or_day_of_covid_indicator,
    L.DEMENTIA_before_or_day_of_covid_indicator,
    L.CONGESTIVEHEARTFAILURE_before_or_day_of_covid_indicator,
    L.SUBSTANCEUSEDISORDER_before_or_day_of_covid_indicator,
    L.DOWNSYNDROME_before_or_day_of_covid_indicator,
    L.KIDNEYDISEASE_before_or_day_of_covid_indicator,
    L.MALIGNANTCANCER_before_or_day_of_covid_indicator,
    L.DIABETESCOMPLICATED_before_or_day_of_covid_indicator,
    L.CEREBROVASCULARDISEASE_before_or_day_of_covid_indicator,
    L.PERIPHERALVASCULARDISEASE_before_or_day_of_covid_indicator,
    L.PREGNANCY_before_or_day_of_covid_indicator,
    L.HEARTFAILURE_before_or_day_of_covid_indicator,
    L.HEMIPLEGIAORPARAPLEGIA_before_or_day_of_covid_indicator,
    L.PSYCHOSIS_before_or_day_of_covid_indicator,
    L.OBESITY_before_or_day_of_covid_indicator,
    L.CORONARYARTERYDISEASE_before_or_day_of_covid_indicator,
    L.SYSTEMICCORTICOSTEROIDS_before_or_day_of_covid_indicator,
    L.DEPRESSION_before_or_day_of_covid_indicator,
    L.METASTATICSOLIDTUMORCANCERS_before_or_day_of_covid_indicator,
    L.HIVINFECTION_before_or_day_of_covid_indicator,
    L.CHRONICLUNGDISEASE_before_or_day_of_covid_indicator,
    L.PEPTICULCER_before_or_day_of_covid_indicator,
    L.SICKLECELLDISEASE_before_or_day_of_covid_indicator,
    L.MYOCARDIALINFARCTION_before_or_day_of_covid_indicator,
    L.DIABETESUNCOMPLICATED_before_or_day_of_covid_indicator,
    L.CARDIOMYOPATHIES_before_or_day_of_covid_indicator,
    L.HYPERTENSION_before_or_day_of_covid_indicator,
    L.OTHERIMMUNOCOMPROMISED_before_or_day_of_covid_indicator,
    L.Antibody_Neg_before_or_day_of_covid_indicator,
    L.PULMONARYEMBOLISM_before_or_day_of_covid_indicator,
    L.TOBACCOSMOKER_before_or_day_of_covid_indicator,
    L.SOLIDORGANORBLOODSTEMCELLTRANSPLANT_before_or_day_of_covid_indicator,
    L.Antibody_Pos_before_or_day_of_covid_indicator,
    CAST(L.number_of_COVID_vaccine_doses_before_or_day_of_covid AS INT) AS number_of_COVID_vaccine_doses_before_or_day_of_covid,
    L.LL_IMV_during_strong_covid_hospitalization_indicator,
    L.COVID_patient_death_during_strong_covid_hospitalization_indicator,
    L.COVIDREGIMENCORTICOSTEROIDS_during_strong_covid_hospitalization_indicator,
    L.COVID_diagnosis_during_strong_covid_hospitalization_indicator,
    L.REMDISIVIR_during_strong_covid_hospitalization_indicator,
    L.LL_ECMO_during_strong_covid_hospitalization_indicator,
    L.LL_IMV_during_weak_covid_hospitalization_indicator,
    L.COVID_patient_death_during_weak_covid_hospitalization_indicator,
    L.COVIDREGIMENCORTICOSTEROIDS_during_weak_covid_hospitalization_indicator,
    L.COVID_diagnosis_during_weak_covid_hospitalization_indicator,
    L.REMDISIVIR_during_weak_covid_hospitalization_indicator,
    L.LL_ECMO_during_weak_covid_hospitalization_indicator,
    CAST(L.BMI_max_observed_or_calculated_post_covid AS FLOAT) AS BMI_max_observed_or_calculated_post_covid,
    L.TUBERCULOSIS_post_covid_indicator,
    L.PCR_AG_Pos_post_covid_indicator,
    L.MILDLIVERDISEASE_post_covid_indicator,
    L.MODERATESEVERELIVERDISEASE_post_covid_indicator,
    L.PNEUMONIADUETOCOVID_post_covid_indicator,
    L.THALASSEMIA_post_covid_indicator,
    L.RHEUMATOLOGICDISEASE_post_covid_indicator,
    L.DEMENTIA_post_covid_indicator,
    L.CONGESTIVEHEARTFAILURE_post_covid_indicator,
    L.SUBSTANCEUSEDISORDER_post_covid_indicator,
    L.Long_COVID_clinic_visit_post_covid_indicator,
    L.DOWNSYNDROME_post_covid_indicator,
    L.KIDNEYDISEASE_post_covid_indicator,
    L.MALIGNANTCANCER_post_covid_indicator,
    L.MISC_post_covid_indicator,
    L.DIABETESCOMPLICATED_post_covid_indicator,
    L.CEREBROVASCULARDISEASE_post_covid_indicator,
    L.PERIPHERALVASCULARDISEASE_post_covid_indicator,
    L.PREGNANCY_post_covid_indicator,
    L.HEARTFAILURE_post_covid_indicator,
    L.HEMIPLEGIAORPARAPLEGIA_post_covid_indicator,
    L.PSYCHOSIS_post_covid_indicator,
    L.OBESITY_post_covid_indicator,
    L.CORONARYARTERYDISEASE_post_covid_indicator,
    L.PCR_AG_Neg_post_covid_indicator,
    L.SYSTEMICCORTICOSTEROIDS_post_covid_indicator,
    L.DEPRESSION_post_covid_indicator,
    L.METASTATICSOLIDTUMORCANCERS_post_covid_indicator,
    L.HIVINFECTION_post_covid_indicator,
    L.CHRONICLUNGDISEASE_post_covid_indicator,
    L.B94_8_post_covid_indicator,
    L.PEPTICULCER_post_covid_indicator,
    L.SICKLECELLDISEASE_post_covid_indicator,
    L.MYOCARDIALINFARCTION_post_covid_indicator,
    L.Long_COVID_diagnosis_post_covid_indicator,
    L.DIABETESUNCOMPLICATED_post_covid_indicator,
    L.CARDIOMYOPATHIES_post_covid_indicator,
    L.HYPERTENSION_post_covid_indicator,
    L.OTHERIMMUNOCOMPROMISED_post_covid_indicator,
    L.Antibody_Neg_post_covid_indicator,
    L.PULMONARYEMBOLISM_post_covid_indicator,
    L.TOBACCOSMOKER_post_covid_indicator,
    L.SOLIDORGANORBLOODSTEMCELLTRANSPLANT_post_covid_indicator,
    L.Antibody_Pos_post_covid_indicator,
    CAST(L.number_of_COVID_vaccine_doses_post_covid AS INT) AS number_of_COVID_vaccine_doses_post_covid,
    L.had_at_least_one_reinfection_post_covid_indicator,
    L.first_strong_COVID_ED_only_start_date,
    L.first_strong_COVID_hospitalization_start_date,
    L.first_strong_COVID_hospitalization_end_date,
    L.first_weak_COVID_ED_only_start_date,
    L.first_weak_COVID_hospitalization_start_date,
    L.first_weak_COVID_hospitalization_end_date,
    L.strong_COVID_hospitalization_length_of_stay,
    L.COVID_patient_death_indicator,
    L.death_within_specified_window_post_covid,
    L.Severity_Type,
    specific_lsds.condition_concept_id,
    specific_lsds.condition_source_concept_id,
    specific_lsds.condition_source_value,
    specific_lsds.specific_condition_name,
    specific_lsds.condition_start_date,
    specific_lsds.condition_end_date,
    specific_lsds.data_partner_id AS lsd_data_partner_id,
    CASE 
           WHEN specific_lsds.condition_concept_id IS NOT NULL THEN 1
           ELSE 0 
       END AS has_lsd,
    CASE 
            WHEN L.sex == 'MALE' THEN 'MALE'
            WHEN L.sex == 'FEMALE' THEN 'FEMALE'
            ELSE NULL
        END AS Sex
FROM Logic_Liaison_Covid_19_Patient_Summary_Facts_Table_De_identified_ L
LEFT JOIN specific_lsds ON L.person_id = specific_lsds.person_id)

SELECT * 
FROM joined 
WHERE COVID_first_poslab_or_diagnosis_date >= '2018-01-01' AND 
    COVID_first_poslab_or_diagnosis_date <= '2026-12-31' --'20100401'  (Format of date yyyymmdd) In lieu of selecting a branch of the dataset

-- LONG to INT
-- number_of_visits_before_covid
-- number_of_visits_post_covid
--age_at_covid
-- number_of_COVID_vaccine_doses_before_or_day_of_covid
-- number_of_COVID_vaccine_doses_post_covid

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.28af6f86-7f3e-423e-bdc9-975173fb9c01"),
    person=Input(rid="ri.foundry.main.dataset.af5e5e91-6eeb-4b14-86df-18d84a5aa010"),
    specific_lsds=Input(rid="ri.foundry.main.dataset.d6021714-b1ff-4530-b6b9-56db702a6337")
)
SELECT l.*,
    race_concept_id,
    race_concept_name,
    gender_concept_id,
    gender_concept_name,
    month_of_birth,
    year_of_birth,
    YEAR(current_date()) - year_of_birth 
    - CASE 
          WHEN MONTH(current_date()) < month_of_birth -- if the month or date has not hit then they are not yet that age
              OR (MONTH(current_date()) = month_of_birth AND DAY(current_date()) < 1)
          THEN 1 
          ELSE 0 
      END AS Age
FROM specific_lsds l
LEFT JOIN person p ON p.person_id = l.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5ec35013-423e-47db-928d-bfa1b3e3aa70"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b"),
    specific_lsds=Input(rid="ri.foundry.main.dataset.d6021714-b1ff-4530-b6b9-56db702a6337")
)
WITH joined AS (SELECT DISTINCT condition_occurrence.*
FROM specific_lsds JOIN condition_occurrence ON specific_lsds.person_id = condition_occurrence.person_id)
SELECT *
FROM joined
-- grouped AS (
-- SELECT
--     person_id,
--     condition_concept_id,
--     condition_start_date,
--     ROW_NUMBER() OVER (
--         PARTITION BY person_id, condition_concept_id
--         ORDER BY condition_start_date DESC) AS rn -- select unique phenotypes. If there is a tie select the latest ones
-- FROM joined
-- WHERE condition_concept_id IS NOT NULL)

-- SELECT *
-- FROM grouped
-- WHERE rn = 1
-- 1,392,740 rows when this commented action is performed

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fc193f85-8de6-4c05-8f5d-613ba7931fc9"),
    Logic_Liaison_Covid_19_Patient_Summary_Facts_Table_De_identified_=Input(rid="ri.foundry.main.dataset.ae01d2c8-5c70-428f-a0aa-de30d587b2bb"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.fd499c1d-4b37-4cda-b94f-b7bf70a014da"),
    lysosomotropic_drugs=Input(rid="ri.foundry.main.dataset.2f63d266-bddb-4824-9fd0-6948c50f5e58")
)
-- shouldn't you group by person_id and drug_concept_name and make sure each person just has one drug and date?
-- right now a person could have taken a drug twice and both are in the table

WITH drugs AS (SELECT d.*
FROM drug_exposure d
JOIN lysosomotropic_drugs l ON d.drug_concept_id = l.concept_id),
cov AS (
SELECT
    c.person_id,
    datediff(e.drug_exposure_end_date,c.COVID_first_poslab_or_diagnosis_date) AS days_diff,
    c.COVID_first_poslab_or_diagnosis_date,
    e.drug_exposure_end_date,
    c.Severity_Type,
    e.drug_concept_name,
    CASE 
        WHEN Severity_Type = 'Mild_No_ED_or_Hosp_around_COVID_index' THEN 0 -- mild covid
        WHEN Severity_Type = 'Mild_ED_around_strong_signal_COVID_index' THEN 1 -- severe
        WHEN Severity_Type = 'Mild_ED_around_weak_signal_COVID_index' THEN 1
        WHEN Severity_Type = 'Moderate_Hosp_around_strong_signal_COVID_index' THEN 1
        WHEN Severity_Type = 'Moderate_Hosp_around_weak_signal_COVID_index' THEN 1
        WHEN Severity_Type = 'Death_within_n_days_after_COVID_index' THEN 1
        WHEN Severity_Type = 'Severe_ECMO_IMV_in_Hosp_around_weak_signal_COVID_index' THEN 1
        WHEN Severity_Type = 'Severe_ECMO_IMV_in_Hosp_around_strong_signal_COVID_index' THEN 1
        ELSE NULL
    END AS Binary_Severity,
    CAST(c.age_at_covid AS INT) AS age_at_covid,
    c.race,
    c.sex,
    c.data_partner_id
FROM drugs e
JOIN Logic_Liaison_Covid_19_Patient_Summary_Facts_Table_De_identified_ c ON e.person_id = c.person_id)

SELECT *
FROM cov
WHERE days_diff BETWEEN 0 AND 30
    AND age_at_covid IS NOT NULL
    AND person_id IS NOT NULL
    AND sex IS NOT NULL
    AND race IS NOT NULL
    AND Severity_Type IS NOT NULL
    AND data_partner_id IS NOT NULL

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2f63d266-bddb-4824-9fd0-6948c50f5e58"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT *
FROM concept_set_members
WHERE codeset_id = 439617420

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8b443c3f-7205-4da9-90f0-bb1bc933fb12"),
    loinc=Input(rid="ri.foundry.main.dataset.1548c5bb-369b-4ffb-a026-6a7691496bad")
)
SELECT concept_id,
    concept_name,
    concept_code,
    scale
FROM loinc
WHERE concept_code IN ('2345-7', -- comprehensive metabolic panel
    '3094-0',  
    '2160-0',
    '2951-2',
    '2823-3',
    '2075-0',
    '2028-9',
    '17861-6',
    '2885-2',
    '1751-7',
    '1975-2',
    '6768-6',
    '1920-8',
    '1742-6',
    '8462-4', -- Diastolic BP
    '8867-4', -- HR
    '9279-1', -- Respiratory rate
    '2708-6', -- Oxygen saturation spO2
    '8480-6', -- Systolic BP
    '8310-5', -- Body temp
    '11558-4', -- pH of blood
    '718-7', -- hemoglobin g/dL
    '6690-2', -- immune markers
    '789-8',
    '718-7',
    '4544-3',
    '787-2',
    '785-6',
    '786-4',
    '788-0',
    '777-3',
    '770-8',
    '736-9',
    '5905-5',
    '713-8',
    '706-2',
    '751-8',
    '731-0',
    '742-7',
    '711-2',
    '704-7',
    '71695-1',
    '53115-2',
    '58413-6',
    '92806-9',
    '55908-8', -- Alpha galactosidase A
    '62310-8', -- Galactosylceramidase
    '55917-9', -- Glucosylceramidase
    '55909-6', -- Alpha-L-iduronidase
    '62316-5', --	Acid sphingomyelinase',
    '55827-0', -- Acid alpha glucosidase
    '79462-8', -- Iduronate-2-Sulfatase
    '1988-5', -- C Reactive protein
    '2276-4', -- ferritin
    '42929-0', -- lactate dehydrogenase
    '26881-3', -- interleukin 6
    '75241-0', -- procalcitonin
    '10839-9', -- troponin
    '42727-8', -- Fibrin D-dimer
    '30934-4', -- 'B naturietic peptide'
    '33211-4', -- IL8
    '26848-2' -- IL10
    )
-- https://loinc.org/62300-9 LSDs (took quantitative terms from here)
-- took any other quantitative LSD terms from here https://loinc.org/105458-4
-- cmp is comprehensive metabolic panel composed of 14 overall results
-- https://www.ucsfhealth.org/medical-tests/comprehensive-metabolic-panel -- For reference ranges
-- LOINC codes from here https://www.labcorp.com/tests/322000/metabolic-panel-14-comprehensive

-- evidence to include interleukins
--https://pmc.ncbi.nlm.nih.gov/articles/PMC9079324/

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2cad9736-dede-4ad5-a0e8-332f08632605"),
    cohort=Input(rid="ri.foundry.main.dataset.228393db-ef06-40ad-be16-0239447aade8")
)
SELECT *
FROM cohort

@transform_pandas(
    Output(rid="ri.vector.main.execute.2bcfa845-995d-4a06-88be-87a24662b94a")
)
-- you need to include ICD9 codes too
-- WHEN l.condition_source_value LIKE '%I10:%' THEN 
--             SUBSTRING_INDEX(l.condition_source_value, 'I10:', -1) -- everything after
--         WHEN l.condition_source_value LIKE '%10:[A-Z]%' THEN 

-- UNION -- keeps all distinct matches. Doesn't work as intended

-- SELECT l.person_id,
--     l.condition_concept_id,
--     l.condition_concept_name,
--     l.condition_start_date,
--     l.condition_end_date,
--     l.condition_source_value,
--     l.condition_source_concept_id,
--     i.icd_10_concept_id,
--     i.icd_10_concept_name,
--     i.concept_code AS icd10_concept_code,
--     l.data_partner_id   
-- FROM lsds_conditions l
-- LEFT JOIN icd10_to_snomed i ON i.concept_code = l.condition_source_value -- Additional matches in the source value

-- condition_source_val AS ( -- exact matches to the source value
--     SELECT c.*,
--         i.concept_code,
--         i.vocabulary_id,
--         i.icd_concept_name
--     FROM condition_source c
--     LEFT JOIN icd_to_snomed i ON c.condition_source_value = i.icd_concept_id
-- ),

--WITH cohort_conditions AS (
--     SELECT 
--         ch.person_id,
--         co.condition_concept_id,
--         co.condition_concept_name,
--         co.condition_start_date,
--         co.condition_end_date,
--         co.condition_source_value,
--         co.condition_source_concept_id,
--         ch.has_lsd
--     FROM cohort ch 
--     JOIN condition_occurrence co ON ch.person_id = co.person_id
-- ),
-- condition_source_values AS (
--     SELECT 
--         c.*,
--         CASE 
--             WHEN c.condition_source_value LIKE '%ICD10CM:%' THEN 
--                 SUBSTRING_INDEX(c.condition_source_value, 'ICD10CM:', -1)
--             WHEN c.condition_source_value LIKE '%:ICD-10-CM%' THEN
--                 SUBSTRING_INDEX(c.condition_source_value, ':ICD-10-CM', 1)
--             WHEN c.condition_source_value LIKE '%|ICD-10-CM%' THEN
--                 SUBSTRING_INDEX(c.condition_source_value, '|ICD-10-CM', 1)
--             ELSE c.condition_source_value 
--         END AS extracted_code
--     FROM cohort_conditions c
-- ),
-- joined1 AS (
--     SELECT 
--         c.*,
--         i.concept_code,
--         i.vocabulary_id,
--         i.icd_concept_name
--     FROM condition_source_values c
--     LEFT JOIN icd_to_snomed i 
--         ON c.condition_source_concept_id = i.icd_concept_id 
-- ),
-- joined2 AS (
--     SELECT 
--         c.*,
--         i.concept_code,
--         i.vocabulary_id,
--         i.icd_concept_name
--     FROM condition_source_values c
--     LEFT JOIN icd_to_snomed i 
--         ON c.extracted_code = i.concept_code 
-- ),
-- joined3 AS (
--     SELECT 
--         c.*,
--         i.concept_code,
--         i.vocabulary_id,
--         i.icd_concept_name
--     FROM condition_source_values c
--     LEFT JOIN icd_to_snomed i 
--         ON c.condition_source_value = i.concept_code 
-- )

-- SELECT * 
-- FROM joined1
-- UNION
-- SELECT *
-- FROM joined2
-- UNION
-- SELECT *
-- FROM joined3;

-- -- WITH cohort_conditions AS (SELECT 
-- --     ch.person_id,
-- --     co.condition_concept_id,
-- --     co.condition_concept_name,
-- --     co.condition_start_date,
-- --     co.condition_end_date,
-- --     co.condition_source_value,
-- --     co.condition_source_concept_id,
-- --     ch.has_lsd
-- -- FROM cohort ch 
-- -- JOIN condition_occurrence co ON ch.person_id = co.person_id),
-- -- condition_source AS (
-- --     SELECT c.*,
-- --            i.concept_code,
-- --            i.vocabulary_id,
-- --            i.icd_concept_name
-- --     FROM cohort_conditions c
-- --     LEFT JOIN icd_to_snomed i 
-- --         ON c.condition_source_concept_id = i.icd_concept_id
-- --     UNION ALL
-- --     SELECT c.*,
-- --            i.concept_code,
-- --            i.vocabulary_id,
-- --            i.icd_concept_name
-- --     FROM cohort_conditions c
-- --     LEFT JOIN icd_to_snomed i 
-- --         ON c.condition_source_value = i.icd_concept_id),
-- -- string_match AS (
-- -- SELECT c.*,
-- --         CASE 
-- --             WHEN c.condition_source_value LIKE '%ICD10CM:%' THEN 
-- --                 SUBSTRING_INDEX(c.condition_source_value, 'ICD10CM:', -1)
-- --             WHEN c.condition_source_value LIKE '%:ICD-10-CM%' THEN
-- --                 SUBSTRING_INDEX(c.condition_source_value, ':ICD-10-CM', 1)
-- --             WHEN c.condition_source_value LIKE '%|ICD-10-CM%' THEN
-- --                 SUBSTRING_INDEX(c.condition_source_value, '|ICD-10-CM', 1)
-- --             ELSE NULL
-- --         END AS extracted_code
-- -- FROM condition_source c),
-- -- joined AS (
-- --     SELECT s.*,
-- --         i.concept_code,
-- --         i.vocabulary_id,
-- --         i.icd_concept_name
-- --     FROM string_match s
-- --     LEFT JOIN icd_to_snomed i ON s.extracted_code = i.concept_code)

-- -- SELECT * -- before I try any regexp funny business
-- -- FROM joined

-- -- string_match AS (
-- --     SELECT c.*,
--         i.concept_code,
--         i.vocabulary_id,
--         i.icd_concept_name
--     FROM condition_source_val
--     LEFT JOIN icd_to_snomed i ON c.condition_source_value REGEXP i.concept_code 
-- )
-- SELECT *
-- FROM string_match

@transform_pandas(
    Output(rid="ri.vector.main.execute.05f1153e-e5e6-4d75-bcd9-a6f8e836f6db")
)
-- WITH exact AS (
--     SELECT l.person_id,
--     l.condition_concept_id,
--     l.condition_concept_name,
--     l.condition_start_date,
--     l.condition_end_date,
--     l.condition_source_value,
--     l.condition_source_concept_id,
--     i.icd_concept_id,
--     i.vocabulary_id,
--     i.icd_concept_name,
--     i.concept_code, 
--     l.data_partner_id
-- FROM lsds_conditions l
-- LEFT JOIN 
--     icd_to_snomed i
--     ON i.icd_concept_id = l.condition_source_concept_id
-- )
--     SELECT l.person_id,
--     l.condition_concept_id,
--     l.condition_concept_name,
--     l.condition_start_date,
--     l.condition_end_date,
--     l.condition_source_value,
--     l.condition_source_concept_id,
--     i.icd_concept_id,
--     i.vocabulary_id,
--     i.icd_concept_name,
--     i.concept_code, 
--     l.data_partner_id
-- FROM exact l
-- LEFT JOIN 
--     icd_to_snomed i
-- ON 
--      l.condition_source_value = i.concept_code
-- -- -- combined AS ( -- combined 
-- -- --     SELECT * FROM exact
-- -- --     UNION ALL
-- -- --     SELECT * FROM source
-- -- -- ),
-- -- regex AS (
-- -- SELECT l.person_id,
-- --     l.condition_concept_id,
-- --     l.condition_concept_name,
-- --     l.condition_start_date,
-- --     l.condition_end_date,
-- --     l.condition_source_value,
-- --     l.condition_source_concept_id,
-- --     i.icd_concept_id,
-- --     i.vocabulary_id,
-- --     i.icd_concept_name,
-- --     i.concept_code, 
-- --     l.data_partner_id
-- -- FROM source l
-- -- LEFT JOIN 
-- --     icd_to_snomed i
-- -- ON 
-- --     l.condition_source_value REGEXP i.concept_code
-- -- WHERE l.concept_code IS NULL)

-- -- SELECT * 
-- -- FROM regex

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.adaf69b9-dd53-48f8-a9c4-32cdc222e228"),
    lsd_conditions_icd=Input(rid="ri.foundry.main.dataset.115d8d02-843c-4c0d-b03e-b1d2e24aad95"),
    phecode_map=Input(rid="ri.foundry.main.dataset.78b961ea-3fd8-4e0d-bdc0-d9805d8de1a4")
)
SELECT lsd_conditions_icd.person_id,
    phecode_map.*
FROM lsd_conditions_icd
JOIN phecode_map ON phecode_map.code = lsd_conditions_icd.combined_concept_code

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7459ad57-f5e2-49e5-a7ae-70aa11a8d3bd"),
    cohort_death=Input(rid="ri.foundry.main.dataset.e50e9f88-3eb1-4b9f-883d-90e096771660"),
    cohort_procedures=Input(rid="ri.foundry.main.dataset.41ee23d6-76f8-44ea-af1c-7677ec730a30")
)
WITH death AS (
    SELECT d.*, 
           co.death_date
    FROM cohort_procedures d
    LEFT JOIN cohort_death co ON co.person_id = d.person_id 
),
combined AS (SELECT 
    e.person_id,
    e.procedure_concept_id,
    e.procedure_concept_name,
    -- e.has_lsd,
    -- e.days_diff,
    -- e.Severity_Type,
    e.death_date,
    COALESCE(
        DATEDIFF(e.death_date, MAX(e.procedure_date)),
        DATEDIFF('2024-12-31', MAX(e.procedure_date)) -- end of analysis period
    ) AS days,
    MAX(e.procedure_date) AS procedure_date
FROM death e
GROUP BY 
    e.person_id,
    e.procedure_concept_id,
    -- e.has_lsd,
    -- e.days_diff,
    --e.Severity_Type,
    e.procedure_concept_name,
    e.death_date)
SELECT co.*,
    CASE 
        WHEN co.death_date IS NOT NULL THEN 1
        ELSE 0
    END AS status,
    CASE
        WHEN LOWER(procedure_concept_name) LIKE '%therapeutic, prophylactic, or diagnostic%' THEN 'therapeutic, prophylactic, or diagnostic injection'
        WHEN LOWER(procedure_concept_name) LIKE '%vaccination%' 
            OR LOWER(procedure_concept_name) LIKE '%immunization%' THEN 'immunization administration'
        WHEN LOWER(procedure_concept_name) LIKE '%continuous positive airway%' THEN 'continuous positive airway pressure ventilation'
        WHEN LOWER(procedure_concept_name) LIKE '%intravenous infusion, for therapy%' THEN 'intravenous infusion, for therapy or prophylaxis'
        WHEN LOWER(procedure_concept_name) LIKE '%intravenous infusion, hydration%' THEN 'intravenous infusion, hydration'
        WHEN LOWER(procedure_concept_name) LIKE '%insulin%' THEN 'administration of insulin'
        WHEN LOWER (procedure_concept_name) LIKE '%extracorporeal membrane oxygenation%' 
            OR LOWER (procedure_concept_name) LIKE '%ecls%'
            THEN 'extracorporeal membrane oxygenation treatment'
        WHEN LOWER (procedure_concept_name) LIKE '%assistance with respiratory ventilation%' THEN 'assistance with respiratory ventilation'
        WHEN LOWER (procedure_concept_name) LIKE '%inhalation treatment%' THEN 'inhalation treatment with aerosol or nebulizer'
        WHEN 
    ELSE 'Other'
    END AS procedure_name
FROM combined co
WHERE co.procedure_date IS NOT NULL
    AND co.days >= 0 

