

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.601f62a4-dcba-4ebd-9cc7-dfad85bd8f0c"),
    lsd_conditions_icd=Input(rid="ri.foundry.main.dataset.115d8d02-843c-4c0d-b03e-b1d2e24aad95")
)
library(comorbidity)
library(dplyr)
cci <- function(lsd_conditions_icd) {

    df <- lsd_conditions_icd %>%
        filter(vocabulary_id == "ICD10CM") %>%
        arrange(desc(person_id))

    charlson <- comorbidity(x = df, id = "person_id", code = "combined_concept_code", map = "charlson_icd10_quan", assign0 = FALSE)
    elix <- comorbidity(x = df, id = "person_id", code = "combined_concept_code", map = "elixhauser_icd10_quan", assign0 = FALSE)

    cci_score <- score(x = charlson, weights = "quan", assign0 = FALSE) # quan or charlson weighted score

    elix_score <- score(x = elix, weights = "swiss", assign0 = FALSE) # swiss or wv

    elix$score <- elix_score

    charlson$score <- cci_score
    
    # charlson_df <- charlson %>%
    #     mutate(total = rowSums(across(where(is.numeric))))

    return(charlson)

    # https://ellessenne.github.io/comorbidity/reference/index.html
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.39905952-41c9-4d9f-96b0-4f183f2d727d"),
    cci=Input(rid="ri.foundry.main.dataset.601f62a4-dcba-4ebd-9cc7-dfad85bd8f0c")
)
library(ggplot2)
cci_plot <- function(cci) {
    
    df <- cci

    p <- ggplot(data = df) + 
        geom_bar(aes(x = score)) + 
        labs(x = "Quan-Charlson Comorbidity Index", y = "Count") + 
        theme_minimal() 

    plot(p)

    # k <- ggplot() + 
    #     geom_bar(aes(x = ))

    return(df)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9e03d97d-e500-4341-addf-46dd7a2a1b83"),
    cohort_conditions_simple=Input(rid="ri.foundry.main.dataset.7bde3f0f-e4da-4120-9019-9f1175cd7be0")
)
library(comorbidity)
library(dplyr)
library(ggplot2)
cohort_cci <- function(cohort_conditions_simple) { 

    df <- cohort_conditions_simple %>%
        filter(vocabulary_id == "ICD10CM") %>%
        arrange(desc(person_id))

    charlson <- comorbidity(x = df, id = "person_id", code = "concept_code", map = "charlson_icd10_quan", assign0 = FALSE)
    elix <- comorbidity(x = df, id = "person_id", code = "concept_code", map = "elixhauser_icd10_quan", assign0 = FALSE)

    cci_score <- score(x = charlson, weights = "quan", assign0 = FALSE) # quan or charlson weighted score

    elix_score <- score(x = elix, weights = "swiss", assign0 = FALSE) # swiss or wv

    elix$score <- elix_score

    charlson$score <- cci_score
    
    charlson <- charlson %>%
        left_join(df, by = "person_id") %>% # join back in has_lsd
        select(person_id, score, has_lsd) %>%
        distinct() # should fix one to many matching problems

    p <- ggplot(charlson, aes(x = score, fill = factor(has_lsd))) + 
        geom_bar(position = "dodge") + 
        labs(x = "Quan-Charlson Comorbidity Index", y = "Count", fill = "Has LSD") + 
        # scale_fill_manual(
        # values = c("0" = "blue", "1" = "red"), 
        # labels = c("Without LSD", "With LSD")
        # ) + 
        theme_minimal()
    
    plot(p)

    return(charlson)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1b418f38-b1b8-4857-827c-6aef6c1c7512"),
    cohort_match_prep=Input(rid="ri.foundry.main.dataset.4ea455cb-97a6-4ae0-957a-d9b258032533")
)
library(MatchIt)
library(dplyr)
cohort_match <- function(cohort_match_prep) {
    df1 <- cohort_match_prep

    df2 <- cohort_match_prep %>%
        mutate( # Convert columns to data types suitable for cohort matching
            Race = as.factor(Race),
            Age = as.numeric(Age),
            data_partner_id = as.numeric(data_partner_id),
            has_lsd = as.factor(has_lsd),
            Sex = as.factor(Sex)
        )

    # Set seed
    set.seed(123)

    # # Exact match on data_partner_id, nearest neighbors on other covariates
    ## Algorithm going very slow so here is a workaround
    
    df2$myfit <- fitted(glm(has_lsd ~ Age + Race + Sex, data = df2, family = "binomial")) # Distance between persons based on their demographics

    matched <- matchit(has_lsd ~ person_id, data = df2, method = "nearest", distance = df2$myfit, exact = c("data_partner_id")) # Exact match on data_partner

    matched_data <- match.data(matched, data = df2) # Extract all data from matched dataset 

    # Get person ids
    person_ids <- matched_data %>% select(person_id) %>% distinct()

    # Join back to Logic Liaison template
    cohort <- inner_join(df1, person_ids)

    return(cohort)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1cfdb695-6fbe-4219-ad56-e2fa963720ca"),
    cohort=Input(rid="ri.foundry.main.dataset.228393db-ef06-40ad-be16-0239447aade8"),
    cohort_measurements_ml=Input(rid="ri.foundry.main.dataset.9148c50d-1263-4446-9781-5c5b249f993f"),
    ml_measurements=Input(rid="ri.foundry.main.dataset.8b443c3f-7205-4da9-90f0-bb1bc933fb12")
)
library(tidyr)
library(dplyr)
cohort_measurement_pivot <- function(cohort, ml_measurements, cohort_measurements_ml) {

    ml <- ml_measurements
    
    cml <- cohort_measurements_ml %>% # Get all measurements within 2 days of COVID diagnosis
        mutate(
            measurement_date = as.Date(measurement_date)
        ) %>%
        select(person_id, measurement_concept_id, measurement_concept_name, harmonized_value_as_number, units, measurement_date) # select columns of interest

    unique_persons <- unique(cohort$person_id) # everyone in the cohort

    all_measurements <- expand.grid( # make grid of all people in the cohort for future imputation
        person_id = unique_persons,
        measurement_concept_id = unique(cml$measurement_concept_id) # 26 concepts
    )

    df <- left_join(all_measurements, cml, by = c("person_id", "measurement_concept_id")) # keep entire grid and fill in with existing data

    worst_value <- df %>% # calculate worst value in first X days
         group_by(person_id, measurement_concept_id) %>%
         summarize(worst_value_as_number = ifelse(all(is.na(harmonized_value_as_number)), 
                                  NA, max(harmonized_value_as_number, na.rm = TRUE))) # add NA if there is no max in a group
    
    with_names <- worst_value %>% # add measurement names before pivoting
        left_join(ml, by = join_by("measurement_concept_id" == "concept_id")) %>%
        select(person_id, concept_name, worst_value_as_number)

    # Pivot the data
    pivoted <- with_names %>% 
        pivot_wider(
        names_from = concept_name, 
        values_from = worst_value_as_number
        )
    
    colnames(pivoted) <- make.names(colnames(pivoted)) # syntactically valid names

    print(colMeans(is.na(pivoted)))
    
    # Fill null values with column medians
    for (col in names(pivoted)) {
        if (is.numeric(pivoted[[col]])) {
            pivoted[[col]][is.na(pivoted[[col]])] <- median(pivoted[[col]], na.rm = TRUE)
        }
    }

    return(pivoted)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.376a6936-153e-4aea-bb9a-19291d51b15e"),
    cohort_death=Input(rid="ri.foundry.main.dataset.e50e9f88-3eb1-4b9f-883d-90e096771660")
)
library(ggplot2)
library(dplyr)
death_cov_severity <- function(cohort_death) {
    
    df <- cohort_death %>%
        mutate(Binary_Severity = case_when(
            Severity_Type == 'Mild_No_ED_or_Hosp_around_COVID_index' ~ 'Not Hospitalized',
            TRUE ~ 'Hospitalized'
        ))

    p <- ggplot(data = df) +
        geom_density(aes(x = days_diff, fill = Binary_Severity, color = Binary_Severity), alpha = 0.5) + 
        labs(x = "Death Days After COVID-19", y = "Density") + 
        theme_minimal() 

    plot(p)

    return(df)

}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d7f0dddd-1ec7-41e9-9497-fc5c7e420e3b"),
    drug_exposure_survival_prep=Input(rid="ri.foundry.main.dataset.ffc7b3b2-038c-462f-a535-6ea666c9b6b1")
)
library(ggsurvfit) # Survival
library(dplyr) # Data wrangling
library(ggplot2) # Plotting
library(broom) # dataframe from Surv object
drug_exposure_survival <- function(drug_exposure_survival_prep) {
    # Drug exposure to FDA approved drugs. Categorize into five drugs dexamethasone, paxlovid, remdesivir, baricitinib, tocilizumab. All are approved for use for covid, although dexamethasone is an anti-inflammatory drug and others are specifically for COVID
    df <- drug_exposure_survival_prep %>% 
        select(days, status, drug_name) %>%
        mutate(
            drug_name = as.factor(drug_name),
            days = as.numeric(days),
            status = as.numeric(status)
        ) %>%
        filter(drug_name != "baricitinib" & drug_name != "tocilizumab" & drug_name != "prednisolone") %>%
        distinct()

    km_fit <- survfit2(Surv(days, status) ~ drug_name, data = df)

    km_df <- tidy(km_fit) %>%
        mutate(strata = gsub("drug_name=", "", strata))
        

    # Cox proportional hazards model
    # cox_model <- survfit(coxph(Surv(days, status) ~ strata(drug_name), data = df)) # need survival package

    # # Print summaries
    # # print(summary(cox_model))
    # print(names(km_fit))
    # print(names(cox_model))
    p <- km_fit |> 
        ggsurvfit(linewidth = 1) +
        add_confidence_interval() +
        add_censor_mark() +
        add_risktable() +
        coord_cartesian(xlim = c(0, 100), ylim = c(0.85, 1)) + 
        scale_ggsurvfit()
    
    plot(p)

    # p <- ggplot(data = km_df, aes(x = time, y = estimate, fill = strata)) + 
    #     geom_line(aes(color = strata)) +           
    #     geom_ribbon(aes(ymin = conf.low, ymax = conf.high), alpha = 0.2) +
    #     labs(
    #         title = "Kaplan-Meier Survival Curve",
    #         x = "Time",
    #         y = "Survival Probability",
    #         color = "Drug"
    #     ) +
    #     guides(fill="none") + 
    #     theme_minimal() #+ 
    #     #facet_wrap(vars(has_lsd))
    # plot(p)

    return(km_df)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5f9a7f4b-1bde-436b-9891-b326b99ab483"),
    lysosomotropic_drug_exposure=Input(rid="ri.foundry.main.dataset.fc193f85-8de6-4c05-8f5d-613ba7931fc9")
)
library(dplyr)
library(broom)
library(ggplot2)
drug_severity_interaction <- function(lysosomotropic_drug_exposure) {
    df <- lysosomotropic_drug_exposure

    train <- df %>%
        select(Binary_Severity, drug_concept_name, age_at_covid, race, sex) %>%
        group_by(drug_concept_name) %>%
        filter(n() > 500) %>% # at least 500 individuals with this exposure, when including low counts the estimates are bad
        ungroup()

    m1 <- glm(Binary_Severity ~ .,
                data = train, family = "binomial")

    model_df <- tidy(m1, conf.int = T)

    return(model_df)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b2c8d292-1866-4186-9851-a136eaa750f3"),
    procedure_survival_prep=Input(rid="ri.foundry.main.dataset.7459ad57-f5e2-49e5-a7ae-70aa11a8d3bd")
)
#library(survival)
library(ggsurvfit)
library(dplyr)
library(ggplot2)
library(broom)
procedure_survival <- function(procedure_survival_prep) {
    #procedures <- c(2314035, 2314026, 2314213, 2213418, 2314204, 4024656) # Procedures of interest
    
    df <- procedure_survival_prep %>% 
        #filter(procedure_concept_id %in% procedures) %>%
        filter(procedure_name != "Other") %>%
        select(days, status, procedure_name) %>%
        mutate(
            procedure_name = as.factor(procedure_name),
            days = as.numeric(days),
            status = as.numeric(status)
        ) %>%
        filter(procedure_name != "assistance with respiratory ventilation" & procedure_name !=
            "extracorporeal membrane oxygenation treatment" & procedure_name != "immunization administration") %>%
        distinct()

    km_fit <- survfit2(Surv(days, status) ~ procedure_name, data = df)

    # Cox proportional hazards model
    # cox_model <- survfit(coxph(Surv(days, status) ~ strata(procedure_name), data = df))

    # Convert to km dataframe
    km_df <- tidy(km_fit) %>%
        mutate(strata = gsub("procedure_name=", "", strata)) 

     p <- km_fit |> 
        ggsurvfit(linewidth = 1) +
        add_confidence_interval() +
        add_censor_mark() +
        add_risktable() +
        coord_cartesian(xlim = c(0, 100), ylim = c(0, 1)) + 
        scale_ggsurvfit()
    
    plot(p)

    # p <- ggplot(data = km_df, aes(x = time, y = estimate, fill = strata)) + 
    #     geom_line(aes(color = strata)) +           
    #     geom_ribbon(aes(ymin = conf.low, ymax = conf.high), alpha = 0.2) +
    #     labs(
    #         title = "Kaplan-Meier Survival Curve",
    #         x = "Time",
    #         y = "Survival Probability",
    #         color = "Procedure"
    #     ) +
    #     guides(fill="none") + 
    #     coord_cartesian(xlim = c(0,500)) + 
    #     theme_minimal() + 
    #     theme(legend.position = "right")
    # plot(p)

    return(km_df)

#     Specimen collection for Severe acute respiratory syndrome coronavirus 2 (SARS-CoV-2)
# Therapeutic, prophylactic, or diagnostic injection (specify substance or drug); each additional sequential intravenous push of a new substance/drug (List separately in addition to code for primary procedure)
# Intravenous infusion, for therapy, prophylaxis, or diagnosis (specify substance or drug); initial, up to 1 hour
# Pressurized or nonpressurized inhalation treatment for acute airway obstruction for therapeutic purposes and/or for diagnostic purposes such as sputum induction with an aerosol generator, nebulizer, metered dose inhaler or intermittent positive pressure breathing (IPPB) device
# Intravenous infusion, hydration; each additional hour (List separately in addition to code for primary procedure)
# Intravenous infusion, for therapy, prophylaxis, or diagnosis (specify substance or drug); each additional hour (List separately in addition to code for primary procedure)
# Continuous positive airway pressure ventilation (CPAP), initiation and management
# Intravenous infusion, hydration; initial, 31 minutes to 1 hour
# Administration of insulin
# Intravenous infusion, for therapy, prophylaxis, or diagnosis (specify substance or drug); additional sequential infusion of a new drug/substance, up to 1 hour (List separately in addition to code for primary procedure)
# Ventilator care management
# Assistance with Respiratory Ventilation, Less than 24 Consecutive Hours, Continuous Positive Airway Pressure
# Respiratory Ventilation, Greater than 96 Consecutive Hours
# Immunization administration (includes percutaneous, intradermal, subcutaneous, or intramuscular injections); 1 vaccine (single or combination vaccine/toxoid)
# Assistance with Respiratory Ventilation, 24-96 Consecutive Hours, Continuous Positive Airway Pressure
# Continuous inhalation treatment with aerosol medication for acute airway obstruction; first hour
# Assistance with Respiratory Ventilation, Greater than 96 Consecutive Hours, Continuous Positive Airway Pressure
# Continuous invasive mechanical ventilation of unspecified duration
# Continuous positive airway pressure ventilation treatment
# Intravenous infusion, hydration; each additional hour (List separately in addition to code for primary procedure)
# Intravenous infusion, hydration; initial, 31 minutes to 1 hour
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.eb616cda-4d79-4572-9bbc-96191d762f11"),
    drug_severity_interaction=Input(rid="ri.foundry.main.dataset.5f9a7f4b-1bde-436b-9891-b326b99ab483")
)
library(dplyr)
library(ggplot2)
protective_drug_summary <- function(drug_severity_interaction) {
    
    df <- drug_severity_interaction %>%
        mutate( # normalize to log-odds
            odds_ratio = exp(estimate),
            top_ci = exp(conf.high),
            bottom_ci = exp(conf.low)
        )

    p <- ggplot(data = df, aes(y = reorder(term, odds_ratio), x = odds_ratio)) + 
        geom_point(size = 2, color = term)) + 
        geom_errorbar(aes(xmin = bottom_ci, xmax = top_ci), linewidth = 1, width = 0.5, color = term) + 
        #geom_text(aes(label = pstars), size = 9/.pt, hjust = 0.5, vjust = -0.25, color = term) +
        geom_vline(xintercept = 1, linetype = "dashed", color = "black", linewidth = 1, alpha = 0.7) +
        theme_gray()

    plot(p)
    
    return(df)

}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d6021714-b1ff-4530-b6b9-56db702a6337"),
    all_lsds=Input(rid="ri.foundry.main.dataset.a14e4e39-499b-4e43-8607-95b3d65ec838"),
    lsd_concept_relationship=Input(rid="ri.foundry.main.dataset.c9d370c7-6e0c-4820-aa13-48985c85f22f")
)
library(dplyr)
specific_lsds <- function(all_lsds, lsd_concept_relationship) {
   
    concepts <- lsd_concept_relationship
    patients <- all_lsds

    concepts <- concepts %>%
        filter(ancestor_concept_id == 4053270 | ancestor_concept_id == 37155637) # Keep only Disorder of lysosomal enzyme: 4053270 and Lysosomal storage disease: 37155637

    conditions <- concepts %>%
        inner_join(patients, by = join_by(descendant_concept_id == condition_concept_id), keep = TRUE) %>%
        group_by(person_id) %>% 
        slice_max(max_levels_of_separation, with_ties = TRUE) %>%  # Select most specific diagnosis based on graph structure (distance from root parent term)
        slice_max(condition_start_date, with_ties = FALSE) %>% # only select max (most recent) date if there are ties in the graph structure
        select(c("person_id","condition_concept_id","condition_source_concept_id","condition_source_value","descendant_name","condition_start_date","condition_end_date","data_partner_id")) %>%
        rename(specific_condition_name = descendant_name)
    
    
    # filter(n_distinct(descendant_name) > 1) %>% add affter group_by(person_id) to find the people with more than one, or more conditions

    return(conditions)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.7e90edec-3a00-4769-af6d-2d896c99f2e7"),
    cohort_phecodes=Input(rid="ri.foundry.main.dataset.3eea9aef-4915-4257-9fae-524ffb1cc7b5")
)
unnamed <- function(cohort_phecodes) {
    
}

