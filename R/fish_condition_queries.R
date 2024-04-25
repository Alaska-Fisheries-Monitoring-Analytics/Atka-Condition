# Title:                      Fish condition as a fxn of length and weight from the fishery
# Authors:                    Andy Kingham, Jane Sullivan, MESA staff
# Contact:                    andy.kingham@noaa.gov, jane.sullivan@noaa.gov
# Create date:                20240423

# Object naming conventions:

#     a.  df_     = data.frame
#     b.  in_     = input parameter
#     c.  fn_     = function
#     d   obj_    = object  
#     e.  plot_   = plot

# Set up the environment ----

if(!require("keyring"))      install.packages("keyring",       repos='http://cran.us.r-project.org')
if(!require("odbc"))         install.packages("odbc",          repos='http://cran.us.r-project.org')
if(!require("rstudioapi"))   install.packages("rstudioapi",    repos='http://cran.us.r-project.org')
# if(!require("plyr"))         install.packages("plyr",          repos='http://cran.us.r-project.org')
if(!require("readr"))        install.packages("readr",         repos='http://cran.us.r-project.org')
if(!require("dplyr"))        install.packages("dplyr",         repos='http://cran.us.r-project.org')
if(!require("tidyr"))        install.packages("tidyr",         repos='http://cran.us.r-project.org')
if(!require("ggplot2"))      install.packages("ggplot2",       repos='http://cran.us.r-project.org')
# if(!require("scales"))       install.packages("scales",        repos='http://cran.us.r-project.org')
# if(!require("broom"))        install.packages("broom",         repos='http://cran.us.r-project.org')
# if(!require("lme4"))         install.packages("lme4",          repos='http://cran.us.r-project.org')
# if(!require("nlstools"))     install.packages("nlstools",      repos='http://cran.us.r-project.org')

# AFSC Connection ----

# Define odbc connection to the Oracle database 
# default connection schema = your schema, with ability to connect to NORPAC, OBSINT, and other schemas within AFSC database
# Change "AFSC" to whatever the TNS alias is, that is defined on your machine, in the 'ORACLE' directory.  
# Contact IT if assistance is needed with TNS alias!

db <- showPrompt(title = '',
                 message ="Database to query: ",
                 default = "AFSC")

obj_channel <- dbConnect(odbc(),
                         db,
                         UID    = keyring::key_list(db)$username,
                         PWD    = keyring::key_get(db, keyring::key_list(db)$username))

# AFSC look-up tables ----

# Species Codes Query 
# Peruse this as needed, to use for species selection.
df_spp_codes <-
  dbGetQuery(obj_channel, 
             "SELECT * FROM norpac.atl_lov_species_code"
  )

in_species <-
  as.numeric(showPrompt(title   = '',
                        message ="Enter the species_code for this analysis:",
                        default = "204")
  )

in_species_name <- df_spp_codes$COMMON_NAME[df_spp_codes$SPECIES_CODE == in_species]

# In-season average weights ----

# species in samples query: to get WEIGHT PER FISH from species comp samples.
# Adjust additional metadata haul-level columns as desired.  
# Additional columns here currently include:
#     • NMFS_REGION
#     • TARGET_FISHERY_CODE
#     • MANAGEMENT_PROGRAM_CODE

df_spp_fshry_samples <-
  dbGetQuery(obj_channel, 
             paste0(
               "WITH init AS
                --since we are ultimately joining across schemas, processing speed is optimized 
                --by doing this subquery on INDEXED columns within the schema first
                --(i.e, join the ATL_ tables first)
                
                  (SELECT  h.cruise, h.permit, h.haul_seq,
                           c.species_code, spp.common_name,
                           sum(c.species_number) AS species_number, 
                           sum(c.species_weight) AS species_weight
                      FROM norpac.atl_haul h 
                      JOIN norpac.atl_sample s 
                        ON s.cruise = h.cruise 
                       AND s.permit = h.permit AND s.haul_seq = h.haul_seq
                      JOIN norpac.atl_species_composition c
                        ON s.cruise = c.cruise 
                       AND s.permit = c.permit AND s.sample_seq = c.sample_seq
                      JOIN atl_lov_species_code spp 
                        ON spp.species_code = c.species_code
                     WHERE c.species_code = ",
                     in_species,
                     " AND (c.species_number > 0 AND c.species_weight > 0) 
                     GROUP BY h.cruise, h.permit, h.haul_seq,
                              c.species_code, spp.common_name  
                   )
                 
              SELECT ch.*, ah.management_program_code, ah.target_fishery_code,
                     tf.target_fishery_description, na.nmfs_region, na.general_region,
                     init.species_code, init.common_name,
                     init.species_number, init.species_weight
                FROM obsint.current_haul ch  
                JOIN init 
                  ON init.cruise = ch.cruise
                 AND init.permit = ch.permit AND init.haul_seq = ch.haul_seq
                LEFT JOIN norpac_views.akr_obs_haul ah
                  ON ah.haul_join = ch.haul_join
                LEFT JOIN loki.akr_target_fishery_codes tf
                  ON tf.target_fishery_code = ah.target_fishery_code
                LEFT JOIN norpac.atl_nmfs_area_v na
                  ON na.nmfs_area = ch.nmfs_area
              ")  
  )
df_spp_fshry_samples %>% 
  rename_all(tolower) %>% 
  readr::write_csv('data/inseason_avg_wts.csv')

# In-season weight-length pairs -----

# Fishery Length/weight samples: to get INDIVIDUAL fish length/weights from specimen samples.
# NOTE: double-UNION ALL-query ensures we get lengths recorded at both the HAUL and SAMPLE levels.
# Adjust additional metadata haul-level columns as desired.  
# Additional columns here currently include:
#     • NMFS_REGION
#     • TARGET_FISHERY_CODE
#     • MANAGEMENT_PROGRAM_CODE

df_spp_fshry_length_wts <-
  dbGetQuery(obj_channel, 
             paste0("
             WITH init AS (
                --haul level specimen lengths and weights
                SELECT DISTINCT h.cruise, h.permit, h.haul_seq, l.length_seq, 
                       l.species_code, spp.common_name,
                       l.length_size as length, sp.weight
                  FROM norpac.atl_haul h
                  JOIN norpac.atl_length l 
                    ON h.cruise = l.cruise AND h.permit = l.permit AND h.haul_seq = l.haul_seq
                  JOIN norpac.atl_fish_inv_specimen sp
                    ON sp.cruise = l.cruise AND sp.permit = l.permit AND sp.length_seq = l.length_seq
                  JOIN atl_lov_species_code spp 
                    ON spp.species_code = l.species_code
                 WHERE specimen_type  = 3  --weight specimen types only.
                   AND l.species_code = ", 
                in_species,
               "UNION ALL 
                --species_composition level specimen lengths and weights
                SELECT DISTINCT h.cruise, h.permit, h.haul_seq, l.length_seq, 
                       c.species_code, spp.common_name,
                       l.length_size as length, sp.weight
                  FROM norpac.atl_haul h
                  JOIN norpac.atl_sample s
                    ON h.cruise = s.cruise AND h.permit = s.permit AND h.haul_seq = s.haul_seq
                  JOIN norpac.atl_species_composition c
                    ON s.cruise = c.cruise AND s.permit = c.permit AND s.sample_seq = c.sample_seq
                  JOIN norpac.atl_length l
                    ON c.cruise = l.cruise AND c.permit = l.permit AND c.species_composition_seq = l.species_composition_seq
                  JOIN norpac.atl_fish_inv_specimen sp
                    ON sp.cruise = l.cruise AND sp.permit = l.permit AND sp.length_seq = l.length_seq
                  JOIN atl_lov_species_code spp 
                    ON spp.species_code = c.species_code
                 WHERE specimen_type = 3  --weight specimen types only. 
                   AND c.species_code = ",
                in_species, 
              ")
                    
             SELECT ch.*, ah.management_program_code, ah.target_fishery_code,
                    tf.target_fishery_description, na.nmfs_region, na.general_region,
                    init.species_code, init.common_name,
                    init.length, init.weight
               FROM obsint.current_haul ch  
               JOIN init 
                 ON init.cruise = ch.cruise AND init.permit = ch.permit AND init.haul_seq = ch.haul_seq
               LEFT JOIN norpac_views.akr_obs_haul ah
                 ON ah.haul_join = ch.haul_join
               LEFT JOIN loki.akr_target_fishery_codes tf
                 ON tf.target_fishery_code = ah.target_fishery_code
               LEFT JOIN norpac.atl_nmfs_area_v na
                 ON na.nmfs_area = ch.nmfs_area   "
             ))

df_spp_fshry_length_wts %>% 
  rename_all(tolower) %>% 
  readr::write_csv('data/inseason_wtlen.csv')

# AKFIN connection ----

# Connect to AKFIN for debriefed data



# Debriefed average weights ----


# Debriefed weight-length pairs ----


#########################
# Boxplot function definition---------------------------------------------------

fn_boxplot <- 
  function(in_df, in_y_lab) { 
    ggplot(data = in_df,
           aes(x = as.factor(YEAR),
               y = Y_VAL) ) +
      geom_boxplot() +
      labs(x     = '',
           y     = in_y_lab,
           title = in_species_name) +
      theme_bw() + 
      theme(strip.text.y.right = element_text(angle = 0))
  }





#######################
# Boxplots for Weight Per Fish from species comp samples -----------------------

# 1. For all data
plot_bx_wt_per_fish_all <- (fn_boxplot(in_df    = df_spp_fshry_samples %>%
                                                   mutate(Y_VAL = SPECIES_WEIGHT/SPECIES_NUMBER),
                                       in_y_lab = 'Weight per fish (kg)') )
plot_bx_wt_per_fish_all


# 2. Faceted by "broad" region
plot_bx_wt_per_fish_broad_region <-
  (fn_boxplot(in_df = df_spp_fshry_samples %>%
                mutate(Y_VAL = SPECIES_WEIGHT/SPECIES_NUMBER),
              in_y_lab = 'Weight per fish (kg)')  +
   facet_grid(GENERAL_REGION ~.)
   ) 
plot_bx_wt_per_fish_broad_region



# 3. Faceted by NMFS region
plot_bx_wt_per_fish_NMFS_region <-
  (fn_boxplot(in_df = df_spp_fshry_samples %>%
                mutate(Y_VAL = SPECIES_WEIGHT/SPECIES_NUMBER),
              in_y_lab = 'Weight per fish (kg)')  +
     facet_grid(NMFS_REGION ~.)
  ) 
plot_bx_wt_per_fish_NMFS_region



# 4. Faceted by Target Fishery
plot_bx_wt_per_tgt_fshry <-
  (fn_boxplot(in_df = df_spp_fshry_samples %>%
                mutate(Y_VAL = SPECIES_WEIGHT/SPECIES_NUMBER),
              in_y_lab = 'Weight per fish (kg)')  +
     facet_grid(TARGET_FISHERY_DESCRIPTION ~.)
  )  
plot_bx_wt_per_tgt_fshry



# 5. Faceted by Management Program
plot_bx_wt_per_mgmt_pgm <-
  (fn_boxplot(in_df = df_spp_fshry_samples %>%
                mutate(Y_VAL = SPECIES_WEIGHT/SPECIES_NUMBER),
              in_y_lab = 'Weight per fish (kg)')  +
     facet_grid(MANAGEMENT_PROGRAM_CODE ~.)
  ) 
plot_bx_wt_per_mgmt_pgm

# debriefed specimen data ----

# flat version


debriefed_specimen <- 
  dbGetQuery(obj_channel, 
                paste0(
                "select   year, nmfs_area, gear, species,
                          length, weight, age, maturity_code, maturity_description,
                          sex, bottom_depth_fathoms, haul_offload_date,
                          performance
                 from     norpac.debriefed_age_flat_mv
                 where    species in '204'"
                ))

# NMFS area look up 
query <- "select  distinct  fmp_area as fmp, fmp_subarea, 
                            reporting_area_code as nmfs_area
          from              council.comprehensive_blend_ca"

nmfs_area_lookup <- sqlQuery(channel_akfin, query) %>% rename_all(tolower)
nmfs_area_lookup <- nmfs_area_lookup[complete.cases(nmfs_area_lookup),]
fsh %>% 
  rename_all(tolower) %>%
  mutate(nmfs_area = as.character(nmfs_area)) %>% 
  left_join(nmfs_area_lookup)  %>% 
  write_csv(paste0(dat_path, "/atka_debriefed_conf.csv"))
###












#######################
# TODO:  Length/weight regression plots, residuals, anomaly analysis, etc-------
# from length/weight specimen samples

