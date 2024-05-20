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
db <- "akfin"
obj_channel <- dbConnect(odbc(),
                         db,
                         UID    = keyring::key_list(db)$username,
                         PWD    = keyring::key_get(db, keyring::key_list(db)$username))

# Debriefed average weights ----

# de-prioritized

# Debriefed weight-length pairs ----

# flat version
debriefed_specimen <- 
  dbGetQuery(obj_channel, 
             paste0(
               "select   year, haul_join, cruise, permit, nmfs_area, gear, species,
                          length, weight, age, maturity_code, maturity_description,
                          sex, bottom_depth_fathoms, 
                          performance, londd_start, latdd_start,
                          haul_offload_date, deployment_trip_start_date
                 from     norpac.debriefed_age_flat_mv
                 where    species in '204' and 
                          nmfs_area in ('541', '542', '543', '518') and
                          weight > 0
                    "
             ))
debriefed_specimen %>% filter(!is.na(LONDD_START)) %>% select(LONDD_START, LATDD_START)
ins %>% select(deployment_longitude, deployment_longitude_converted, e_w)
sort(names(ins))
cn[grepl('target',cn)]
tbl(obj_channel,sql('norpac.debriefed_age_flat_mv')) %>% rename_all(tolower) %>% colnames ->cn
debriefed_specimen %>% 
  rename_all(tolower) %>% 
  readr::write_csv('data/debriefed_wtlen.csv')

# NMFS area look up 
# query <- "select  distinct  fmp_area as fmp, fmp_subarea, 
#                             reporting_area_code as nmfs_area
#           from              council.comprehensive_blend_ca"
# 
# nmfs_area_lookup <- dbGetQuery(obj_channel, query) %>% rename_all(tolower)
# nmfs_area_lookup <- nmfs_area_lookup[complete.cases(nmfs_area_lookup),]
# fsh %>% 
#   rename_all(tolower) %>%
#   mutate(nmfs_area = as.character(nmfs_area)) %>% 
#   left_join(nmfs_area_lookup)  %>% 
  
# Condition analysis -----

rm(list=ls())

ins <- read_csv('data/inseason_wtlen.csv', guess_max = 1500)
deb <- read_csv('data/debriefed_wtlen.csv')
colnames(ins)[grepl('weig', colnames(ins))]
table(ins$management_program_code)
table(ins$target_fishery_description)
table(ins$nmfs_area)

# processor size grade categories
# 2L:  850-999 g
# L:  750-849 g
# M:  650-749 g
# S:  550-649 g
# 2S:  450-549 g *
# 3S:  360-449 g *
# 4S:  300-359 g *
# 5S:  240-299 g *
# 6S:  180-239 g
# 7S:  110-179 g

df <- ins %>% 
  filter(management_program_code == "A80" &
           target_fishery_description == "Atka mackerel" &
           nmfs_area %in% c(541, 542, 543) &
           year > 2010) %>% 
  mutate(
    # # detailed by grade
    # sizegrade = case_when(weight >= 0.45 ~ "2S+ (>=450 g)",
    #                       weight < 0.45 & weight >= 0.36 ~ "3S (360-449 g)",
    #                       weight < 0.36 & weight >= 0.30 ~ "4S (300-359 g)",
    #                       weight <= 0.29 ~ "5S- (<= 299 g)"),
    # # sm med lg
    # sizegrade = case_when(weight >= 0.45 ~ "2S+ (>=450 g)",
    #                       weight < 0.45 & weight >= 0.30 ~ "3S and 4S (300-449 g)",
    #                       weight <= 0.29 ~ "5S- (<= 299 g)"),
    # sm lg
    sizegrade = case_when(weight >= 0.45 ~ "2S+ (>=450 g)",
                          weight < 0.45 ~ "3S- (<450 g)"),
         condition_stratum = NA, area_biomass = 1,
         julian = lubridate::yday(haul_date),
         common_name = paste0("Atka in area ", nmfs_area, ": ", sizegrade),
         species_code = as.integer(as.factor(common_name)))

(maxday <- df %>% 
    filter(year == 2024 & haul_date == max(haul_date)) %>%
    distinct(julian, haul_date) %>% 
    pull(julian))

# Make sure all fish are from a comparable time period
dfs <- df %>% filter(julian <= maxday) # %>% nrow

# check sample sizes, set minimum threshold
dfs %>% 
  dplyr::count(species_code, nmfs_area,year) %>% 
    tidyr::pivot_wider(id_cols = year, names_from = species_code, values_from = n, values_fill = 0)# %>% 
    # View()

minn <- 25
dfs <- dfs %>% group_by(common_name, year) %>% filter(n() >= minn) %>% ungroup()

ggplot(dfs, aes(x = log(length), y = log(weight), col = factor(year))) +
  geom_point() + 
  geom_smooth(method = 'lm') + 
  facet_wrap(~common_name, ncol = 2) 

(opts <- unique(dfs$species_code))

# Calculate length weight residuals -----
for(i in 1:length(opts)) {
  
  # Separate slope for each stratum. Bias correction according to Brodziak, no outlier detection.
  atka_df <- akfishcondition::calc_lw_residuals(len = dfs$length[dfs$species_code == opts[i]], 
                                wt = dfs$weight[dfs$species_code == opts[i]], 
                                year = dfs$year[dfs$species_code == opts[i]], 
                                stratum = dfs$condition_stratum[dfs$species_code == opts[i]], 
                                make_diagnostics = TRUE, # Make diagnostics
                                bias.correction = TRUE, # Bias correction turned on
                                outlier.rm = FALSE, # Outlier removal turned on
                                region = "AI", 
                                species_code = dfs$species_code[dfs$species_code == opts[i]],
                                include_ci = TRUE)
  dfs$resid_mean[dfs$species_code == opts[i]] <- atka_df$lw.res_mean
  dfs$resid_lwr[dfs$species_code == opts[i]] <- atka_df$lw.res_lwr
  dfs$resid_upr[dfs$species_code == opts[i]] <- atka_df$lw.res_upr
  
}

# Estimate mean and std. err for each stratum, filter out strata with less than 10 samples
atka_resids <- dfs %>% 
  dplyr::group_by(common_name, species_code, year, condition_stratum, area_biomass) %>% 
  dplyr::summarise(stratum_resid_mean = mean(resid_mean),
                   stratum_resid_sd = sd(resid_mean),
                   n = n()) %>%
  dplyr::filter(n >= 10) %>%
  dplyr::mutate(stratum_resid_se = stratum_resid_sd/sqrt(n))

# Weight strata by biomass 
for(i in 1:length(opts)) {
  
  atka_resids$weighted_resid_mean[atka_resids$species_code == opts[i]] <- akfishcondition::weight_lw_residuals(
    residuals = atka_resids$stratum_resid_mean[atka_resids$species_code == opts[i]],
    year = atka_resids$year[atka_resids$species_code == opts[i]],
    # stratum = rep("no_strata", nrow(atka_resids)),
    stratum = atka_resids$condition_stratum[atka_resids$species_code == opts[i]],
    stratum_biomass = atka_resids$area_biomass[atka_resids$species_code == opts[i]])
  
  atka_resids$weighted_resid_se[atka_resids$species_code == opts[i]] <- akfishcondition::weight_lw_residuals(
    residuals = atka_resids$stratum_resid_se[atka_resids$species_code == opts[i]],
    year = atka_resids$year[atka_resids$species_code == opts[i]],
    # stratum = rep("no_strata", nrow(atka_resids)),
    stratum = atka_resids$condition_stratum[atka_resids$species_code == opts[i]],
    stratum_biomass = atka_resids$area_biomass[atka_resids$species_code == opts[i]])
}

# Biomass-weighted residual and SE by year
atka_ann_mean_resid_df <- atka_resids %>% 
  dplyr::group_by(year, common_name) %>%
  dplyr::summarise(mean_wt_resid = mean(weighted_resid_mean),
                   se_wt_resid = mean(weighted_resid_se))

# Plot

atka_ann_mean_resid_df %>% 
  filter(grepl("2S+", common_name)) %>% 
  ggplot() + 
  geom_bar(aes(x = year, 
               y = mean_wt_resid), 
           stat = "identity", 
           fill = "plum", 
           color = "black") +
  geom_errorbar(aes(x = year, 
                    ymax = mean_wt_resid + 1.96 * se_wt_resid,
                    ymin = mean_wt_resid - 1.96 * se_wt_resid),
                width = 0.2) +
  geom_hline(yintercept = 0) +
  facet_wrap(~common_name, ncol = 1) + 
  scale_x_continuous(name = "Year") +
  scale_y_continuous(name = "Length-weight residual (ln(g))") +
  theme_minimal(base_size = 13) +
  ggtitle("Condition of spring large (>= 450 g) Atka mackerel\n")

ggsave(paste0("output/large_atka_condition.png"),
       width = 6, height = 7, units = "in", bg = 'white')

atka_ann_mean_resid_df %>% 
  filter(grepl("3S-", common_name)) %>% 
  ggplot() + 
  geom_bar(aes(x = year, 
               y = mean_wt_resid), 
           stat = "identity", 
           fill = "plum", 
           color = "black") +
  geom_errorbar(aes(x = year, 
                    ymax = mean_wt_resid + 1.96 * se_wt_resid,
                    ymin = mean_wt_resid - 1.96 * se_wt_resid),
                width = 0.2) +
  geom_hline(yintercept = 0) +
  facet_wrap(~common_name, ncol = 1) + 
  scale_x_continuous(name = "Year") +
  scale_y_continuous(name = "Length-weight residual (ln(g))") +
  theme_minimal(base_size = 13) +
  ggtitle("Condition of spring small (< 450 g) Atka mackerel\n")

ggsave(paste0("output/small_atka_condition.png"),
       width = 6, height = 7, units = "in", bg = 'white')

# Avg weights ----

df <- read_csv('data/inseason_avg_wts.csv') 
names(df)

df <- df %>% 
  filter(management_program_code == "A80" &
         target_fishery_description == "Atka mackerel" &
         nmfs_area %in% c(541, 542, 543)) %>% 
  mutate(julian = lubridate::yday(haul_date),
         month = lubridate::month(haul_date))

(maxday <- df %>% 
    filter(year == 2024 & haul_date == max(haul_date)) %>%
    distinct(julian, haul_date) %>% 
    pull(julian))

# Make sure all fish are from a comparable time period
dfs <- df %>% filter(julian <= maxday) # %>% nrow


# Boxplot function definition---------------------------------------------------

fn_boxplot <- 
  function(in_df, in_y_lab) { 
    ggplot(data = in_df,
           aes(x = as.factor(year),
               y = y_val) ) +
      geom_boxplot() +
      labs(x     = '',
           y     = in_y_lab,
           title = in_species_name) +
      theme_bw(base_size = 20) + 
      theme(strip.text.y.right = element_text(angle = 0))
  }


#######################
# Boxplots for Weight Per Fish from species comp samples -----------------------

# 1. For all data
# plot_bx_wt_per_fish_all <- (fn_boxplot(in_df    = dfs %>%
#                                          mutate(y_val = species_weight/species_number),
#                                        in_y_lab = 'Weight per fish (kg)') )
# plot_bx_wt_per_fish_all

# 2. AI wide
plot_bx_wt_per_fish_region <-
  (fn_boxplot(in_df = dfs %>%
                mutate(y_val = species_weight/species_number),
              in_y_lab = 'Weight per fish (kg)')  +
     facet_grid(~nmfs_region)
  ) 
plot_bx_wt_per_fish_region

ggsave(paste0("output/atka_avg_wt_ai.png"),
       width = 6, height = 4, units = "in", bg = 'white')


# 3. Faceted by NMFS area
plot_bx_wt_per_fish_nmfs_region <-
  (fn_boxplot(in_df = dfs %>%
                mutate(y_val = species_weight/species_number),
              in_y_lab = 'Weight per fish (kg)')  +
     facet_grid(~nmfs_area)
  ) 
plot_bx_wt_per_fish_nmfs_region +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))

ggsave(paste0("output/atka_avg_wt_by_area.png"),
       width = 12, height = 6, units = "in", bg = 'white')


# 4. Faceted by Target Fishery
# plot_bx_wt_per_tgt_fshry <-
#   (fn_boxplot(in_df = dfs %>%
#                 mutate(y_val = species_weight/species_number),
#               in_y_lab = 'Weight per fish (kg)')  +
#      facet_grid(target_fishery_description ~.)
#   )  
# plot_bx_wt_per_tgt_fshry

# 5. Faceted by Management Program
# plot_bx_wt_per_mgmt_pgm <-
#   (fn_boxplot(in_df = dfs %>%
#                 mutate(y_val = species_weight/species_number),
#               in_y_lab = 'Weight per fish (kg)')  +
#      facet_grid(management_program_code ~.)
#   ) 
# plot_bx_wt_per_mgmt_pgm

# 6. Faceted by Month

ggplot(data = df %>% 
         filter(year >= 2016),
       aes(x = as.factor(month),
           y = species_weight/species_number)) +
  geom_boxplot() +
  labs(x     = '',
       y     = 'Weight per fish (kg)',
       title = in_species_name) +
  facet_grid(~nmfs_area) +
  theme_bw(base_size = 20) + 
  theme(strip.text.y.right = element_text(angle = 0))

ggsave(paste0("output/atka_avg_wt_by_month.png"),
       width = 12, height = 6, units = "in", bg = 'white')


library(ggthemes)
ggplot(data = df %>% 
         filter(year > 2016), # & month <= 5),
       aes(x = as.factor(month),
           y = species_weight/species_number,
           col = factor(year),
           fill = factor(year)) ) +
  geom_boxplot(alpha = 0.25) +
  labs(x     = '',
       y     = 'Weight per fish (kg)',
       title = in_species_name,
       col   = 'Year', fill = 'Year') +
  facet_grid(~nmfs_area) +
  theme_bw(base_size = 20) + 
  scale_color_colorblind() +
  scale_fill_colorblind() +
  theme(strip.text.y.right = element_text(angle = 0)) 
ggsave(paste0("output/atka_spring_avg_wt_by_year.png"),
       width = 15, height = 6, units = "in", bg = 'white')

ggplot(data = df %>% 
         filter(year > 2016 & nmfs_area == 541), # & month <= 5),
       aes(x = as.factor(month),
           y = species_weight/species_number,
           col = factor(year),
           fill = factor(year)) ) +
  geom_boxplot(alpha = 0.3) +
  labs(x     = 'Month',
       y     = 'Weight per fish (kg)',
       col   = 'Year', fill = 'Year') +
  facet_grid(~nmfs_area) +
  theme_bw(base_size = 20) + 
  scale_color_colorblind() +
  scale_fill_colorblind() +
  theme(strip.text.y.right = element_text(angle = 0)) 
ggsave(paste0("output/atka_spring_avg_wt_by_year_541.png"),
       width = 7, height = 6, units = "in", bg = 'white')
