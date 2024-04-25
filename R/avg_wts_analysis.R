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

