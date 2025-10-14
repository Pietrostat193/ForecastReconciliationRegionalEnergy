# ===========================
# main.R  — Docker-ready entry script
# ===========================


# ---- 0) Setup & helpers ----
data_dir   <- Sys.getenv("DATA_DIR", unset = "/data")
output_dir <- Sys.getenv("OUTPUT_DIR", unset = "/outputs")

# NON mettere data_dir <- "C:/..." e NON mettere output_dir <- "C:/..."
# Lasciali presi da env vars, così nel container saranno /data e /outputs.

# Se ti serve il file locale della pipeline, lasciamo il source così:
source("forecast_reconciliation_pipeline2.R")

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

message("DATA_DIR   = ", data_dir)
message("OUTPUT_DIR = ", output_dir)



p <- function(...) file.path(data_dir, ...)
o <- function(...) file.path(output_dir, ...)

check_exists <- function(path) {
  if (!file.exists(path)) stop("Required file not found: ", path, call. = FALSE)
}

# ---- 1) Libraries ----
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(lubridate)
  library(ggplot2)
  library(readxl)
  library(mgcv)
  library(purrr)
  # library(ProbCast)  # if not automatically loaded by your image
})

# ---- 2) Input files (edit here if your layout differs) ----
f_models     <- p("Xhourly_cal.Rdata")
f_env_south  <- p("features_by_province.Rdata")
f_env_cal    <- p("features_by_province_calabria.Rdata")
f_load_south <- p("Load_South_2023.xlsx")
f_load_cal   <- p("Load_Calabria_hourly.Rdata")


invisible(lapply(c(f_models, f_env_south, f_env_cal, f_load_south, f_load_cal), check_exists))

# ---- 3) Load data ----
load(f_models)      # expected: X_hourly (and maybe qreg_gbm / qreg_gbm_fn)
load(f_env_south)   # expected: features_by_province
load(f_env_cal)     # expected: features_by_province_calabria
Load_South_2023 <- readxl::read_excel(f_load_south)
load(f_load_cal)    # expected: Load_Calabria_hourly

# ---- 4) Clean features (drop skewness) ----
drop_skew <- c("solar_skewness","precip_skewness","w10_skewness","w100_skewness","t2m_skewness")
fp  <- features_by_province %>% select(-any_of(drop_skew))
fpc <- features_by_province_calabria %>% select(-any_of(drop_skew))

# ---- 5) Build hourly aggregates (MEAN across provinces) ----
hourly_mean_south <- fp %>%
  group_by(datetime) %>%
  summarise(
    n_provinces = n(),
    across(where(is.numeric), ~ mean(.x, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  mutate(Province = "South Italy") %>%
  arrange(datetime)

hourly_mean_calabria <- fpc %>%
  group_by(datetime) %>%
  summarise(
    n_provinces = n(),
    across(where(is.numeric), ~ mean(.x, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  mutate(Province = "Calabria") %>%
  arrange(datetime)

# ---- 6) Normalize solar units if needed (J/m^2·hr -> W/m^2) ----
normalize_solar_scale <- function(df, solar_cols = c("solar_mean","solar_sd")) {
  if (all(solar_cols %in% names(df))) {
    med <- suppressWarnings(median(df$solar_mean, na.rm = TRUE))
    if (is.finite(med) && med > 2e3) {
      df <- df %>%
        mutate(
          solar_mean = solar_mean / 3600,
          solar_sd   = solar_sd   / 3600
        )
    }
  }
  df
}
hourly_mean_south    <- normalize_solar_scale(hourly_mean_south)
hourly_mean_calabria <- normalize_solar_scale(hourly_mean_calabria)

# Optional: domain reweight for South solar (set via ENV, default 1.0 = off)
eta_south <- as.numeric(Sys.getenv("ETA_SOUTH_SOLAR", unset = "1.0"))
if (!is.na(eta_south) && eta_south != 1) {
  solar_cols <- grep("^solar", names(hourly_mean_south), value = TRUE)
  if (length(solar_cols) > 0) {
    hourly_mean_south[solar_cols] <- lapply(hourly_mean_south[solar_cols], `*`, eta_south)
  }
  message("Applied solar reweight to South Italy: eta_south = ", eta_south)
}

# ---- 7) Build env_wide (what the pipeline expects: features by Province) ----
env_wide <- bind_rows(hourly_mean_south, hourly_mean_calabria) %>%
  arrange(datetime, Province)

# Save a quick preview
readr::write_csv(head(env_wide, 50), o("preview_env_wide.csv"))

# ---- 8) Prepare hourly loads ----
# South: aggregate to hourly mean
hourly_load_south <- Load_South_2023 %>%
  mutate(hour = floor_date(Date, "hour")) %>%
  group_by(hour) %>%
  summarise(load = mean(`Total Load [MW]`, na.rm = TRUE), .groups = "drop") %>%
  rename(datetime = hour) %>%
  arrange(datetime)

# Calabria: harmonize names from RData object
hourly_load_calabria <- Load_Calabria_hourly %>%
  rename(
    Province      = `Bidding Zone`,
    datetime      = Date,
    load          = `Total Load [MW]`,
    load_forecast = `Forecast Total Load [MW]`
  ) %>%
  arrange(datetime)

# Global/macro load = South + Calabria (inner join on datetime)
load_sum <- hourly_load_south %>%
  inner_join(hourly_load_calabria %>% select(datetime, load), by = "datetime", suffix = c("_south","_cal")) %>%
  transmute(datetime, load = load_south + load_cal) %>%
  arrange(datetime)

readr::write_csv(head(load_sum, 50), o("preview_load_sum.csv"))

# ---- 9) Province structural keys (shares) ----
prov_keys <- bind_rows(
  hourly_load_south    %>% transmute(Province = "South Italy", load),
  hourly_load_calabria %>% transmute(Province = "Calabria",    load)
) %>%
  group_by(Province) %>%
  summarise(total_load = sum(load, na.rm = TRUE), .groups = "drop") %>%
  mutate(struct_share = total_load / sum(total_load)) %>%
  select(Province, struct_share)

stopifnot(abs(sum(prov_keys$struct_share) - 1) < 1e-6)
readr::write_csv(prov_keys, o("prov_keys.csv"))

# ---- 10) Quick sanity plot: first 168 hours of load per Province ----
first168 <- bind_rows(
  hourly_load_south    %>% mutate(Province = "South Italy"),
  hourly_load_calabria %>% mutate(Province = "Calabria")
) %>%
  group_by(Province) %>%
  arrange(datetime, .by_group = TRUE) %>%
  slice_head(n = 168) %>%
  ungroup()

p_load <- ggplot(first168, aes(datetime, load, color = Province)) +
  geom_line(linewidth = 0.7) +
  labs(title = "First 168 hours — Load by Province", x = NULL, y = "MW", color = NULL) +
  theme_minimal(base_size = 11) +
  theme(legend.position = "top")

ggsave(o("first168_load.png"), p_load, width = 10, height = 6, dpi = 150)

# ---- 11) Run reconciled pipeline (if available) ----
# We expect reconciled_forecast_pipeline2() and possibly qreg_gbm / qreg_gbm_fn.
if (!exists("reconciled_forecast_pipeline2")) {
  message("WARNING: 'reconciled_forecast_pipeline2' not found. ",
          "Skipping model run. Ensure the function is available (e.g., ProbCast loaded).")
  quit(save = "no", status = 0)
}

# Prefer qreg_gbm_fn if present; otherwise use qreg_gbm if available
qreg_gbm_fn <- if (exists("qreg_gbm_fn")) qreg_gbm_fn else if (exists("qreg_gbm")) qreg_gbm else NULL

# Basic parameterization (edit dates/models to taste)
train_end  <- as.Date("2023-06-30")
val_start  <- as.Date("2023-07-01")
val_end    <- as.Date("2023-09-30")
test_start <- as.Date("2023-10-01")

out <- reconciled_forecast_pipeline2(
  hourly_load          = load_sum,
  X_hourly             = X_hourly,          # from your models RData
  features_by_province = env_wide,
  prov_keys            = prov_keys,
  train_end            = train_end,
  val_start            = val_start,
  val_end              = val_end,
  test_start           = test_start,
  calendar_model       = "GAM",
  env_model            = "GAM",
  residual_allocation  = "local_gradient",
  share_struct_pull    = 0.5,
  share_temperature    = 0.6,
  grad_struct_pull     = 0.7,
  cap_deviation_alpha   = 0.30,
  cap_deviation_alpha_t = 0.30,
  qreg_gbm_fn          = qreg_gbm_fn,
  verbose              = TRUE
)

# ---- 12) Save key outputs ----
if (!is.null(out$global_test)) {
  readr::write_csv(out$global_test, o("global_test_predictions.csv"))
}
if (!is.null(out$provinces_test)) {
  readr::write_csv(out$provinces_test, o("province_test_predictions.csv"))
}

# ---- 13) Basic evaluation on test period ----
if (!is.null(out$provinces_test)) {
  test_range <- range(out$provinces_test$datetime)
  
  truth <- bind_rows(
    hourly_load_calabria %>% transmute(datetime, Province = "Calabria",    load_true = load),
    hourly_load_south    %>% transmute(datetime, Province = "South Italy", load_true = load)
  ) %>%
    filter(datetime >= test_range[1], datetime <= test_range[2])
  
  comp <- out$provinces_test %>%
    select(datetime, Province, y_cal, y_res, pred) %>%
    inner_join(truth, by = c("datetime","Province")) %>%
    mutate(
      err    = pred - load_true,
      abserr = abs(err),
      sqerr  = err^2,
      ape    = abserr / pmax(load_true, 1e-6)
    )
  
  metrics_by_prov <- comp %>%
    group_by(Province) %>%
    summarise(
      n     = n(),
      MAE   = mean(abserr),
      RMSE  = sqrt(mean(sqerr)),
      MAPE  = 100*mean(ape),
      Bias  = mean(err),
      Corr  = cor(pred, load_true),
      .groups = "drop"
    )
  
  metrics_overall <- comp %>%
    summarise(
      n     = n(),
      MAE   = mean(abserr),
      RMSE  = sqrt(mean(sqerr)),
      MAPE  = 100*mean(ape),
      Bias  = mean(err),
      Corr  = cor(pred, load_true)
    )
  
  readr::write_csv(metrics_by_prov, o("metrics_by_province.csv"))
  readr::write_csv(metrics_overall, o("metrics_overall.csv"))
  
  # Plot observed vs predicted
  p_obs_pred <- ggplot(comp, aes(datetime)) +
    geom_line(aes(y = load_true, color = "Observed"), linewidth = 0.7) +
    geom_line(aes(y = pred,      color = "Predicted"), linewidth = 0.7, linetype = 2) +
    facet_wrap(~ Province, ncol = 1, scales = "free_y") +
    labs(title = "Provincial load — observed vs predicted (test)",
         x = NULL, y = "MW", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "top")
  
  ggsave(o("observed_vs_predicted_test.png"), p_obs_pred, width = 11, height = 7, dpi = 150)
  
  # Reconciliation check (sum of provincial preds vs global)
  if (!is.null(out$global_test)) {
    chk <- comp %>%
      group_by(datetime) %>%
      summarise(sum_pred_prov = sum(pred), .groups = "drop") %>%
      inner_join(out$global_test %>% select(datetime, pred_global), by = "datetime") %>%
      mutate(diff = sum_pred_prov - pred_global)
    
    capture.output(summary(chk$diff), file = o("reconciliation_summary.txt"))
  }
}

message("Done. Check the 'outputs/' folder for CSVs and plots.")
