# =========================
# 1) Quantile GBM utilities
# =========================

# install.packages("gbm") # if needed
suppressPackageStartupMessages(library(gbm))

# Trainer that returns a model with class "qgbm"
qreg_gbm_fn <- function(data, formula,
                        interaction.depth = 2,
                        n.trees           = 1000,
                        n.minobsinnode    = 10,
                        shrinkage         = 0.05,
                        bag.fraction      = 0.7,
                        quantiles         = 0.50,
                        cores             = 1) {
  m <- gbm::gbm(
    formula              = formula,
    data                 = data,
    distribution         = list(name = "quantile", alpha = quantiles),
    interaction.depth    = interaction.depth,
    n.trees              = n.trees,
    n.minobsinnode       = n.minobsinnode,
    shrinkage            = shrinkage,
    bag.fraction         = bag.fraction,
    train.fraction       = 1.0,
    n.cores              = cores,
    keep.data            = FALSE,
    verbose              = FALSE
  )
  class(m) <- c("qgbm", class(m))
  m
}

# Make predict() return a column matrix so "[,1]" in caller is safe
predict.qgbm <- function(object, newdata, n.trees, ...) {
  as.matrix(gbm::predict.gbm(object, newdata = newdata, n.trees = n.trees))
}

# ============================================
# 2) Reconciled pipeline (QBRT-ready in both)
# ============================================

reconciled_forecast_pipeline2 <- function(
    hourly_load,
    X_hourly,
    features_by_province,
    prov_keys,
    # windows
    train_end, val_start, val_end, test_start,
    # models
    calendar_model = c("LM","GAM","QBRT"),
    env_model      = c("LM","GAM","QBRT"),
    # covariates
    calendar_covariates = "auto",
    env_covariates      = "auto",
    # QBRT hook (REQUIRED if either model == "QBRT")
    qreg_gbm_fn = NULL,
    gbm_pars_cal = list(interaction.depth=2, n.trees=1000, n.minobsinnode=10,
                        shrinkage=0.05, bag.fraction=0.7, quantile=0.50, cores=1),
    gbm_pars_env = list(interaction.depth=2, n.trees=1500, n.minobsinnode=10,
                        shrinkage=0.05, bag.fraction=0.7, quantile=0.50, cores=1),
    # residual allocation
    residual_allocation = c("fixed","local_shares","local_gradient"),
    # quick MAE horizons
    horizons = c(1,24,168),
    # verbosity
    verbose = TRUE,
    # structure controls
    share_struct_pull = 0.25,  # pull α, α_t toward struct_share (0..1)
    share_temperature = 0.7,   # <1 flattens dynamic shares; 1 = no effect
    grad_struct_pull  = 0.50,  # blend gradient vs structural residual split
    # optional caps (NULL = disable; numeric like 0.30 = ±30%)
    cap_deviation_alpha   = NULL,
    cap_deviation_alpha_t = NULL
){
  suppressPackageStartupMessages({
    library(dplyr); library(tidyr); library(lubridate); library(mgcv)
  })
  `%||%` <- function(a,b) if(!is.null(a)) a else b
  calendar_model <- match.arg(calendar_model)
  env_model      <- match.arg(env_model)
  residual_allocation <- match.arg(residual_allocation)
  
  # ----- 0) Checks -----
  stopifnot(all(c("datetime","load") %in% names(hourly_load)))
  stopifnot(all(c("Province","struct_share") %in% names(prov_keys)))
  if (!all(c("date","hour") %in% names(X_hourly))) {
    stop("X_hourly must include join keys: date (Date) and hour (0..23).")
  }
  if ((calendar_model=="QBRT" || env_model=="QBRT") && is.null(qreg_gbm_fn)) {
    stop("QBRT selected but qreg_gbm_fn is NULL.")
  }
  
  # ----- Build calendar frame -----
  data_cal <- hourly_load %>%
    mutate(date = as.Date(datetime), hour = lubridate::hour(datetime)) %>%
    left_join(X_hourly, by = c("date","hour")) %>%
    arrange(datetime)
  
  # ----- 1) Calendar covariates -----
  xhourly_covs <- setdiff(names(X_hourly), c("date","hour"))
  has_hour     <- "hour" %in% names(data_cal)
  if (identical(calendar_covariates, "auto") || is.null(calendar_covariates)) {
    avail_cal_covs <- intersect(xhourly_covs, names(data_cal))
  } else {
    avail_cal_covs <- intersect(calendar_covariates, names(data_cal))
    if (!length(avail_cal_covs)) avail_cal_covs <- intersect(xhourly_covs, names(data_cal))
  }
  cal_covs_for_lmgbm <- setdiff(avail_cal_covs, "hour")
  if (!length(cal_covs_for_lmgbm) && has_hour) cal_covs_for_lmgbm <- "hour"
  if (!length(cal_covs_for_lmgbm) && !(calendar_model=="GAM" && has_hour)) {
    stop("No usable calendar covariates.")
  }
  
  # splits
  train_df <- data_cal %>% filter(date <= as.Date(train_end))
  val_df   <- data_cal %>% filter(date >= as.Date(val_start), date <= as.Date(val_end))
  test_df  <- data_cal %>% filter(date >= as.Date(test_start))
  
  # ----- 2) Fit CALENDAR -----
  fit_calendar <- switch(calendar_model,
                         "LM" = lm(reformulate(cal_covs_for_lmgbm, response = "load"), data = train_df),
                         "GAM" = {
                           rhs <- paste(setdiff(avail_cal_covs,"hour"), collapse = " + ")
                           gam_form <- if (has_hour)
                             as.formula(paste0("load ~ s(hour, bs='cc', k=24)", if (nzchar(rhs)) paste0(" + ", rhs)))
                           else as.formula(paste0("load ~ ", rhs))
                           mgcv::gam(gam_form, data = train_df, method = "REML")
                         },
                         "QBRT" = qreg_gbm_fn(
                           data = train_df %>% tidyr::drop_na(all_of(c("load", cal_covs_for_lmgbm))),
                           formula = reformulate(cal_covs_for_lmgbm, response = "load"),
                           interaction.depth = gbm_pars_cal$interaction.depth %||% 2,
                           n.trees           = gbm_pars_cal$n.trees %||% 1000,
                           n.minobsinnode    = gbm_pars_cal$n.minobsinnode %||% 10,
                           shrinkage         = gbm_pars_cal$shrinkage %||% 0.05,
                           bag.fraction      = gbm_pars_cal$bag.fraction %||% 0.7,
                           quantiles         = gbm_pars_cal$quantile %||% 0.50,
                           cores             = gbm_pars_cal$cores %||% 1
                         )
  )
  pred_fun_cal <- function(mod, newdata){
    if (calendar_model=="QBRT") {
      as.numeric(predict(mod, newdata = newdata %>% dplyr::select(dplyr::all_of(cal_covs_for_lmgbm)),
                         n.trees = gbm_pars_cal$n.trees)[,1])
    } else as.numeric(predict(mod, newdata = newdata))
  }
  cal_pred_val   <- tibble(datetime=val_df$datetime,   cal_hat = pred_fun_cal(fit_calendar, val_df))
  cal_pred_test  <- tibble(datetime=test_df$datetime,  cal_hat = pred_fun_cal(fit_calendar, test_df))
  
  # ----- 3) Residuals on VALIDATION (global) -----
  val_residuals <- val_df %>%
    left_join(cal_pred_val, by="datetime") %>%
    transmute(datetime, resid = load - cal_hat)
  
  # ENV covariates list (by-province wide table)
  env_cols <- if (identical(env_covariates, "auto") || is.null(env_covariates)) {
    setdiff(names(features_by_province), c("datetime","Province"))
  } else {
    intersect(env_covariates, names(features_by_province))
  }
  if (!length(env_cols)) stop("No usable environmental covariates in features_by_province.")
  
  # sums of env across provinces (global regressors)
  env_sums <- features_by_province %>%
    group_by(datetime) %>%
    summarise(across(all_of(env_cols), ~ sum(.x, na.rm = TRUE), .names = "{.col}_sum"),
              .groups="drop")
  
  env_train_val <- env_sums %>% inner_join(val_residuals, by="datetime")
  env_pred_cols <- setdiff(names(env_train_val), c("datetime","resid"))
  if (!length(env_pred_cols)) stop("No predictors for ENV model after joining.")
  
  fit_env <- switch(env_model,
                    "LM"  = lm(reformulate(env_pred_cols, response="resid"), data = env_train_val),
                    "GAM" = {
                      env_train_val_complete <- env_train_val %>% tidyr::drop_na(all_of(c("resid", env_pred_cols)))
                      nuniq <- sapply(env_pred_cols, function(v) length(unique(env_train_val_complete[[v]])))
                      is_smoothable <- nuniq >= 4
                      k_map <- pmax(2, pmin(6, nuniq - 1))
                      smooth_terms <- sprintf("s(%s, k=%d)", env_pred_cols[is_smoothable], k_map[is_smoothable])
                      linear_terms <- env_pred_cols[!is_smoothable]
                      rhs <- paste(c(smooth_terms, linear_terms), collapse=" + ")
                      gam_form_env <- as.formula(paste("resid ~", rhs))
                      mgcv::gam(gam_form_env, data = env_train_val, method="REML", select=TRUE)
                    },
                    "QBRT"= qreg_gbm_fn(
                      data = env_train_val %>% tidyr::drop_na(all_of(c("resid", env_pred_cols))),
                      formula = reformulate(env_pred_cols, response = "resid"),
                      interaction.depth = gbm_pars_env$interaction.depth %||% 2,
                      n.trees           = gbm_pars_env$n.trees %||% 1500,
                      n.minobsinnode    = gbm_pars_env$n.minobsinnode %||% 10,
                      shrinkage         = gbm_pars_env$shrinkage %||% 0.05,
                      bag.fraction      = gbm_pars_env$bag.fraction %||% 0.7,
                      quantiles         = gbm_pars_env$quantile %||% 0.50,
                      cores             = gbm_pars_env$cores %||% 1
                    )
  )
  pred_fun_env <- function(mod, newdata){
    if (env_model=="QBRT") {
      as.numeric(predict(mod, newdata = newdata %>% dplyr::select(dplyr::all_of(env_pred_cols)),
                         n.trees = gbm_pars_env$n.trees)[,1])
    } else as.numeric(predict(mod, newdata = newdata))
  }
  
  # ----- 4) α from VALIDATION (for "fixed") + structure pull/cap -----
  gamma_tbl <- {
    if (env_model=="LM") {
      co <- coef(fit_env); co <- co[setdiff(names(co), "(Intercept)")]
      keep <- names(co)[names(co) %in% env_pred_cols]
      if (!length(keep)) tibble(drv = env_cols, gamma = 1) else {
        pos <- pmax(co[keep], 0); if (sum(pos)<=0 || any(!is.finite(pos))) pos[] <- 1
        tibble(drv = sub("_sum$","", names(pos)), gamma = as.numeric(pos))
      }
    } else tibble(drv = env_cols, gamma = 1)
  }
  
  feat_val <- features_by_province %>%
    filter(as.Date(datetime) >= as.Date(val_start), as.Date(datetime) <= as.Date(val_end)) %>%
    select(datetime, Province, all_of(env_cols))
  
  long_loc  <- feat_val %>% pivot_longer(-c(datetime, Province), names_to="drv", values_to="val")
  long_glob <- feat_val %>%
    group_by(datetime) %>%
    summarise(across(all_of(env_cols), ~ sum(pmax(.x,0), na.rm=TRUE), .names="{.col}_tot"),
              .groups="drop") %>%
    pivot_longer(-datetime, names_to="drv_tot", values_to="tot") %>%
    mutate(drv = sub("_tot$","", drv_tot), .keep="unused")
  
  dyn_scores_val <- long_loc %>%
    left_join(long_glob, by = c("datetime","drv")) %>%
    left_join(gamma_tbl, by = "drv") %>%
    mutate(share = ifelse(is.finite(tot) & tot>0, pmax(val,0)/tot, 1e-99),
           wshare = (gamma %||% 1) * share) %>%
    group_by(datetime, Province) %>%
    summarise(score = sum(wshare, na.rm=TRUE), .groups="drop") %>%
    group_by(datetime) %>%
    mutate(p_dyn = if (sum(score)>0) score/sum(score) else 1/n()) %>%
    ungroup()
  
  alpha_tbl <- dyn_scores_val %>%
    group_by(Province) %>%
    summarise(alpha = mean(p_dyn, na.rm=TRUE), .groups="drop") %>%
    mutate(alpha = ifelse(!is.finite(alpha), 0, alpha),
           alpha = alpha / sum(alpha))
  
  # geometric pull toward structure + optional cap on α
  weights_tbl <- prov_keys %>% select(Province, w = struct_share)
  eps_num <- 1e-12
  alpha_tbl <- alpha_tbl %>%
    left_join(weights_tbl, by = "Province") %>%
    mutate(
      log_alpha  = log(pmax(alpha, eps_num)),
      log_w      = log(pmax(w,     eps_num)),
      log_blend  = (1 - share_struct_pull) * log_alpha + share_struct_pull * log_w,
      alpha      = exp(log_blend)
    ) %>%
    mutate(alpha = alpha / sum(alpha)) %>%
    { if (is.numeric(cap_deviation_alpha)) {
      mutate(.,
             lo = pmax(eps_num, w * (1 - cap_deviation_alpha)),
             hi = pmin(1.0,     w * (1 + cap_deviation_alpha)),
             alpha = pmin(pmax(alpha, lo), hi)
      ) %>% mutate(alpha = alpha / sum(alpha)) %>% select(Province, alpha)
    } else select(., Province, alpha)
    }
  
  # ----- 5) TEST predictions (global) -----
  cal_test <- cal_pred_test %>% rename(cal_pred = cal_hat)
  env_test <- env_sums %>% filter(as.Date(datetime) >= as.Date(test_start)) %>% arrange(datetime)
  res_test <- env_test %>% mutate(res_pred = pred_fun_env(fit_env, .)) %>% select(datetime, res_pred)
  global_test <- cal_test %>%
    inner_join(res_test, by="datetime") %>%
    mutate(pred_global = cal_pred + res_pred) %>%
    select(datetime, pred_global, cal_pred, res_pred)
  
  prov_cal_piece <- cal_test %>%
    tidyr::crossing(weights_tbl) %>%
    mutate(y_cal = w * cal_pred) %>%
    select(datetime, Province, y_cal)
  
  # ----- 6) Allocate residual to provinces -----
  if (residual_allocation == "fixed") {
    prov_res_piece <- res_test %>%
      tidyr::crossing(alpha_tbl) %>%
      mutate(y_res = alpha * res_pred) %>%
      select(datetime, Province, y_res)
    
  } else if (residual_allocation == "local_shares") {
    # time-varying dynamic shares with temperature + geometric pull + cap
    feat_test <- features_by_province %>%
      filter(as.Date(datetime) >= as.Date(test_start)) %>%
      select(datetime, Province, all_of(env_cols))
    long_loc_test <- feat_test %>% pivot_longer(-c(datetime, Province), names_to="drv", values_to="val")
    long_glob_test <- feat_test %>%
      group_by(datetime) %>%
      summarise(across(all_of(env_cols), ~ sum(pmax(.x,0), na.rm=TRUE), .names="{.col}_tot"),
                .groups="drop") %>%
      pivot_longer(-datetime, names_to="drv_tot", values_to="tot") %>%
      mutate(drv = sub("_tot$","", drv_tot), .keep="unused")
    
    p_dyn_test <- long_loc_test %>%
      left_join(long_glob_test, by = c("datetime","drv")) %>%
      left_join(gamma_tbl, by = "drv") %>%
      mutate(
        share  = ifelse(is.finite(tot) & tot>0, pmax(val,0)/tot, 1e-99),
        wshare = pmax((gamma %||% 1) * share, 1e-12),
        wshare = wshare ^ share_temperature
      ) %>%
      group_by(datetime, Province) %>%
      summarise(score = sum(wshare, na.rm=TRUE), .groups="drop") %>%
      group_by(datetime) %>%
      mutate(alpha_t_raw = if (sum(score)>0) score/sum(score) else 1/n()) %>%
      ungroup() %>%
      left_join(weights_tbl, by = "Province") %>%
      group_by(datetime) %>%
      mutate(
        log_alpha  = log(pmax(alpha_t_raw, 1e-12)),
        log_w      = log(pmax(w,          1e-12)),
        log_blend  = (1 - share_struct_pull) * log_alpha + share_struct_pull * log_w,
        alpha_t    = exp(log_blend),
        alpha_t    = alpha_t / sum(alpha_t)
      ) %>%
      { if (is.numeric(cap_deviation_alpha_t)) {
        mutate(.,
               lo = pmax(1e-12, w * (1 - cap_deviation_alpha_t)),
               hi = pmin(1.0,   w * (1 + cap_deviation_alpha_t)),
               alpha_t = pmin(pmax(alpha_t, lo), hi)
        ) %>% group_by(datetime) %>%
          mutate(alpha_t = alpha_t / sum(alpha_t)) %>%
          ungroup()
      } else .
      } %>%
      ungroup() %>%
      select(datetime, Province, alpha_t)
    
    prov_res_piece <- res_test %>%
      inner_join(p_dyn_test, by = "datetime") %>%
      mutate(y_res = alpha_t * res_pred) %>%
      select(datetime, Province, y_res)
    
  } else { # residual_allocation == "local_gradient"
    # vectorized per-driver finite-difference gradients
    env_test_by_prov <- features_by_province %>%
      filter(as.Date(datetime) >= as.Date(test_start)) %>%
      arrange(datetime, Province) %>%
      select(datetime, Province, all_of(env_cols))
    
    Xsum_test <- env_test_by_prov %>%
      group_by(datetime) %>%
      summarise(across(all_of(env_cols), ~ sum(.x, na.rm=TRUE), .names="{.col}_sum"),
                .groups="drop")
    
    eps_fd <- 1e-3
    if (verbose) cat("\nComputing gradients for", length(env_pred_cols),
                     "drivers across", nrow(Xsum_test), "timestamps...\n")
    
    grad_list <- lapply(seq_along(env_pred_cols), function(i) {
      col <- env_pred_cols[i]
      step_vec <- eps_fd * pmax(1, abs(Xsum_test[[col]]))
      Xp <- Xsum_test; Xm <- Xsum_test
      Xp[[col]] <- Xp[[col]] + step_vec
      Xm[[col]] <- Xm[[col]] - step_vec
      fp <- pred_fun_env(fit_env, Xp)
      fm <- pred_fun_env(fit_env, Xm)
      g  <- (fp - fm) / (2 * step_vec)
      if (verbose) cat(sprintf("  • Driver %d/%d: %-20s → %d derivatives\n",
                               i, length(env_pred_cols), col, length(g)))
      tibble::tibble(
        datetime = Xsum_test$datetime,
        drv_sum  = col,
        m        = as.numeric(g),
        drv      = sub("_sum$","", col)
      )
    })
    grad_tbl <- dplyr::bind_rows(grad_list)
    
    # driver contribution per province: sum_d m_{d,t} * x_{i,d,t}
    long_loc_test <- env_test_by_prov %>%
      pivot_longer(all_of(env_cols), names_to="drv", values_to="xloc")
    drv_loc <- long_loc_test %>%
      left_join(grad_tbl, by = c("datetime","drv")) %>%
      mutate(cval = (m %||% 0) * (xloc %||% 0)) %>%
      group_by(datetime, Province) %>%
      summarise(y_res_drv = sum(cval, na.rm=TRUE), .groups="drop")
    
    # remainder R_t = res_pred - sum_d m_{d,t} * Xsum_d
    grad_dot_Xsum <- grad_tbl %>%
      left_join(
        Xsum_test %>% pivot_longer(all_of(env_pred_cols), names_to="drv_sum", values_to="Xsum"),
        by = c("datetime","drv_sum")
      ) %>%
      group_by(datetime) %>%
      summarise(approx_sum = sum(m * Xsum, na.rm=TRUE), .groups="drop")
    
    remainder <- res_test %>%
      left_join(grad_dot_Xsum, by = "datetime") %>%
      mutate(R = (res_pred %||% 0) - (approx_sum %||% 0)) %>%
      select(datetime, R)
    
    remainder_piece <- remainder %>%
      tidyr::crossing(weights_tbl) %>%
      mutate(R_i = w * R) %>%
      select(datetime, Province, R_i)
    
    prov_res_piece_grad <- drv_loc %>%
      left_join(remainder_piece, by = c("datetime","Province")) %>%
      mutate(y_res_grad = y_res_drv + (R_i %||% 0)) %>%
      select(datetime, Province, y_res_grad)
    
    # structural split (for blending)
    struct_res_piece <- res_test %>%
      tidyr::crossing(weights_tbl) %>%
      transmute(datetime, Province, y_res_struct = w * res_pred)
    
    # OPTIONAL CAP: limit gradient shares around structure before blend
    if (is.numeric(cap_deviation_alpha_t)) {
      prov_res_piece_grad <- prov_res_piece_grad %>%
        left_join(weights_tbl, by = "Province") %>%
        group_by(datetime) %>%
        mutate(
          total_grad = sum(y_res_grad, na.rm = TRUE),
          share_grad = ifelse(total_grad > 0, y_res_grad / total_grad, w),
          lo = pmax(1e-12, w * (1 - cap_deviation_alpha_t)),
          hi = pmin(1.0,   w * (1 + cap_deviation_alpha_t)),
          share_grad_capped = pmin(pmax(share_grad, lo), hi),
          share_grad_capped = share_grad_capped / sum(share_grad_capped, na.rm = TRUE),
          y_res_grad = share_grad_capped * total_grad
        ) %>%
        ungroup() %>%
        select(datetime, Province, y_res_grad)
    }
    
    # final blend
    prov_res_piece <- prov_res_piece_grad %>%
      left_join(struct_res_piece, by = c("datetime","Province")) %>%
      mutate(y_res = (1 - grad_struct_pull) * y_res_grad + grad_struct_pull * y_res_struct) %>%
      select(datetime, Province, y_res)
  }
  
  # ----- 7) Province predictions & reconciliation -----
  provinces_test <- prov_cal_piece %>%
    left_join(prov_res_piece, by = c("datetime","Province")) %>%
    mutate(y_res = tidyr::replace_na(y_res, 0),
           pred  = y_cal + y_res)
  
  reconciliation_check <- provinces_test %>%
    group_by(datetime) %>% summarise(sum_prov = sum(pred), .groups="drop") %>%
    inner_join(global_test, by="datetime") %>%
    mutate(diff = sum_prov - pred_global)
  
  # ----- 8) Optional MAE table -----
  mae_table <- NULL
  if (length(horizons) > 0) {
    actual_test <- data_cal %>%
      filter(date >= as.Date(test_start)) %>%
      select(datetime, load) %>%
      arrange(datetime)
    for (h in horizons) {
      actual_test[[paste0("Y_lead_", h)]] <- dplyr::lead(actual_test$load, h)
    }
    eval_h <- function(h) {
      col <- paste0("Y_lead_", h)
      tibble(datetime = global_test$datetime,
             pred = global_test$pred_global) %>%
        inner_join(actual_test %>% select(datetime, !!rlang::sym(col)), by = "datetime") %>%
        rename(Y_lead = !!rlang::sym(col)) %>%
        summarise(
          n   = sum(is.finite(Y_lead) & is.finite(pred)),
          MAE = mean(abs(Y_lead - pred), na.rm = TRUE),
          .groups = "drop"
        ) %>% mutate(h = h)
    }
    mae_table <- do.call(bind_rows, lapply(horizons, eval_h)) %>%
      tidyr::pivot_wider(names_from = h, values_from = c(MAE, n),
                         names_glue = "{.value}_h{h}")
  }
  
  # ----- RETURN -----
  list(
    inputs_used = list(
      calendar_model=calendar_model, env_model=env_model,
      residual_allocation=residual_allocation,
      calendar_covariates=avail_cal_covs, env_covariates=env_cols,
      splits=list(
        train_end = as.character(as.Date(train_end)),
        val_start = as.character(as.Date(val_start)),
        val_end   = as.character(as.Date(val_end)),
        test_start= as.character(as.Date(test_start))
      ),
      controls=list(
        share_struct_pull=share_struct_pull,
        share_temperature=share_temperature,
        grad_struct_pull=grad_struct_pull,
        cap_deviation_alpha=cap_deviation_alpha,
        cap_deviation_alpha_t=cap_deviation_alpha_t
      )
    ),
    models = list(calendar=fit_calendar, env=fit_env),
    alpha_weights = alpha_tbl,
    global_test = global_test,
    provinces_test = provinces_test,
    reconciliation_check = reconciliation_check,
    mae_table_test = mae_table
  )
}

# ===========================
# 3) Example usage (two ways)
# ===========================

# A) QBRT in CALENDAR, GAM in ENV
# out <- reconciled_forecast_pipeline2(
#   hourly_load = load_sum,
#   X_hourly = X_hourly,
#   features_by_province = env_wide,
#   prov_keys = prov_keys,
#   train_end  = as.Date("2023-06-30"),
#   val_start  = as.Date("2023-07-01"),
#   val_end    = as.Date("2_

