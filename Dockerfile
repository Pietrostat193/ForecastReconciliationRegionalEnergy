# Dockerfile
FROM rocker/tidyverse:4.3.3

# ---- System deps (for devtools/remotes + compiling from source) ----
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata libcurl4-openssl-dev libssl-dev libxml2-dev zlib1g-dev \
    libgit2-dev git build-essential \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Europe/Rome
ENV DATA_DIR=/data
ENV OUTPUT_DIR=/outputs

# ---- Install R packages as root (system library is writable for root) ----
RUN R -q -e "install.packages(c( \
      'devtools','dplyr','tidyr','lubridate','ggplot2','readxl','mgcv','purrr' \
    ), repos='https://cloud.r-project.org')"

# ProbCast from GitHub
RUN R -q -e "devtools::install_github('jbrowell/ProbCast', upgrade = 'never')"

# ---- Create non-root user & set up workspace/permissions ----
RUN useradd -m ruser
WORKDIR /app

# Copia sia main.R che la pipeline
COPY main.R /app/main.R
COPY forecast_reconciliation_pipeline2.R /app/forecast_reconciliation_pipeline2.R

RUN mkdir -p /outputs && chown -R ruser:ruser /app /outputs
USER ruser

CMD [ "Rscript", "/app/main.R" ]
