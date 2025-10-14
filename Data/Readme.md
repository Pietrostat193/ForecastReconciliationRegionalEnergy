# 🇮🇹 Wind Energy Production and Forecasting – Italy 2023

This repository contains data and resources used to estimate and model **hourly wind energy production at the provincial level in Italy**, using a combination of **national-level time-series**, **annual production by province**, and **geolocated wind farm data**.

---

## 📁 Files

### 1. `GenerazioneAnnualeProvince2023.xlsx`
- **Source:** [Terna](https://www.terna.it) – the Italian electricity transmission system operator.
- **Description:** Annual wind energy production (MWh) per province in 2023.
- **Usage:** Used to distribute national hourly wind production across provinces based on annual totals.

---

### 2. `Wind2023HourlyItaly.xlsx`
- **Source:** National energy dataset or measurement platform (e.g., Terna, GSE).
- **Description:** Hourly wind energy production (MWh) for the whole of Italy in 2023 (8,760 hours).
- **Usage:** Used as the base time-series to be scaled per province using either static annual shares or dynamic weather-based weights.

---

### 3. `Impianti.geojson`
- **Source:** [OpenStreetMap Overpass API](https://overpass-turbo.eu/), using the query:

```overpassql
[out:json][timeout:180];
area["ISO3166-1"="IT"][admin_level=2]->.italy;
(
  node["generator:source"="wind"](area.italy);
  relation["power"="plant"]["plant:source"="wind"](area.italy);
);
out center tags;



