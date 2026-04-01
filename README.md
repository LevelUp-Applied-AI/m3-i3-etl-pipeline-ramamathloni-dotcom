# ETL Pipeline — Amman Digital Market

[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)

## Overview
This project implements a robust **Python ETL (Extract, Transform, Load) Pipeline** for the "Amman Digital Market". The pipeline automates the process of:
1. **Extracting** raw data from a PostgreSQL database.
2. **Transforming** fragmented tables into a comprehensive customer-level analytics summary using **Pandas**.
3. **Validating** data integrity through strict quality checks to ensure reliable business insights.
4. **Loading** the final dataset back into a PostgreSQL analytics table and exporting it as a CSV for reporting.

## Setup

1. **Start PostgreSQL container:**
   ```bash
   docker run -d --name postgres-m3-int \
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=amman_market \
     -p 5432:5432 -v pgdata_m3_int:/var/lib/postgresql/data \
     postgres:15-alpine