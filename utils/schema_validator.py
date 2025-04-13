# Databricks notebook source
# utils/schema_validator.py

def validate_schema(df, expected_columns):
    """
    Valida que un DataFrame contenga todas las columnas esperadas.
    expected_columns debe ser una lista de strings (nombres de columnas).
    Retorna una lista de columnas faltantes (vacía si todo está bien).
    """
    df_columns = set(df.columns)
    expected_set = set(expected_columns)
    missing = expected_set - df_columns
    return list(missing)
