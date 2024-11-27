#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para ejecutar migraciones de la base de datos
"""

from db.migrations import run_migrations

if __name__ == "__main__":
    run_migrations()